import os
import re
import json
import PyPDF2
from io import BytesIO
from datetime import datetime
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# ─────────────────────────────────────────────
# Railway working directory
# ─────────────────────────────────────────────
root_root_dir = os.getcwd()
sys.path.append(root_root_dir)

# ─────────────────────────────────────────────
# Imports vanuit Functions structuur
# ─────────────────────────────────────────────
from Functions.system.path.path import dropbox_path
from Functions.date.date_functions import working_days
from Functions.connection.db_connection import get_cursor
from Functions.database.lookup import account
from Functions.log_tools.logging import Color


# ─────────────────────────────────────────────
# Database connectie
# ─────────────────────────────────────────────
cursor, conn = get_cursor()


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────
def select_files_for_date(table, file_date):

    sql = f"SELECT file_name FROM {table} WHERE file_date = %s"
    cursor.execute(sql, (file_date,))
    return {row["file_name"] for row in cursor.fetchall()}


def parse_number(value_str):

    if not value_str:
        return None

    value_str = value_str.strip()

    negative = False

    if value_str.endswith('-'):
        negative = True
        value_str = value_str[:-1]

    if value_str.startswith('-'):
        negative = True
        value_str = value_str[1:]

    clean = value_str.replace('.', '').replace(',', '')

    try:
        num = int(clean)
        return -num if negative else num
    except:
        return None


def parse_account_from_filename(filename):

    base = os.path.basename(filename)

    # print(f"\n{Color.OKBLUE}📄 Bestand: {base}{Color.ENDC}")

    if re.match(r"^DEF\.6370-\d{8}\.pdf$", base):
        return "6370", "6370"

    if re.match(r"^DEF\.6370\.C6370-\d{8}\.pdf$", base):
        return "6370.C6370", "6370.TRAD-1"

    if re.match(r"^DEF\.6370\.C6372-\d{8}\.pdf$", base):
        return "6370.C6372", "6372"

    trad_match = re.search(r"DEF\.6370\.C(\d+)\.TRAD\.(\d+)", base)

    if trad_match:
        core, trad_num = trad_match.groups()
        return f"6370.C{core}.TRAD.{trad_num}", f"TRAD.{trad_num}"

    match_simple = re.search(r"DEF\.6370\.C(\d+)", base)

    if match_simple:
        core = match_simple.group(1)
        return f"6370.C{core}", core

    return None, None


def parse_date_from_filename(filename):

    match = re.search(r"-(\d{8})\.pdf$", filename)

    if not match:
        return None

    return datetime.strptime(match.group(1), "%Y%m%d").date()


# ─────────────────────────────────────────────
# Hoofdproces
# ─────────────────────────────────────────────
def process_pdfs():

    for proc_date in working_days():

        proc_date_str = proc_date.strftime("%Y-%m-%d")

        existing_files = select_files_for_date("files", proc_date_str)

        # print(f"\n{Color.HEADER}─────────────────────────────────────────────{Color.ENDC}")
        # print(f"{Color.OKBLUE}📅 Verwerk datum: {proc_date_str}{Color.ENDC}")
        # print(f"{Color.CYAN}   → {len(existing_files)} bestaande PDF's gevonden{Color.ENDC}")
        # print(f"{Color.HEADER}─────────────────────────────────────────────{Color.ENDC}")

        dropbox_folder = os.path.join(dropbox_path, proc_date_str)

        if not os.path.exists(dropbox_folder):
            # print(f"{Color.WARNING}⚠ Geen map gevonden: {dropbox_folder}{Color.ENDC}")
            continue

        for file in os.listdir(dropbox_folder):

            if file.startswith("."):
                continue

            if not file.lower().endswith(".pdf"):
                continue

            if re.match(r"^DEF\.6370\.C6370\.TRAD\.\d+-\d{8}\.pdf$", file, re.IGNORECASE):
                # print(f"{Color.WARNING}⚠ Skip TCAF_C TRAD: {file}{Color.ENDC}")
                continue

            if file in existing_files:
                # print(f"{Color.WARNING}⚠ Skip (al in DB): {file}{Color.ENDC}")
                continue

            if not re.match(r"^DEF\.6370.*\.pdf$", file, re.IGNORECASE):
                # print(f"{Color.WARNING}⚠ Skip (geen DEF.6370): {file}{Color.ENDC}")
                continue

            file_path = os.path.join(dropbox_folder, file)

            account_id, abn_account = parse_account_from_filename(file)

            if not account_id:
                # print(f"{Color.FAIL}⚠ Geen account in bestandsnaam: {file}{Color.ENDC}")
                continue

            account_treecap, portfolio_manager = account(account_id, cursor)

            if isinstance(portfolio_manager, (list, dict)):
                portfolio_manager = json.dumps(portfolio_manager)

            elif not portfolio_manager:
                portfolio_manager = json.dumps(["TREECAP"])

            # print(f"   🆔 account_id      → {account_id}")
            # print(f"   🏦 account_treecap → {account_treecap}")
            # print(f"   👤 portfolio_mgr   → {portfolio_manager}")
            # print(f"   💼 abn_account     → {abn_account}")

            with open(file_path, "rb") as f:

                reader = PyPDF2.PdfReader(f)

                writer = PyPDF2.PdfWriter()

                for page in reader.pages:
                    writer.add_page(page)

                out = BytesIO()
                writer.write(out)

                pdf_blob = out.getvalue()

            first_text = reader.pages[0].extract_text() if len(reader.pages) else ""

            net_liq_match = re.search(r"Net\.?\s*Liq\.?\s*([-]?\d[\d\.,-]*)", first_text, re.IGNORECASE)
            haircut_match = re.search(r"Haircut\s*([-]?\d[\d\.,-]*)", first_text, re.IGNORECASE)

            net_liq = parse_number(net_liq_match.group(1)) if net_liq_match else None
            haircut = parse_number(haircut_match.group(1)) if haircut_match else None
            #
            # print(f"   💰 NetLiq  → {net_liq}")
            # print(f"   ✂️  Haircut → {haircut}")

            file_size = os.path.getsize(file_path)
            file_extension = os.path.splitext(file)[1].lstrip(".")
            file_date = parse_date_from_filename(file)
            upload_date = datetime.now()

            usage_description = f"PDF voor account {account_id}"

            sql = """
                INSERT INTO files (
                    file_name, file_extension, file_content, file_type,
                    usage_description, file_size, file_date, upload_date,
                    account_treecap, portfolio_manager, abn_account, net_liq, haircut
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                    net_liq = VALUES(net_liq),
                    haircut = VALUES(haircut)
            """

            values = (
                file,
                file_extension,
                pdf_blob,
                "pdf",
                usage_description,
                file_size,
                file_date,
                upload_date,
                account_treecap,
                portfolio_manager,
                abn_account,
                net_liq,
                haircut
            )

            try:

                cursor.execute(sql, values)
                conn.commit()

                # print(f"{Color.OKGREEN}    ✅ Ingevoegd: {account_treecap} ({abn_account}) — NLQ={net_liq}, HC={haircut}{Color.ENDC}")

            except Exception as e:

                # print(f"{Color.FAIL}    ❌ Fout bij invoegen {file}: {e}{Color.ENDC}")
                conn.rollback()

    print(f"{Color.OKGREEN}✔️ Alle PDF's succesvol verwerkt.{Color.ENDC}")


# ─────────────────────────────────────────────
# START
# ─────────────────────────────────────────────
if __name__ == "__main__":

    process_pdfs()

    cursor.close()
    conn.close()