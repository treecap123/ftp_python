# === import_ctr_xml_into_clearing.py ===

import os
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, date
import json
import pymysql
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

# ─────────────────────────────────────────────
# Railway working directory
# ─────────────────────────────────────────────
root_root_dir = os.getcwd()
sys.path.append(root_root_dir)

# ─────────────────────────────────────────────
# Imports vanuit Functions structuur
# ─────────────────────────────────────────────
from Functions.date.date_functions import get_previous_workday, working_days
from Functions.connection.db_connection import get_cursor
from Functions.database.lookup import account as account_search
from Functions.log_tools.logging import Color
from Functions.system.path.path import dropbox_path


# ─────────────────────────────────────────────
# Verbindingen (lokaal + Railway)
# ─────────────────────────────────────────────
mycursor, conn = get_cursor()

try:
    connection_info = mycursor.get_server_info()
    mycursor.execute("SELECT DATABASE() AS db, USER() AS user;")
    info = mycursor.fetchone()

    print(f"{Color.CYAN}🔌 Verbonden met server: {connection_info}{Color.ENDC}")
    print(f"{Color.OKGREEN}📂 Database: {info['db']} | 👤 Gebruiker: {info['user']}{Color.ENDC}")

except Exception as e:
    print(f"{Color.FAIL}❌ Kon verbindingsinfo niet ophalen: {e}{Color.ENDC}")


# ─────────────────────────────────────────────
# Functies
# ─────────────────────────────────────────────
def get_existing_dates(cursor):

    sql = "SELECT DISTINCT date FROM clearing WHERE name = 'ctr'"
    mycursor.execute(sql)
    rows = cursor.fetchall()

    return {
        r["date"].strftime("%Y-%m-%d") if isinstance(r["date"], date) else str(r["date"])
        for r in rows
    }


def get_master(cursor, symbol, as_of_date=None):

    if as_of_date is None:
        as_of_date = date.today()

    elif isinstance(as_of_date, str):
        as_of_date = datetime.strptime(as_of_date, "%Y-%m-%d").date()

    elif isinstance(as_of_date, datetime):
        as_of_date = as_of_date.date()

    prev_workday = get_previous_workday(as_of_date)

    sql = """
        SELECT category, deal, symbol_treecap
        FROM master_instrument
        WHERE abn_symbol = %s
          AND date <= %s
        ORDER BY date DESC
        LIMIT 1
    """

    cursor.execute(sql, (symbol, prev_workday))
    row = cursor.fetchone()

    if not row:
        return None, None, None

    return row["category"], row["deal"], row["symbol_treecap"]


# ─────────────────────────────────────────────
# CTR Importfunctie
# ─────────────────────────────────────────────
def import_ctr_xml_into_clearing():

    processed_count = 0

    existing_dates_local = get_existing_dates(mycursor)

    print(f"📅 Reeds verwerkte dagen (lokaal): {sorted(existing_dates_local)}")

    for proc_date in working_days():

        date_str = proc_date.strftime("%Y-%m-%d")

        if date_str in existing_dates_local or proc_date > date.today():
            continue

        print(f"{Color.OKBLUE}📅 Verwerking gestart voor {date_str}...{Color.ENDC}")

        dropbox_folder = os.path.join(dropbox_path, date_str)

        if not os.path.exists(dropbox_folder):
            print(f"{Color.WARNING}⚠ Geen map gevonden: {dropbox_folder}{Color.ENDC}")
            continue

        for filename in os.listdir(dropbox_folder):

            if filename.startswith("."):
                continue

            if "3182-C3182-CTR (L)" not in filename:
                continue

            full_path = os.path.join(dropbox_folder, filename)

            print(f"  🔍 Processing file: {full_path}")

            try:
                tree = ET.parse(full_path)

            except Exception as e:
                print(f"{Color.FAIL}❌ XML leesfout: {e}{Color.ENDC}")
                continue

            root = tree.getroot()

            for account in root.findall(".//AccountContracts"):

                account_id = account.attrib.get("AccountID", "")

                account_treecap, portfolio = account_search(account_id, mycursor)

                if not portfolio:
                    portfolio_manager = json.dumps(["TREECAP"])
                else:
                    portfolio_manager = json.dumps(
                        portfolio if isinstance(portfolio, list) else [portfolio]
                    )

                print(
                    f"{Color.CYAN}    ↳ Account: {account_id} | treecap={account_treecap} | portfolio={portfolio_manager}{Color.ENDC}"
                )

                for fixed in account.findall(".//FixedDeal"):

                    symbol = fixed.findtext("Product/Symbol", default="")
                    currency_code = fixed.findtext("Currency/CurrencyCode", default="")

                    try:
                        quantity_val = int(fixed.findtext("Quantity", default="0"))
                    except ValueError:
                        quantity_val = 0

                    try:
                        interest_rate_val = round(
                            float(fixed.findtext("InterestRate", default="0")), 4
                        )
                    except ValueError:
                        interest_rate_val = 0.0

                    try:
                        pricing_unit_val = int(
                            fixed.findtext("Currency/CurrencyPricingUnit", default="") or 0
                        )
                    except ValueError:
                        pricing_unit_val = 0

                    full_symbol = f"{symbol} {currency_code}".strip()

                    category, deal, symbol_treecap = get_master(
                        mycursor, full_symbol, proc_date
                    )

                    insert_sql = """
                        INSERT INTO clearing (
                            date, name, account_treecap, portfolio_manager, abn_account,
                            category, deal, symbol_treecap, ProductGroupCode,
                            ProductGroupName, abn_symbol, InstrumentIDType,
                            InstrumentIDValue, DepotId, SafeKeepingId,
                            CurrencyCode, currency_pricing_unit,
                            InternalReference, Quantity,
                            opening_date, due_date, entry_date,
                            interest_rate, last_movement
                        )
                        VALUES (
                            %s,%s,%s,%s,%s,
                            %s,%s,%s,%s,
                            %s,%s,%s,
                            %s,%s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s
                        )
                    """

                    params = (
                        proc_date,
                        "ctr",
                        account_treecap,
                        portfolio_manager,
                        account_id,
                        category,
                        deal,
                        symbol_treecap,
                        fixed.findtext("Product/ProductGroupCode", ""),
                        fixed.findtext("Product/ProductGroupName", ""),
                        full_symbol,
                        fixed.findtext("Product/InstrumentIDType", ""),
                        fixed.findtext("Product/InstrumentIDValue", ""),
                        fixed.findtext("Depot/DepotId", ""),
                        fixed.findtext("SafeKeeping/SafeKeepingId", ""),
                        currency_code,
                        pricing_unit_val,
                        fixed.findtext("InternalReference", ""),
                        quantity_val,
                        fixed.findtext("OpeningDate", ""),
                        fixed.findtext("DueDate", ""),
                        fixed.findtext("EntryDate", ""),
                        interest_rate_val,
                        fixed.findtext("LastMovementDate", ""),
                    )

                    mycursor.execute(insert_sql, params)
                    conn.commit()
        processed_count += 1

        print(f"{Color.OKGREEN}✔ CTR data voor {date_str} verwerkt.{Color.ENDC}")

    if processed_count == 0:
        print(f"{Color.WARNING}Alles al up-to-date.{Color.ENDC}")


# ─────────────────────────────────────────────
# Main entrypoint
# ─────────────────────────────────────────────
if __name__ == "__main__":

    import_ctr_xml_into_clearing()

    mycursor.close()
    conn.close()
