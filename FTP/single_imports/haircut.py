# === import_haircut_xml_into_clearing.py ===

import os
import re
import json
import xml.etree.ElementTree as ET
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
from Functions.date.date_functions import get_previous_workday, select_date, working_days
from Functions.connection.db_connection import get_cursor
from Functions.database.lookup import account, trad_list
from Functions.log_tools.logging import Color


# ─────────────────────────────────────────────
# Database verbinding
# ─────────────────────────────────────────────
cursor, conn = get_cursor()


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────
def checkrows(date, account_treecap, portfolio_manager, reformed_account):

    sql_check = """
        SELECT 1
        FROM clearing
        WHERE date = %s
        AND name = 'haircut'
        AND account_treecap = %s
        AND JSON_CONTAINS(portfolio_manager, %s)
        AND abn_account = %s
        LIMIT 1
    """

    cursor.execute(sql_check, (
        date,
        account_treecap,
        json.dumps(portfolio_manager),
        reformed_account
    ))

    return cursor.fetchone()


def is_processed(date):

    sql = "SELECT 1 FROM clearing WHERE date = %s AND name = 'haircut' LIMIT 1"

    cursor.execute(sql, (date,))
    return True if cursor.fetchone() else False


# ─────────────────────────────────────────────
# XML parser
# ─────────────────────────────────────────────
def parse_xml_file(file_path, account_treecap, portfolio_manager, unique_account):

    # print(f"Processing file: {file_path}")

    tree = ET.parse(file_path)
    root = tree.getroot()

    namespace = {"ns": "http://www.abnamroclearing.com/coh"}

    underlying_data = []

    for underlying in root.findall(".//ns:Underlying", namespace):

        try:

            ul_info = underlying.find("ns:ULInfo", namespace)

            if ul_info is None:
                continue

            underlying_id = underlying.get("id", "N/A")

            ul_product = ul_info.find("ns:Value[@id='UL product']", namespace).text.strip()
            reference_product = ul_info.find("ns:Value[@id='Reference product']", namespace).text.strip()

            haircut = float(ul_info.find("ns:Value[@id='Haircut in EUR     ']", namespace).text.strip())
            worstcase = float(ul_info.find("ns:Value[@id='Worstcase']", namespace).text.strip())
            prev_hc = float(ul_info.find("ns:Value[@id='PrevHC']", namespace).text.strip())
            crash_hc = float(ul_info.find("ns:Value[@id='CrashHC']", namespace).text.strip())

            market_value = float(ul_info.find("ns:Value[@id='MarketValue']", namespace).text.strip())
            theo_value = float(ul_info.find("ns:Value[@id='TheoValue']", namespace).text.strip())
            difference = float(ul_info.find("ns:Value[@id='Difference']", namespace).text.strip())

            isin_element = ul_info.find("ns:Identification[@id='ISIN']", namespace)
            isin = isin_element.text.strip() if isin_element is not None else "N/A"

            percentage = ((haircut / market_value) * 100) if market_value != 0 else 0

            underlying_data.append({

                "Underlying": underlying_id,
                "UL product": ul_product,
                "Reference product": reference_product,
                "Haircut in EUR": haircut,
                "Worstcase": worstcase,
                "PrevHC": prev_hc,
                "CrashHC": crash_hc,
                "MarketValue": market_value,
                "TheoValue": theo_value,
                "Difference": difference,
                "ISIN": isin,
                "Percentage": percentage,
                "Account Treecap": account_treecap,
                "Portfolio Manager": portfolio_manager,
                "abn_account": unique_account
            })

        except AttributeError as e:
            print(f"{Color.WARNING}Missing data in {file_path}: {e}{Color.ENDC}")

    return underlying_data


# ─────────────────────────────────────────────
# Database insert
# ─────────────────────────────────────────────
def insert_into_database(date, underlying_data):

    sql = """
        INSERT INTO clearing (
            date, name, account_treecap, portfolio_manager, abn_account,
            underlying, symbol_treecap, reference_product,
            haircut, worst_case, prev_hc,
            crash_hc, market_value, theo_value, difference,
            percentage, isin
        )
        VALUES (%s,'haircut',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    for data in underlying_data:

        params = (
            date,
            data["Account Treecap"],
            json.dumps(data["Portfolio Manager"]),
            data["abn_account"],
            data["Underlying"],
            data["UL product"],
            data["Reference product"],
            data["Haircut in EUR"],
            data["Worstcase"],
            data["PrevHC"],
            data["CrashHC"],
            data["MarketValue"],
            data["TheoValue"],
            data["Difference"],
            data["Percentage"],
            data["ISIN"]
        )

        try:

            cursor.execute(sql, params)

        except Exception as e:

            print(f"\n❌ SQL Error inserting record for {data.get('ISIN', 'N/A')}")
            print(e)

    conn.commit()


# ─────────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────────
for proc_date in working_days():

    proc_date_str = proc_date.strftime("%Y-%m-%d")
    proc_date_str_nodash = proc_date.strftime("%Y%m%d")

    dropbox_folder = os.path.join(dropbox_path, proc_date_str)

    if not os.path.exists(dropbox_folder):
        continue

    for file in os.listdir(dropbox_folder):

        if file.startswith("."):
            continue

        if not file.endswith(".xml"):
            continue

        if "DEF.6370" not in file and not any(trad in file for trad in trad_list(cursor)):
            continue

        file_path = os.path.join(dropbox_folder, file)

        # print(f"Found file: {file_path}")

        unique_account_match = re.search(r'DEF\.([\w.-]+\.TRAD\.\d{1,2})', file)

        unique_account = unique_account_match.group(1) if unique_account_match else "N/A"

        total = f'DEF.6370-{proc_date_str_nodash}.xml'
        total2 = f'DEF.6370.C6370-{proc_date_str_nodash}.xml'
        total3 = f'DEF.6370.C6372-{proc_date_str_nodash}.xml'

        if file == total:
            unique_account = '6370'
        elif file == total2:
            unique_account = '6370.TRAD-1'
        elif file == total3:
            unique_account = '6372'

        date_match = re.search(r'\d{8}', file)

        if not date_match:
            continue

        date_str = date_match.group()
        date = datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d')

        account_treecap, portfolio_manager = account(unique_account, cursor)

        if 'TRAD.' in unique_account:
            reformed_account = unique_account[-6:]
        else:
            reformed_account = unique_account

        if checkrows(date, account_treecap, portfolio_manager, reformed_account):

            print(f"{Color.WARNING}⚠ Data bestaat al voor {date} → skip{Color.ENDC}")
            continue

        data = parse_xml_file(file_path, account_treecap, portfolio_manager, reformed_account)

        insert_into_database(date, data)


# ─────────────────────────────────────────────
# Cleanup
# ─────────────────────────────────────────────
cursor.close()
conn.close()

print(f"{Color.OKGREEN}✔️ Alle haircut bestanden succesvol verwerkt.{Color.ENDC}")