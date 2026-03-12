import os
import sys
import json
import pandas as pd
import numpy as np
from datetime import datetime, date as dt_date
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Railway working directory
root_root_dir = os.getcwd()

# voeg FTP toe aan python path
sys.path.append(os.path.join(root_root_dir))

# ─────────────────────────────────────────────
# IMPORTS
# ─────────────────────────────────────────────

from global_functions.system.path.path import dropbox_path
from global_functions.database_connection.connection import get_cursor
from global_functions.date.date_functions import working_days
from global_functions.database.lookup import account, instrument
from global_functions.log_tools.logging import Color


# ─────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────

mycursor, conn = get_cursor()


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def get_account_pm(row):

    acc, pm = account(f"{row['clientname']}_{row['strategy']}", mycursor)

    if pm:
        return acc, json.dumps([pm]) if isinstance(pm, str) else json.dumps(pm)

    return acc, None


def select_date(table):

    mycursor.execute(f"SELECT DISTINCT date FROM {table}")

    result = mycursor.fetchall()

    normalized = []

    for r in result:

        d = r["date"]

        if isinstance(d, (datetime, dt_date)):
            normalized.append(d.strftime("%Y-%m-%d"))

        elif isinstance(d, str):
            normalized.append(d)

    return normalized


dates = select_date("positions_synthetics")


# ─────────────────────────────────────────────
# CSV → DB mapping
# ─────────────────────────────────────────────

columns = [
"date","clientname","fund","swap","strategy","security","ric","type","tradeccy","swapccy",
"positionid","tdside","tdquantity","tdavgcostulccy","tdavgcostswapccy",
"tdcostnotionalulccy","tdcostnotionalswapccy","mtmpriceulccy","mtmpricedate",
"fx","mtmpriceswapccy","tdmarketnotionalulccy","tdmarketnotionalswapccy",
"tdmtmulccy","tdmtmswapccy","vdquantity","vdside","vdavgcostulccy",
"vdavgcostswapccy","vdcostnotionalulccy","vdcostnotionalswapccy",
"vdmtmnotionalulccy","vdmtmnotionalswapccy","vdmtmswapccy",
"accruedborrowcost","accruednotionalfinancing","accruedtradinggains","accruedfees",
"accruedcashfinancing","accruedresetcash","accrueddividends","totalaccruals",
"tdtotalplswapccy","avgspread","avgborrowcost","avgdivrate","benchmarkname",
"daycount","unwindmethodology","country","region","preflistingid","bbgticker","sedol",
"isin","swapenddate","divpaytype","tradinggainspaytype","financingpaytype",
"borrowcostpaytype","independentamountcomputationmode","independentamountpercent",
"independentamountswapccy","bookingtype","account_treecap","portfolio_manager",
"category","deal","symbol_treecap","abn_symbol"
]


column_mapping = {
'report date': 'date',
'client name': 'clientname',
'trade ccy': 'tradeccy',
'swap ccy': 'swapccy',
'position id': 'positionid',
'td side': 'tdside',
'td quantity': 'tdquantity',
'td avg cost (u/l ccy)': 'tdavgcostulccy',
'td avg cost (swap ccy)': 'tdavgcostswapccy',
'td cost notional (u/l ccy)': 'tdcostnotionalulccy',
'td cost notional (swap ccy)': 'tdcostnotionalswapccy',
'mtm price (u/l ccy)': 'mtmpriceulccy',
'mtm price date': 'mtmpricedate',
'mtm price (swap ccy)': 'mtmpriceswapccy'
}


# ─────────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────────

for workday in working_days():

    workday_str = workday.strftime("%Y-%m-%d")

    if workday_str in dates or workday == datetime.now().date():
        continue

    folder = os.path.join(dropbox_path, workday_str)

    if not os.path.exists(folder):

        print(f"{Color.WARNING}⚠ folder missing: {folder}{Color.ENDC}")

        continue


    for file in os.listdir(folder):

        if "Positions-0000006372" not in file or not file.endswith(".csv"):
            continue


        print(f"{Color.OKBLUE}📄 processing: {file} ({workday_str}){Color.ENDC}")

        file_path = os.path.join(folder, file)

        df = pd.read_csv(file_path)

        df = df.replace({np.nan: None, '': None})

        df.columns = df.columns.str.strip().str.lower()

        df.rename(columns=column_mapping, inplace=True)


        df["account_treecap"], df["portfolio_manager"] = zip(*df.apply(get_account_pm, axis=1))


        df["category"], df["deal"], df["symbol_treecap"] = zip(*df.apply(
            lambda row: instrument(f"{row.get('ric','')} {row.get('tradeccy','')}", mycursor),
            axis=1
        ))


        df["abn_symbol"] = df['ric'].astype(str).str.strip() + " " + df['tradeccy'].astype(str).str.strip()


        try:
            df = df[columns]

        except KeyError as e:

            print(f"{Color.FAIL}❌ missing columns: {e}{Color.ENDC}")

            continue


        sql_columns = ', '.join([f'`{col}`' for col in columns])

        placeholders = ', '.join(['%s'] * len(columns))


        sql = f"""
        INSERT INTO positions_synthetics ({sql_columns})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE
        tdquantity = VALUES(tdquantity),
        tdavgcostulccy = VALUES(tdavgcostulccy),
        tdavgcostswapccy = VALUES(tdavgcostswapccy),
        tdcostnotionalulccy = VALUES(tdcostnotionalulccy),
        tdcostnotionalswapccy = VALUES(tdcostnotionalswapccy),
        mtmpriceulccy = VALUES(mtmpriceulccy),
        mtmpriceswapccy = VALUES(mtmpriceswapccy)
        """


        data = [tuple(row) for row in df.to_numpy()]


        try:

            mycursor.executemany(sql, data)

            conn.commit()

            print(f"{Color.OKGREEN}✅ {len(data)} rows inserted for {workday_str}{Color.ENDC}")


        except Exception as e:

            conn.rollback()

            print(f"{Color.FAIL}❌ insert error: {e}{Color.ENDC}")


# ─────────────────────────────────────────────
# CLEANUP
# ─────────────────────────────────────────────

mycursor.close()
conn.close()

print(f"{Color.OKGREEN}✔ synthetics backfill complete{Color.ENDC}")