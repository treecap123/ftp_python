import pandas as pd
import numpy as np
from datetime import datetime as date
import os
import importlib.util
import json
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Railway working directory
root_root_dir = os.getcwd()

# voeg project root toe aan python path
sys.path.append(os.path.join(root_root_dir))

# ─────────────────────────────────────────────
# IMPORTS
# ─────────────────────────────────────────────

from Functions.system.path.path import tables_path, dropbox_path
from Functions.date.date_functions import working_days
from Functions.connection.db_connection import get_cursor
from Functions.database.lookup import account, instrument
from Functions.log_tools.logging import Color

# ─────────────────────────────────────────────
# Load tables config
# ─────────────────────────────────────────────

spec = importlib.util.spec_from_file_location("tables", tables_path)
tables = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tables)

werkdagen = working_days()

print(f"Working days to check: {werkdagen}")

# ─────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────

try:
    mycursor, conn = get_cursor()
    print(f"{Color.OKBLUE}✅ Connected to Railway database{Color.ENDC}")
except Exception as e:
    print(f"{Color.FAIL}DB connection failed: {e}{Color.ENDC}")
    sys.exit()


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def select_date(table, date_column="date"):

    sql = f"SELECT DISTINCT `{date_column}` FROM {table}"

    print(sql)

    mycursor.execute(sql)

    result = mycursor.fetchall()

    return [row[date_column] for row in result]


def clean_records(records):

    cleaned = []

    for i, row in enumerate(records):

        new_row = []

        for val in row:

            if pd.isna(val) or str(val).lower() in ("nan", "none", ""):
                new_row.append(None)

            else:
                new_row.append(val)

        cleaned.append(tuple(new_row))

        if i < 3:
            print(f"DEBUG row {i}: {new_row}")

    return cleaned


def extract_date_from_path(path: str):

    folder = os.path.basename(os.path.dirname(path))

    return folder


# ─────────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────────

for table_name, info in tables.tables.items():

    print(f"Controleer tabel: {table_name}")

    if "ProcessingDate" in info:
        date_col = info["ProcessingDate"]
    else:
        date_col = "date"

    existing_dates = set(select_date(info['table'], date_col))

    print(f"  Bestaande datums in {table_name}: {existing_dates}")

    for werkdag in werkdagen:

        print(f"Checking date: {werkdag}")

        if werkdag not in existing_dates:

            print(f"{Color.WARNING}Missing {werkdag} in {table_name}{Color.ENDC}")

            folder_path = os.path.join(dropbox_path, str(werkdag))

            if not os.path.exists(folder_path):

                print(f"{Color.WARNING}Folder not found: {folder_path}{Color.ENDC}")

                continue

            for file in os.listdir(folder_path):

                if "UpcomingDividends-0000006372" in file:
                    print(f"{Color.WARNING}Skipping file: {file}{Color.ENDC}")
                    continue

                if table_name in file and file.endswith(info.get('extension', '')):

                    file_path = os.path.join(folder_path, file)

                    print(f"{Color.OKGREEN}Found file: {file_path}{Color.ENDC}")

                    df = pd.read_csv(file_path, dtype=object)

                    if df.empty:

                        print(f"{Color.WARNING}Skipping empty file: {file_path}{Color.ENDC}")

                        continue

                    # --- special case daily loan rate ---
                    if table_name.lower() == "daily loan rate" and "date" not in df.columns:

                        workday_str = extract_date_from_path(file_path)

                        print(f"{Color.CYAN}Adding date column: {workday_str}{Color.ENDC}")

                        df.insert(0, "date", workday_str)

                    cols_to_insert = []

                    if info.get('account_index'):
                        cols_to_insert += ['account_treecap', 'portfolio_manager']

                    if info.get('abn_index'):
                        cols_to_insert += ['category', 'deal', 'symbol_treecap', 'abn_symbol']

                    for col in reversed(cols_to_insert):

                        if col not in df.columns:
                            df.insert(loc=1, column=col, value=None)

                    # ─────────────────────────
                    # Lookups
                    # ─────────────────────────

                    for idx, row in df.iterrows():

                        abn_data = ""
                        account_data = ""

                        if info.get('abn_index'):

                            abn_cols = row[info['abn_index']].dropna().astype(str)

                            abn_data = " ".join(abn_cols[abn_cols != ""].tolist()).strip()

                        if info.get('account_index'):

                            acc_cols = row[info['account_index']].dropna().astype(str)

                            account_data = " ".join(acc_cols[acc_cols != ""].tolist()).strip()

                        # account lookup

                        account_treecap, portfolio_manager = account(account_data, mycursor)

                        if portfolio_manager is None:
                            portfolio_manager = []

                        elif isinstance(portfolio_manager, str):
                            portfolio_manager = [portfolio_manager]

                        df.at[idx, "account_treecap"] = account_treecap

                        df.at[idx, "portfolio_manager"] = json.dumps(portfolio_manager, ensure_ascii=False)

                        # instrument lookup

                        if abn_data:

                            category, deal, symbol_treecap = instrument(abn_data, mycursor)

                            df.at[idx, "category"] = category

                            df.at[idx, "deal"] = deal

                            df.at[idx, "symbol_treecap"] = symbol_treecap

                            df.at[idx, "abn_symbol"] = abn_data

                    # ─────────────────────────
                    # SQL
                    # ─────────────────────────

                    print("DF columns:", list(df.columns))

                    print("DB columns:", info["column_names"])

                    sql_columns = ", ".join(f"`{col}`" for col in info["column_names"])

                    sql_placeholders = ", ".join(["%s"] * len(info["column_names"]))

                    sql = f"""
                    INSERT INTO {info['table']} ({sql_columns})
                    VALUES ({sql_placeholders})
                    """

                    records = df.values.tolist()

                    records = clean_records(records)

                    print(f"Inserting {len(records)} rows into {info['table']}")

                    try:

                        mycursor.executemany(sql, records)

                        conn.commit()

                        print(f"{Color.OKGREEN}Inserted {len(records)} rows into {info['table']}{Color.ENDC}")

                    except Exception as e:

                        conn.rollback()

                        print(f"{Color.FAIL}ERROR inserting into {info['table']}: {e}{Color.ENDC}")


# ─────────────────────────────────────────────
# CLEANUP
# ─────────────────────────────────────────────

mycursor.close()
conn.close()

print(f"{Color.OKGREEN}✔ All tables processed successfully.{Color.ENDC}")