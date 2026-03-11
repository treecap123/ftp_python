# === Adapted DPR processing script with working_days backfill and Railway sync ===

import os
import sys
import json
import pandas as pd
import numpy as np
from datetime import datetime as dt


import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Railway working directory
root_root_dir = os.getcwd()

# voeg FTP toe aan python path
sys.path.append(os.path.join(root_root_dir))


# === Imports ===
from Functions.date.date_functions import get_previous_workday, working_days, select_date
from Functions.connection.db_connection import get_cursor
from Functions.system.path.path import dropbox_path, root_dir
from Functions.database.lookup import account
from Functions.log_tools.logging import Color



# === ✅ Gebruik standaard cursors ===
mycursor, conn = get_cursor()



# === Column mapping for DPR table ===
column_mapping = {
    "processing date": "date",
    "account_treecap": "account_treecap",
    "portfolio_manager": "portfolio_manager",
    "client": "Client",
    "client name": "Client_Name",
    "account type": "Account_Type",
    "account": "Account",
    "account name": "Account_Name",
    "sub-account": "sub_account",
    "sub-account name": "Sub_account_Name",
    "ulv symbol": "ULV_Symbol",
    "description": "Description",
    "isin": "ISIN",
    "ulv isin": "ULV_ISIN",
    "exchange": "Exchange",
    "symbol": "Symbol",
    "abn_symbol": "abn_symbol",
    "strike price": "Strike_Price",
    "expiry date": "Expiry_Date",
    "put call": "Put_Call",
    "quantity long": "Quantity_Long",
    "quantity short": "Quantity_Short",
    "valuation price": "Valuation_Price",
    "valuation price currency": "Valuation_Price_Currency",
    "mark to market value": "Mark_To_Market_Value",
    "ote": "OTE",
    "external account": "External_Account",
    "counter party": "Counter_Party",
    "safekeeping": "Safekeeping",
    "product group": "Product_Group",
    "contract year month": "Contract_Year_Month",
    "contract size": "Contract_Size",
    "prompt date": "Prompt_Date",
    "unit of measurement": "Unit_Of_Measurement",
    "payrecote": "PayRecOTE",
    "payrecotecurrency": "PayRecOTECurrency",
    "previous valuation price": "Previous_Valuation_Price",
    "previous valuation price date": "Previous_Valuation_Price_Date",
    "variation margin": "Variation_Margin",
    "settledindicator": "SettledIndicator",
    "settlement date": "Settlement_Date",
    "depot_depotid": "Depot_DepotId",
    "accruedcoupon_value": "AccruedCoupon_Value",
    "accruedcoupon_valuedc": "AccruedCoupon_ValueDC",
    "accruedcoupon_valuecur": "AccruedCoupon_ValueCur",
    "pricing unit": "Pricing_Unit",
    "final settlement date": "Final_Settlement_Date",
    "ulv closing price": "ULV_Closing_Price",
    "ulv closing price currency": "ULV_Closing_Price_Currency",
    "option key": "option_key"

}

# Kolommen die numeriek zijn in de DB
numeric_columns = [
    "Strike_Price", "Quantity_Long", "Quantity_Short", "Valuation_Price",
    "Mark_To_Market_Value", "OTE", "Contract_Size",
    "Variation_Margin", "AccruedCoupon_Value", "AccruedCoupon_ValueDC",
    "AccruedCoupon_ValueCur", "ULV_Closing_Price"
]


def master(date, abn, account_code, portfolio):
    """Controleert of instrument bestaat in master_instrument, anders toevoegen."""
    sql = """
    SELECT category, deal, symbol_treecap
    FROM master_instrument
    WHERE abn_symbol = %s
    """
    mycursor.execute(sql, (abn,))
    rows = mycursor.fetchall()
    result = rows[0] if rows else None

    if result is None:
        insert_sql = """
        INSERT INTO master_instrument
            (date, abn_symbol, account_treecap, portfolio_manager)
        VALUES (%s, %s, %s, %s)
        """
        mycursor.execute(insert_sql, (date, abn, account_code, portfolio))
        conn.commit()
        return (None, None, None)
    else:
        return result


def make_abn_symbol(row):
    """Genereert abn_symbol en zorgt dat master_instrument wordt bijgewerkt."""
    parts = []
    for key in ["Symbol", "Strike_Price", "Expiry_Date", "Put_Call", "Valuation_Price_Currency"]:
        val = row.get(key)
        if pd.notna(val) and str(val).strip():
            parts.append(str(val).strip())
    if row.get("Counter_Party") == "CAAC":
        parts.append("CAAC")
    abn = " ".join(parts).replace(".0", "")
    master(row["date"], abn, row["account_treecap"], row["portfolio_manager"])
    return abn


def make_option_key(row):

    print("\n====================")
    # print("Processing row:", row)

    required = [
        "Symbol",
        "Expiry_Date",
        "Put_Call",
        "Strike_Price",
        "Valuation_Price_Currency",
    ]

    # Check required fields
    for key in required:
        val = row.get(key)
        if val is None or str(val).strip() == "":
            print(f"[FAIL] Missing required field: {key} | Value: {val}")
            return None
        else:
            print(f"[OK] {key} = {val}")

    # Symbol
    symbol = str(row["Symbol"]).strip()
    print("[STEP] Parsed symbol:", symbol)

    # Expiry
    try:
        expiry_raw = str(int(float(row["Expiry_Date"])))
        print("[STEP] Raw expiry numeric:", expiry_raw)

        expiry_dt = dt.strptime(expiry_raw, "%Y%m%d")
        expiry = expiry_dt.strftime("%m/%d/%y")

        print("[STEP] Formatted expiry:", expiry)

    except Exception as e:
        print("[FAIL] Expiry conversion failed:", e)
        return None

    # Put/Call
    putcall_raw = str(row["Put_Call"]).strip().upper()
    print("[STEP] Raw Put/Call:", putcall_raw)

    if putcall_raw == "CALL":
        putcall = "C"
    elif putcall_raw == "PUT":
        putcall = "P"
    else:
        print("[FAIL] Invalid Put/Call value:", putcall_raw)
        return None

    print("[STEP] Converted Put/Call:", putcall)

    # Strike
    try:
        strike_float = float(row["Strike_Price"])
        print("[STEP] Raw strike float:", strike_float)

        if strike_float.is_integer():
            strike = str(int(strike_float))
            print("[STEP] Strike is integer, formatted as:", strike)
        else:
            strike = str(strike_float)
            print("[STEP] Strike is decimal, formatted as:", strike)

    except Exception as e:
        print("[FAIL] Strike conversion failed:", e)
        return None

    # Currency
    ccy_raw = str(row["Valuation_Price_Currency"]).strip().upper()
    print("[STEP] Raw currency:", ccy_raw)

    if ccy_raw == "USD":
        country = "US"
    else:
        country = ccy_raw  # fallback

    print("[STEP] Country code used:", country)

    final_key = f"{symbol} {country} {expiry} {putcall}{strike} Equity"

    print("[SUCCESS] Final Option Key:", final_key)
    print("====================\n")

    return final_key

# === Main processing loop: backfill missing DPR dates ===
existing_dates = select_date('dpr')

for workday in working_days():
    if workday == dt.now().date():
        continue
    if workday not in existing_dates and workday:
        folder = os.path.join(dropbox_path, str(workday))

        print(f"Processing DPR for date: {workday} in folder: {folder}")


        for filename in os.listdir(folder):
            if "Daily Position" in filename:
                df = pd.read_csv(os.path.join(folder, filename))
                df = df.replace({np.nan: None})
                df.columns = df.columns.str.strip().str.lower()

                # Voeg account en portfolio toe
                def enrich_account(row):
                    acc, pm = account(f"{row['client']}_{row['account type']}_{row['account']}", mycursor)
                    if isinstance(pm, (list, dict)):
                        pm = json.dumps(pm)
                    return pd.Series([acc, pm])

                df[['account_treecap', 'portfolio_manager']] = df.apply(enrich_account, axis=1)

                # Datum formatteren en kolomnamen aanpassen
                df['processing date'] = pd.to_datetime(df['processing date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
                df.rename(columns=column_mapping, inplace=True)

                # abn_symbol genereren
                df.insert(df.columns.get_loc("Symbol") + 1, "abn_symbol", "")
                df["abn_symbol"] = df.apply(make_abn_symbol, axis=1)
                # print(df.columns)
                df["option_key"] = df.apply(make_option_key, axis=1)
                row = df.loc[42]
                # print(
                #     row["Symbol"],
                #     row["Put_Call"],
                #     row["Strike_Price"],
                #     row["Expiry_Date"],
                #     row["option_key"],
                # )
                # print(df[["Symbol", "Put_Call", "Strike_Price", "Expiry_Date", "option_key"]].head(10))
                # print(df["option_key"])
                # print(df.columns)

                # Numerieke kolommen corrigeren
                for col in numeric_columns:
                    if col in df.columns:
                        df[col] = df[col].apply(lambda x: None if x in ("", None) else x)

                # Batch insert voorbereiden
                cols = list(column_mapping.values())
                placeholders = ", ".join(["%s"] * len(cols))
                insert_sql = f"INSERT INTO dpr ({', '.join(cols)}) VALUES ({placeholders})"
                # print(insert_sql)

                data = []
                for _, row in df.iterrows():
                    row_data = []
                    for c in cols:
                        val = row.get(c)
                        if c in numeric_columns and (val == "" or pd.isna(val)):
                            row_data.append(None)
                        else:
                            row_data.append(val)
                    data.append(tuple(row_data))

                # === ✅ Insert in lokale DB
                mycursor.executemany(insert_sql, data)
                conn.commit()


print("✅ Done DPR backfill processing")

# === Sluit verbindingen netjes af ===
mycursor.close()
conn.close()

