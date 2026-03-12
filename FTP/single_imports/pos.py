import os
import sys
import json
import pandas as pd
import numpy as np
from datetime import datetime
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Railway working directory
root_root_dir = os.getcwd()

# voeg FTP toe aan python path
sys.path.append(os.path.join(root_root_dir))

# =========================================================
# IMPORTS
# =========================================================

from global_functions.date.date_functions import working_days, select_date
from global_functions.database_connection.connection import get_cursor
from global_functions.database.lookup import account
from global_functions.log_tools.logging import Color
from global_functions.system.path.path import dropbox_path

# =========================================================
# DATABASE
# =========================================================

mycursor, conn = get_cursor()

# =========================================================
# MASTER LOOKUP
# =========================================================

def get_master(abn_symbol):

    sql = """
        SELECT category, deal, symbol_treecap
        FROM pos_instrument
        WHERE abn_symbol = %s
    """

    mycursor.execute(sql, (abn_symbol,))
    row = mycursor.fetchone()

    if row:
        return row['category'], row['deal'], row['symbol_treecap']

    insert_sql = """
        INSERT INTO pos_instrument (abn_symbol)
        VALUES (%s)
    """

    mycursor.execute(insert_sql, (abn_symbol,))
    conn.commit()

    return (None, None, None)

# =========================================================
# COLUMN MAPPING
# =========================================================

column_mapping = {
    'P&L Indicator': 'pl_indicator',
    'P&L Amount': 'pl_amount',
    'Original Currency': 'original_currency',
    'P&L Amount Client Currency': 'pl_amount_client_currency',
    'Client Currency': 'client_currency',
    'Conversion Rate': 'conversion_rate',
    'Cash Amount Change Value': 'CashAmountChange_Value',
    'Cash Amount Change DC': 'CashAmountChangeDC',
    'Cash Amount New Value': 'CashAmountNew_Value',
    'Cash Amount New DC': 'CashAmountNewDC',
    'Cash Amount New Value Cur': 'CashAmountNew_ValueCur',
    'CashDescription': 'CashDescription',
    'Product Symbol': 'Product_Symbol',
    'Product Expiry': 'Product_Expiry',
    'FutureStyleIndicatio': 'FutureStyleIndication',
}

KEY = "POS"

# =========================================================
# POS TABLE COLUMNS
# =========================================================

COLUMNS = [
'date','account_treecap','portfolio_manager','category','deal','symbol_treecap','abn_symbol',
'BCCId','ClientID','AccountID','ClientName','Currency_CurrencyCode','Currency_CurrencyPricingUnit',
'CashAmountIdentifier','JournalAccount','CashAmountChange_Value','CashAmountChangeDC',
'CashAmountChangeCurrency','CashAmountNew_Value','CashAmountNewDC','CashAmountNew_ValueCur',
'CurrencyPrice','CashDescription','OppositeParty_OppositePartyCode','ListingExchange',
'Product_ProductGroupCode','Product_ProductGroupName','Product_Type','Product_Symbol',
'Product_Expiry','Product_Strike','Product_InstrumentIDType','Product_InstrumentIDValue',
'ConversionTradingunit','DeliveredTradingunit','SettlementType','OptionStyle',
'FutureStyleIndication','ULV_InstrumentType','ULV_PlaceOfTrade','ULV_Symbol','ULV_Expiry',
'ULV_PreferredTradingExchange','ULV_InstrumentIDType','ULV_InstrumentIDValue',
'ULV_SafeKeepingId','SafeKeeping_SafeKeepingId','Depot_DepotId','ExternalMember',
'ExternalAccount','Exchange_PreferredTradingExchange','TradeDate','SettlementDate',
'SettlementStatus','GrossNetCode','QuantityLong','QuantityShort','InternalPositionReference',
'AdjustmentPrice','ValuationPrice','ValuationPriceFractional','ValuationPriceCurrency_CurrencyCode',
'ValuationPriceCurrency_CurrencyPricingUnit','TransactionPrice','TransactionPriceFractional',
'TransactionCurrency_CurrencyCode','TransactionCurrency_CurrencyPricingUnit',
'OpenTradeEquity_Value','OpenTradeEquity_ValueDC','OpenTradeEquity_ValueCur',
'AccruedCoupon_Value','AccruedCoupon_ValueDC','AccruedCoupon_ValueCur',
'GrossAmount_Value','GrossAmount_ValueDC','GrossAmount_ValueCur','NetAmount_Value',
'NetAmount_ValueDC','NetAmount_ValueCur','MarktoMarket_Value','MarktoMarket_ValueDC',
'MarktoMarket_ValueCur','Delta','Tradingunit','ConversionRatio','ContractSize',
'LastMovementDate','PositionType','BookDate','Type','pl_indicator','pl_amount',
'original_currency','pl_amount_client_currency','client_currency','conversion_rate'
]

col_names = ', '.join(f"`{c}`" for c in COLUMNS)
placeholders = ', '.join(['%s'] * len(COLUMNS))

insert_sql = f"INSERT INTO pos ({col_names}) VALUES ({placeholders})"

# =========================================================
# EXISTING DATES
# =========================================================

existing_dates = set(select_date('pos'))

print("POS import started")

# =========================================================
# BACKFILL LOOP
# =========================================================

for workday in working_days():

    if workday == datetime.now().date():
        continue

    if workday in existing_dates:
        continue

    print(f"{Color.WARNING}Processing {workday}{Color.ENDC}")

    folder = os.path.join(dropbox_path, str(workday))

    if not os.path.isdir(folder):
        print(f"Folder missing: {folder}")
        continue

    for fname in os.listdir(folder):

        if KEY not in fname or not fname.lower().endswith(".csv"):
            continue

        print(f"Processing file: {fname}")

        file_path = os.path.join(folder, fname)

        df = pd.read_csv(file_path)

        df.columns = df.columns.str.strip()

        df['date'] = pd.to_datetime(df['ProcessingDate'], format='%Y%m%d').dt.date

        df.drop(columns=['ProcessingDate'], inplace=True)

        df = df.replace({np.nan: 0})

        df = df[df['P&L Indicator'] == 'Y']

        df['account_key'] = df['AccountID'].astype(str).str.replace(" ", "_")

        df[['account_treecap','portfolio_manager']] = df['account_key'].apply(
            lambda x: pd.Series(account(x, cursor=mycursor))
        )

        df['Product_Symbol'] = (
            df['Product_Symbol']
            .astype(str)
            .str.strip()
            .replace({'0':'COST'})
        )

        df['abn_symbol'] = df.apply(
            lambda r: 'COST'
            if r['Product_Symbol']=='COST'
            else f"{r['Product_Symbol']} {r['Currency_CurrencyCode']}",
            axis=1
        )

        mids = df.apply(
            lambda r: get_master(r['abn_symbol']),
            axis=1,
            result_type='expand'
        )

        df[['category','deal','symbol_treecap']] = mids

        df.rename(columns=column_mapping, inplace=True)

        df['portfolio_manager'] = df['portfolio_manager'].apply(
            lambda x: json.dumps(x) if not isinstance(x,str) else json.dumps(x.strip())
        )

        df = df.replace({np.nan:''})

        data = [tuple(r[c] for c in COLUMNS) for _, r in df.iterrows()]

        print(f"{Color.OKBLUE}Prepared {len(data)} records{Color.ENDC}")

        try:

            mycursor.executemany(insert_sql, data)
            conn.commit()

            print(f"{Color.OKGREEN}{len(data)} records inserted{Color.ENDC}")

        except Exception as e:

            conn.rollback()

            print(f"{Color.WARNING}Insert error: {e}{Color.ENDC}")

conn.close()
mycursor.close()

print("POS import finished")