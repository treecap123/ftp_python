# === PSET processing script (Railway version) ===

import os
import sys
import json
import pandas as pd
import numpy as np
from datetime import datetime as dt
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# ─────────────────────────────────────────────
# Railway working directory
# ─────────────────────────────────────────────
root_root_dir = os.getcwd()
sys.path.append(root_root_dir)

# ─────────────────────────────────────────────
# Imports
# ─────────────────────────────────────────────
from Functions.date.date_functions import working_days, select_date
from Functions.database.lookup import account, instrument
from Functions.connection.db_connection import get_cursor
from Functions.system.path.path import dropbox_path


# ─────────────────────────────────────────────
# DB connectie
# ─────────────────────────────────────────────
cursor, conn = get_cursor()


# ─────────────────────────────────────────────
# Column mapping
# ─────────────────────────────────────────────
column_mapping = {
    "processingdate": "date",
    "bccid": "BCCId",
    "account_treecap": "account_treecap",
    "portfolio_manager": "portfolio_manager",
    "abn_symbol": "abn_symbol",
    "category": "category",
    "deal": "deal",
    "symbol_treecap": "symbol_treecap",
    "clientid": "ClientId",
    "clientname": "ClientName",
    "account type": "account_type",
    "account": "Account",
    "account name": "account_name",
    "sub-account": "sub_account",
    "sub-account name": "sub_account_name",
    "scaid": "SCAId",
    "oppositeparty_oppositepartycode": "OppositeParty_OppositePartyCode",
    "product_listingexchange": "Product_ListingExchange",
    "product_productgroupcode": "Product_ProductGroupCode",
    "product_productgroupname": "Product_ProductGroupName",
    "product_symbol": "Product_Symbol",
    "product_isin": "Product_ISIN",
    "product_shortname": "Product_ShortName",
    "depot_depotid": "Depot_DepotId",
    "safekeeping_safekeepingid": "SafeKeeping_SafeKeepingId",
    "safekeeping_placeofsafekeeping": "SafeKeeping_PlaceofSafekeeping",
    "externalmember": "ExternalMember",
    "externalaccount": "ExternalAccount",
    "exchange_preferredtradingexchange": "Exchange_PreferredTradingExchange",
    "settlementquantity": "SettlementQuantity",
    "receivedsettlementquantity": "ReceivedSettlementQuantity",
    "transactiondate": "TransactionDate",
    "settlementdate": "SettlementDate",
    "buy sell": "buy_sell",
    "currency_currencycode": "Currency_CurrencyCode",
    "transactioncurrencypricingunit": "TransactionCurrencyPricingUnit",
    "transactiontypecode": "TransactionTypeCode",
    "settlementinstructionreference": "SettlementInstructionReference",
    "accruedinterest_value": "AccruedInterest_Value",
    "accruedinterest_valuedc": "AccruedInterest_ValueDC",
    "exchangefee_value": "ExchangeFee_Value",
    "exchangefee_valuedc": "ExchangeFee_ValueDC",
    "exchangefee_valuecur": "ExchangeFee_ValueCur",
    "effectivevalue_value": "EffectiveValue_Value",
    "effectivevalue_valuedc": "EffectiveValue_ValueDC",
    "effectivevalue_valuecur": "EffectiveValue_ValueCur",
    "settlementbalance_value": "SettlementBalance_Value",
    "settlementbalance_valuedc": "SettlementBalance_ValueDC",
    "receivedsettlement_amount": "ReceivedSettlement_Amount",
    "receivedsettlement_amountdc": "ReceivedSettlement_AmountDc",
    "settlementstatus": "SettlementStatus",
    "settlementreason": "SettlementReason",
    "settlementreasonnarrative": "SettlementReasonNarrative",
    "placeofsettlement": "place_of_settlement",
    "placeofsettlementshort": "place_of_settlement_short",
    "poolreference": "PoolReference",
    "exchangereference1": "ExchangeReference1",
    "exchangereference2": "ExchangeReference2",
    "ccpindicator": "CCPIndicator",
    "transactionprice": "TransactionPrice",
    "typeofsettledtransaction": "TypeOfSettledTransaction",
    "transactionorigin": "TransactionOrigin",
    "oppositepartybiccode": "OppositePartyBICCode",
    "instructionstatus": "InstructionStatus",
    "comment": "Comment",
}

abn_account_cols = ["clientid", "account type", "account"]
abn_symbol_col = ["product_symbol", "currency_currencycode"]


# ─────────────────────────────────────────────
# Backfill dates
# ─────────────────────────────────────────────
existing_dates = select_date("pset")


# ─────────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────────
for workday in working_days():

    if workday == dt.now().date():
        continue

    if workday in existing_dates:
        continue

    folder = os.path.join(dropbox_path, str(workday))

    print(f"\n🔥 PROCESS DATE → {workday}")

    if not os.path.isdir(folder):
        print(f"❌ Folder missing: {folder}")
        continue

    target_file = None

    for f in os.listdir(folder):

        if f.startswith("."):
            continue

        if "PSET Instructions" in f and f.endswith(".csv"):
            target_file = os.path.join(folder, f)
            break

    if not target_file:
        print("❌ No PSET file found")
        continue

    print(f"📄 Processing PSET file: {target_file}")

    df = pd.read_csv(target_file, dtype=object)
    df = df.replace({np.nan: None})
    df.columns = df.columns.str.strip().str.lower()

    if "processingdate" in df.columns:
        df["processingdate"] = pd.to_datetime(
            df["processingdate"], format="%Y%m%d"
        ).dt.strftime("%Y-%m-%d")

    # account lookup
    def enrich_account(row):

        parts = []

        for col in abn_account_cols:

            val = row.get(col)

            if val and str(val).strip():
                parts.append(str(val).replace(".0", "").strip())

        acc_key = " ".join(parts)

        acc, pm = account(acc_key, cursor)

        if isinstance(pm, (list, dict)):
            pm = json.dumps(pm)

        return pd.Series([acc, pm])

    df[["account_treecap", "portfolio_manager"]] = df.apply(enrich_account, axis=1)

    # abn_symbol
    df["abn_symbol"] = df[abn_symbol_col].apply(
        lambda r: " ".join(
            [str(v) for v in r if v not in [None, "", "nan"]]
        ).strip(),
        axis=1,
    )

    # instrument lookup
    def enrich_instrument(row):

        abn = row.get("abn_symbol")

        if not abn:
            return pd.Series([None, None, None])

        cat, deal, sym = instrument(abn, cursor)

        return pd.Series([cat, deal, sym])

    df[["category", "deal", "symbol_treecap"]] = df.apply(enrich_instrument, axis=1)

    df.rename(columns=column_mapping, inplace=True)

    cols = list(column_mapping.values())

    data = []

    for _, row in df.iterrows():

        row_data = []

        for c in cols:

            val = row.get(c)

            if pd.isna(val) or val == "":
                row_data.append(None)
            else:
                row_data.append(val)

        data.append(tuple(row_data))

    placeholders = ", ".join(["%s"] * len(cols))

    insert_sql = f"""
        INSERT INTO pset ({', '.join(cols)})
        VALUES ({placeholders})
    """

    try:

        cursor.executemany(insert_sql, data)

        conn.commit()

        print(f"✅ Inserted {len(data)} rows → PSET")

    except Exception as e:

        print(f"❌ ERROR: {e}")

        conn.rollback()


# ─────────────────────────────────────────────
# Close DB
# ─────────────────────────────────────────────
cursor.close()
conn.close()

print("🎉 Done PSET backfill processing")