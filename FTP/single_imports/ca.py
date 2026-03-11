# === import_ca_xml_into_clearing.py (geoptimaliseerde versie) ===

import os
import sys
import json
import xml.etree.ElementTree as ET
import pymysql
from datetime import datetime

import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Railway working directory
root_root_dir = os.getcwd()

# voeg FTP toe aan python path
sys.path.append(root_root_dir)

from Functions.date.date_functions import get_previous_workday, working_days
from Functions.connection.db_connection import get_cursor
from Functions.system.path.path import dropbox_path, root_dir
from Functions.database.lookup import account
from Functions.log_tools.logging import Color

try:
    mycursor, conn = get_cursor()
    print(f"{Color.OKBLUE}✅ Verbonden met Railway database{Color.ENDC}")
except Exception as e:
    print(f"{Color.WARNING}⚠ Railway-verbinding mislukt: {e}{Color.ENDC}")
    exit()



# ─────────────────────────────────────────────
#  Hulpfuncties
# ─────────────────────────────────────────────
def select_date(cursor, table):
    sql = f"SELECT DISTINCT date FROM {table}"
    cursor.execute(sql)
    result = cursor.fetchall()
    return [r["date"] for r in result]


def get_previous_workday_from_db(cursor, start_date=None):
    """
    Haalt vorige werkdag op uit de database.
    """
    if start_date is None:
        start_date = datetime.today().date()
    elif isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    elif isinstance(start_date, datetime):
        start_date = start_date.date()

    sql = """
        SELECT date AS d
        FROM working_day
        WHERE date < %s
          AND is_workday = 0
        ORDER BY date DESC
        LIMIT 1
    """
    cursor.execute(sql, (start_date,))
    row = cursor.fetchone()
    return row["d"].strftime("%Y-%m-%d") if row and row["d"] else None


# ─────────────────────────────────────────────
#  Corporate Actions Import
# ─────────────────────────────────────────────
def import_ca_xml_into_clearing():
    """
    Process Corporate Actions (CA) XML files en insert data in zowel lokale als Railway database.
    """

    existing_dates = select_date(mycursor, "corporate_actions")

    for work_day in working_days():
        today_str = datetime.today().strftime("%Y-%m-%d")

        if work_day not in existing_dates and work_day != today_str:
            # print(f"📅 Verwerking gestart voor {work_day}...")
            dropbox_folder = os.path.join(dropbox_path, work_day.strftime("%Y-%m-%d"))

            if not os.path.exists(dropbox_folder):
                # print(f"⚠️ Map niet gevonden: {dropbox_folder}")
                continue

            for file in os.listdir(dropbox_folder):
                print(f"🔍 Controleren bestand: {file}")
                if file.startswith("."):
                    continue
                if "3182-C3182-CA (L)" not in file:
                    continue

                print(f"✅ Bestand geselecteerd voor verwerking: {file}")
                full_path = os.path.join(dropbox_folder, file)
                print(f"📄 Verwerken van bestand: {full_path}")

                try:
                    tree = ET.parse(full_path)
                    root = tree.getroot()
                except Exception as e:
                    print(f"❌ XML-fout in {file}: {e}")
                    continue

                for account_actions in root.findall(".//AccountCorporateActions"):
                    bcc_id = account_actions.attrib.get("BCCId", "")
                    client_id = account_actions.attrib.get("ClientId", "")
                    account_id = account_actions.attrib.get("AccountID", "")
                    client_name = account_actions.attrib.get("ClientName", "")

                    account_treecap, portfolio_manager = account(account_id, mycursor)

                    if not portfolio_manager or str(portfolio_manager).strip() == "":
                        portfolio_manager_json = None
                    elif "_" in portfolio_manager or "&" in portfolio_manager:
                        portfolio_manager_json = json.dumps(portfolio_manager.replace("&", "_").split("_"))
                    else:
                        portfolio_manager_json = json.dumps(portfolio_manager)

                    for ca in account_actions.findall(".//CorporateAction"):
                        processing_date_raw = ca.findtext("ProcessingDate", default=get_previous_workday_from_db(mycursor))

                        try:
                            processing_date = datetime.strptime(processing_date_raw, "%Y%m%d").strftime("%Y-%m-%d")
                        except Exception:
                            processing_date = processing_date_raw

                        product = ca.find("Product")
                        product_group_code = product.findtext("ProductGroupCode", default="") if product is not None else ""
                        product_group_name = product.findtext("ProductGroupName", default="") if product is not None else ""
                        symbol = product.findtext("Symbol", default="") if product is not None else ""
                        instrument_id_type = product.findtext("InstrumentIDType", default="") if product is not None else ""
                        instrument_id_value = product.findtext("InstrumentIDValue", default="") if product is not None else ""

                        depot_id = ca.findtext("DepotId", default="")
                        safe_keeping_id = ca.findtext("SafeKeepingId", default="")
                        product_short_name = ca.findtext("ProductShortName", default="")

                        currency_element = ca.find("Currency")
                        currency_code = currency_element.findtext("CurrencyCode", default="") if currency_element is not None else ""

                        exdividend_date = ca.findtext("ExdividendDate", default="")
                        recorddate = ca.findtext("Recorddate", default="")
                        internal_reference = ca.findtext("InternalReference", default="")

                        try:
                            dividend_sequence_number = int(ca.findtext("DividendSequenceNumber", default="0"))
                        except ValueError:
                            dividend_sequence_number = 0

                        dividend_status = ca.findtext("DividendStatus", default="")
                        solz_indicator = ca.findtext("SolzIndicator", default="")
                        ca_event_indicator = ca.findtext("CAEventIndicator", default="")

                        try:
                            dividend_payment_year = int(ca.findtext("DividendPaymentYear", default="0"))
                        except ValueError:
                            dividend_payment_year = 0

                        dividend_pay_date = ca.findtext("DividendPayDate", default="")

                        try:
                            quantity_settled_no_tax = int(ca.findtext("QuantitySettledNoTax", default="0"))
                        except ValueError:
                            quantity_settled_no_tax = 0

                        quantity_settled_no_tax_ls = ca.findtext("QuantitySettledNoTax_LS", default="")

                        try:
                            quantity_unsettled = int(ca.findtext("QuantityUnsettled", default="0"))
                        except ValueError:
                            quantity_unsettled = 0

                        quantity_unsettled_ls = ca.findtext("QuantityUnsettled_LS", default="")

                        try:
                            borrowed_lended_dividend_amount = float(ca.findtext("BorrowedLendedDividendAmount", default="0"))
                        except ValueError:
                            borrowed_lended_dividend_amount = 0.0

                        borrowed_lended_dividend_amount_dc = ca.findtext("BorrowedLendedDividendAmountDC", default="")
                        borrowed_lended_dividend_amount_cur = ca.findtext("BorrowedLendedDividendAmountCur", default="")

                        dividend_amount_indicator = ca.findtext("DividendAmountIndicator", default="")
                        ca_event_reference = ca.findtext("CAEventReference", default="")

                        # Dividend element
                        dividend_element = ca.find("Dividend")
                        if dividend_element is not None:
                            try:
                                dividend_value = float(dividend_element.findtext("Value", default="0"))
                            except ValueError:
                                dividend_value = 0.0
                            dividend_value_dc = dividend_element.findtext("ValueDC", default="")
                            dividend_value_cur = dividend_element.findtext("ValueCur", default="")
                        else:
                            dividend_value, dividend_value_dc, dividend_value_cur = 0.0, "", ""

                        # DividendCash element
                        dividend_cash_element = ca.find("DividendCash")
                        if dividend_cash_element is not None:
                            try:
                                dividend_cash_value = float(dividend_cash_element.findtext("Value", default="0"))
                            except ValueError:
                                dividend_cash_value = 0.0
                            dividend_cash_value_dc = dividend_cash_element.findtext("ValueDC", default="")
                            dividend_cash_value_cur = dividend_cash_element.findtext("ValueCur", default="")
                        else:
                            dividend_cash_value, dividend_cash_value_dc, dividend_cash_value_cur = 0.0, "", ""

                        try:
                            tax_amount = float(ca.findtext("TaxAmount", default="0"))
                        except ValueError:
                            tax_amount = 0.0

                        values = (
                            bcc_id, client_id, account_id, client_name, account_treecap, portfolio_manager_json,
                            processing_date, product_group_code, product_group_name, symbol, instrument_id_type,
                            instrument_id_value, depot_id, safe_keeping_id, product_short_name, currency_code,
                            exdividend_date, recorddate, internal_reference, dividend_sequence_number, dividend_status,
                            solz_indicator, ca_event_indicator, dividend_payment_year, dividend_pay_date,
                            quantity_settled_no_tax, quantity_settled_no_tax_ls, quantity_unsettled, quantity_unsettled_ls,
                            borrowed_lended_dividend_amount, borrowed_lended_dividend_amount_dc,
                            borrowed_lended_dividend_amount_cur, dividend_amount_indicator, ca_event_reference,
                            dividend_value, dividend_value_dc, dividend_value_cur, dividend_cash_value,
                            dividend_cash_value_dc, dividend_cash_value_cur, tax_amount
                        )

                        sql = """
                            INSERT INTO corporate_actions (
                                bcc_id, client_id, account_id, client_name, account_treecap, portfolio_manager,
                                date, product_group_code, product_group_name, symbol, instrument_id_type, instrument_id_value,
                                depot_id, safe_keeping_id, product_short_name, currency_code, exdividend_date, recorddate, internal_reference,
                                dividend_sequence_number, dividend_status, solz_indicator, ca_event_indicator, dividend_payment_year, dividend_pay_date,
                                quantity_settled_no_tax, quantity_settled_no_tax_ls, quantity_unsettled, quantity_unsettled_ls,
                                borrowed_lended_dividend_amount, borrowed_lended_dividend_amount_dc, borrowed_lended_dividend_amount_cur,
                                dividend_amount_indicator, ca_event_reference, dividend_value, dividend_value_dc, dividend_value_cur,
                                dividend_cash_value, dividend_cash_value_dc, dividend_cash_value_cur, tax_amount
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            )
                        """

                        try:
                            mycursor.execute(sql, values)
                            conn.commit()
                        except Exception as e:
                            print(f"❌ Database-insert fout voor {file}: {e}")
                            continue

            print(f"✅ Dag {work_day} volledig verwerkt.\n")


# ─────────────────────────────────────────────
#  Main Entry
# ─────────────────────────────────────────────
if __name__ == "__main__":
    import_ca_xml_into_clearing()
    mycursor.close()
    conn.close()

