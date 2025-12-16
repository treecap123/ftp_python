import sys
import json
from datetime import datetime

sys.path.append(r'C:\Users\Operations\Documents\.AAtreecap\test\backend')


def account(abn_account, cursor):
    """
    Zoek een account in account_specs.
    Als het niet bestaat, voeg het toe met een lege JSON-array als portfolio_manager.
    """
    abn_account = abn_account.replace(" ", "_")

    sql = "SELECT account_treecap, portfolio_manager FROM account_specs WHERE unique_account_aacb = %s"
    cursor.execute(sql, (abn_account,))
    result = cursor.fetchone()

    if result is None:
        # insert nieuwe account met lege portfolio_manager
        insert_sql = (
            "INSERT INTO account_specs (unique_account_aacb, portfolio_manager) "
            "VALUES (%s, %s)"
        )
        cursor.execute(insert_sql, (abn_account, json.dumps([])))
        return "N/A", []  # nieuw account heeft nog geen data

    # portfolio_manager teruggeven als Python list (parsed JSON)
    account_treecap = result["account_treecap"]
    portfolio_manager = (
        json.loads(result["portfolio_manager"]) if result["portfolio_manager"] else []
    )
    return account_treecap, portfolio_manager


def instrument(abn_symbol, cursor, proc_date=None):
    """
    Zoek een instrument in master_instrument.
    Als het niet bestaat, voeg het toe met een geldige datum.
    """
    sql = """
        SELECT category, deal, symbol_treecap
        FROM master_instrument
        WHERE abn_symbol = %s
        ORDER BY date DESC
        LIMIT 1
    """
    cursor.execute(sql, (abn_symbol,))
    result = cursor.fetchone()

    if result is None:
        # if proc_date is None:
        #     proc_date = datetime.today().strftime("%Y-%m-%d")
        #
        # insert_sql = """
        #     INSERT INTO master_instrument (abn_symbol, date, category, deal, symbol_treecap)
        #     VALUES (%s, %s, NULL, NULL, NULL)
        # """
        # cursor.execute(insert_sql, (abn_symbol, proc_date))
        return "N/A", "N/A", "N/A"

    return result["category"], result["deal"], result["symbol_treecap"]


def trad_list(cursor, type=None):
    """
    Haalt alle unieke trads uit de table trad_list
    """
    if type:
        sql = "SELECT DISTINCT trad FROM trad_list WHERE JSON_CONTAINS(type, %s)"
        cursor.execute(sql, (json.dumps(type),))
    else:
        sql = "SELECT DISTINCT trad FROM trad_list"
        cursor.execute(sql)

    result = cursor.fetchall()
    return [r['trad'] for r in result]
