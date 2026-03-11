import pandas as pd
import numpy as np
from datetime import datetime as date
import os
import sys
import json
import dotenv

import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
from Functions.system.path.path import dropbox_path

# Railway working directory
root_root_dir = os.getcwd()

# voeg FTP toe aan python path
sys.path.append(os.path.join(root_root_dir, "FTP"))

from Functions.date.date_functions import get_previous_workday, select_date, working_days
from Functions.connection.db_connection import get_cursor
from Functions.database.lookup import account
from Functions.log_tools.logging import Color

# ─────────────────────────────────────────────
# Setup: twee connecties
# ─────────────────────────────────────────────

try:
    mycursor, conn = get_cursor()
    print(f"{Color.OKBLUE}✅ Verbonden met Railway database{Color.ENDC}")
except Exception as e:
    print(f"{Color.WARNING}⚠ Railway-verbinding mislukt: {e}{Color.ENDC}")
    exit()

fx_list = {
    'EUR': 0, 'GBP': 0, 'USD': 0, 'AUD': 0, 'CAD': 0,
    'CHF': 0, 'DKK': 0, 'HKD': 0, 'JPY': 0, 'MXN': 0,
    'NOK': 0, 'SEK': 0, 'SGD': 0, 'PLN': 0,
}


def get_fx(date):
    fx = f"""
        SELECT currency, MAX(conversionRate) AS conversionRate
        FROM account_level
        WHERE date = '{date}'
        GROUP BY currency
    """
    print(f"Executing SQL to get FX rates for date {date}: {fx}")
    mycursor.execute(fx)
    result = mycursor.fetchall()

    for x in result:
        fx_list[x['currency']] = x['conversionRate']
        print(fx_list)


def row_to_sql_values(row_tuple):
    values = []
    for x in row_tuple:
        if x is None or x == '':
            values.append('')
        elif isinstance(x, (int, float)):
            values.append(str(x))  # getallen niet quoten
        else:
            x_escaped = str(x).replace("'", "''")
            values.append(f"'{x_escaped}'")
    return f"({', '.join(values)})"


# ─────────────────────────────────────────────
# Verwerking per werkdag
# ─────────────────────────────────────────────
for proc_date in working_days():
    if proc_date not in select_date('account_level'):
        dropbox_folder = os.path.join(dropbox_path, str(proc_date))
        print(dropbox_folder)

        if not os.path.exists(dropbox_folder):
            print(f"{Color.WARNING}⚠ Map niet gevonden: {dropbox_folder}{Color.ENDC}")
            continue

        for file in os.listdir(dropbox_folder):
            if "Daily Cash Summary Account Level" not in file:
                continue

            df = pd.read_csv(os.path.join(dropbox_folder, file))
            df = df.replace({np.nan: ''})
            df.sort_values(by=['Cash Title', 'Currency'], inplace=True)

            get_fx(get_previous_workday(proc_date))  # hier

            for idx, row in df.iterrows():

                opening_eur = row['Opening Balance'] * row['Conversion Rate']
                previous_eur = row['Previous Balance'] * fx_list[row['Currency']]

                account_treecap, portfolio_manager = account(
                    f"{row['Client']}_{row['Account Type']}_{row['Account']}",
                    mycursor
                )

                if isinstance(portfolio_manager, (list, dict)):
                    portfolio_manager = json.dumps(portfolio_manager)

                sql = f"""
                    INSERT INTO account_level (
                        date, account_treecap, portfolio_manager, client, clientName,
                        accountType, account, accountName, currency, cashTitle,
                        openingBalance, changeBalance, previousBalance,
                        clientCurrency, conversionRate, monthToDate, grossIndicator,
                        journalCode, productGroup, openingBalanceClientCur,
                        changeBalanceClientCur, previousBalanceClientCur,
                        marginDate, previous_eur
                    )
                    VALUES (
                        '{row['Processing Date']}',
                        '{account_treecap}',
                        '{portfolio_manager}',
                        '{row['Client']}',
                        '{row['Client Name']}',
                        '{row['Account Type']}',
                        '{row['Account']}',
                        '{row['Account Name']}',
                        '{row['Currency']}',
                        '{row['Cash Title']}',
                        '{row['Opening Balance']}',
                        '{row['Change Balance']}',
                        '{row['Previous Balance']}',
                        '{row['Client Currency']}',
                        '{row['Conversion Rate']}',
                        '{row['Month to Date']}',
                        '{row['Gross Indicator']}',
                        '{row['Journal Code']}',
                        '{row['Product Group']}',
                        '{row['Opening Balance in Client Cur']}',
                        '{row['Change Balance in Client Cur']}',
                        '{row['Previous Balance in Client Cur']}',
                        '{row['Margin Date']}',
                        '{previous_eur}'
                    )
                    ON DUPLICATE KEY UPDATE
                        openingBalance = VALUES(openingBalance),
                        previousBalance = VALUES(previousBalance)
                """

                # 🔸 Insert lokaal
                try:
                    mycursor.execute(sql)
                    conn.commit()
                except Exception as e:
                    print(f"{Color.WARNING}⚠ Lokale insert mislukt: {e}{Color.ENDC}")
                print(f"{Color.OKGREEN}✅ Data inserted successfully{Color.ENDC}")

# ─────────────────────────────────────────────
# Sluiten
# ─────────────────────────────────────────────
mycursor.close()
conn.close()

print(f"{Color.OKGREEN}✔️ Alle data lokaal en op Railway ingevoerd.{Color.ENDC}")
