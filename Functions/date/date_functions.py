import sys
import os
from typing import Optional, Union
from datetime import datetime, timedelta, date

# Zorg dat je backend-map in PYTHONPATH zit
sys.path.append(r'C:\Users\Operations\Documents\.AAtreecap\test\backend')

from global_functions.database_connection.connection import get_cursor

blacklist = []


def select_date(table):
    mycursor, conn = get_cursor()

    try:
        sql = f"SELECT DISTINCT date FROM {table}"
        mycursor.execute(sql)
        rows = mycursor.fetchall()
        # zorg dat we de kolom 'date' pakken uit elke row dict
        return [row["date"] for row in rows if row.get("date")]
    except Exception as e:
        print(f"⚠️ Error fetching dates from {table}: {e}")
        return []
    finally:
        mycursor.close()
        conn.close()



def working_days():
    mycursor, conn = get_cursor()

    try:
        workdays = f"SELECT date FROM working_day WHERE is_workday = 0 AND date < now() AND date LIKE '%{datetime.now().year}%' ORDER BY date"
        mycursor.execute(workdays)
        result = mycursor.fetchall()
        if result and result[-1]['date'] == datetime.today().date():
            result.pop()  # Verwijder vandaag als het een werkdag is
        result = [r['date'] for r in result]
        return result
    except Exception as e:
        print(f"Error fetching working days: {e}")
        return []
    finally:
        mycursor.close()
        conn.close()







# -----------------------------
# Hoofdfunctie: vorige werkdag berekenen
# -----------------------------
def get_previous_workday(start_date=None, days=1):
    """
    Geef de vorige werkdag terug (geen weekend, geen blacklist).
    - start_date: optioneel, datetime of string 'YYYY-MM-DD'. Anders pak 'vandaag'.
    - days: hoeveel dagen terug (standaard 1).
    """

    # 1) Startdatum bepalen
    if start_date is None:
        start_date = datetime.today().date()
    else:
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        elif isinstance(start_date, datetime):
            start_date = start_date.date()

    # 2) Eerste poging
    current = start_date - timedelta(days=days)

    # 3) Extra handmatige vrije dagen
    excluded_dates = [
        datetime(current.year, 1, 1).date(),   # Nieuwjaarsdag
        datetime(current.year, 12, 25).date()  # Kerstmis
    ]

    # 4) Check blacklist (weekend, handmatig, DB)
    while (
        current.weekday() >= 5 or  # weekend
        current in excluded_dates or
        current in blacklist

    ):
        current -= timedelta(days=1)

    # 5) Geef terug in stringvorm
    return current.strftime('%Y-%m-%d')

def parse_vue_date_string(date_str: str) -> date:
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except Exception:
        raise ValueError(f"Invalid date format (expected YYYY-MM-DD): {date_str}")


def make_date_where_clause(
    d: Optional[date], sd: Optional[date], ed: Optional[date]
) -> Optional[str]:
    if d:
        return f"date = '{d.isoformat()}'"
    if sd and ed:
        return f"date BETWEEN '{sd.isoformat()}' AND '{ed.isoformat()}'"
    if sd:
        return f"date >= '{sd.isoformat()}'"
    if ed:
        return f"date <= '{ed.isoformat()}'"
    return None
