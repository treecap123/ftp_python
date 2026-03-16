# === import_ca_xml_into_clearing.py ===

import os
import sys
import json
import xml.etree.ElementTree as ET
import pymysql
from datetime import datetime

import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# ─────────────────────────────────────────────
# Railway working directory
# ─────────────────────────────────────────────
root_root_dir = os.getcwd()
sys.path.append(root_root_dir)

# ─────────────────────────────────────────────
# Project imports
# ─────────────────────────────────────────────
from Functions.date.date_functions import working_days
from Functions.connection.db_connection import get_cursor
from Functions.system.path.path import dropbox_path
from Functions.log_tools.logging import Color


today = datetime.today().date()

def working_days_including_today():
    days = working_days()
    if today not in days:
        days.append(today)
    return sorted(days)


# ─────────────────────────────────────────────
# Database verbinding
# ─────────────────────────────────────────────
local_cursor, local_conn = get_cursor()
railway_cursor, railway_conn = get_cursor()


# ─────────────────────────────────────────────
# Unique notification types
# ─────────────────────────────────────────────
UNIQUE_NOTIFICATION_TYPES = [
    "Payment Advice",
    "Pre-announcement notifications",
    "Change In Client Position",
    "Event Announcement notifications"
]

def filename_matches_notification_type(filename: str):
    for notif in UNIQUE_NOTIFICATION_TYPES:
        if notif in filename:
            return True
    return False


# ─────────────────────────────────────────────
# bestaande file_dates ophalen
# ─────────────────────────────────────────────
def select_existing_file_dates(cursor):
    sql = "SELECT DISTINCT file_date FROM ca_notifications"
    cursor.execute(sql)
    result = cursor.fetchall()
    return [r["file_date"] for r in result]


# ─────────────────────────────────────────────
# date uit filename halen
# ─────────────────────────────────────────────
def extract_date_from_filename(filename: str):
    return filename[:10]


# ─────────────────────────────────────────────
# insert functie
# ─────────────────────────────────────────────
def insert_notification(cursor, conn, file_date, filename, extension, file_size, xml_content):

    sql = """
        INSERT INTO ca_notifications
        (file_date, filename, extension, file_size, xml_content)
        VALUES (%s, %s, %s, %s, %s)
    """

    try:
        cursor.execute(sql, (file_date, filename, extension, file_size, xml_content))
        conn.commit()
        print(f"✅ Inserted {filename}")

    except pymysql.err.IntegrityError:
        print(f"⚠️ Bestond al → {filename}")

    except Exception as e:
        print(f"❌ Insert error: {e}")


# ─────────────────────────────────────────────
# MAIN PROCESS
# ─────────────────────────────────────────────
existing_dates = select_existing_file_dates(local_cursor)


for work_day in working_days_including_today():

    if work_day not in existing_dates:

        print(f"📅 Verwerking gestart voor {work_day}")

        dropbox_folder = os.path.join(dropbox_path, work_day.strftime("%Y-%m-%d"))

        if not os.path.exists(dropbox_folder):
            print(f"⚠️ Map ontbreekt → {dropbox_folder}")
            continue

        for file in os.listdir(dropbox_folder):

            # skip hidden / done files
            if file.startswith("."):
                continue

            # alleen xml
            if not file.lower().endswith(".xml"):
                continue

            if not filename_matches_notification_type(file):
                print(f"⏩ Skip (geen notification type): {file}")
                continue

            file_path = os.path.join(dropbox_folder, file)

            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    xml_text = f.read()

            except Exception as e:
                print(f"❌ XML read error: {file_path} → {e}")
                continue

            filename = file
            extension = file.split(".")[-1]
            file_size = os.path.getsize(file_path)

            file_date = extract_date_from_filename(filename)

            if not file_date:
                print(f"⚠️ Geen geldige file_date: {filename}")
                continue

            insert_notification(local_cursor, local_conn, file_date, filename, extension, file_size, xml_text)
            insert_notification(railway_cursor, railway_conn, file_date, filename, extension, file_size, xml_text)

        print(f"✔️ Done met {work_day}")