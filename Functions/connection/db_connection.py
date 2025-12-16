import os
import pymysql

railway_conf = {
    "host": os.getenv("LOCAL_HOST"),
    "user": os.getenv("LOCAL_USER"),
    "password": os.getenv("LOCAL_PASSWORD"),
    "database": os.getenv("LOCAL_DATABASE"),
    "port": int(os.getenv("LOCAL_PORT", 3306)),
}

def get_cursor():
    if not railway_conf["host"]:
        raise ValueError("Geen Railway DB-config gevonden (check env vars).")

    conn = pymysql.connect(
        **railway_conf,
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
        charset="utf8mb4"
    )
    return conn.cursor(), conn
