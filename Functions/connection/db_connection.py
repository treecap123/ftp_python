import os
import pymysql

railway_conf = {
    "host": os.getenv("MYSQLHOST"),
    "user": os.getenv("MYSQLUSER"),
    "password": os.getenv("MYSQL_ROOT_PASSWORD"),
    "database": os.getenv("MYSQL_DATABASE"),
    "port": int(os.getenv("MYSQLPORT", 3306)),
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
