import sys
import os
import logging
from datetime import datetime, timedelta

# Zorg dat je backend-map in PYTHONPATH zit
sys.path.append(r'/')

from global_functions.database_connection.connection import get_cursor





def get_privilege_by_token(token: str):
    mycursor, conn = get_cursor()
    try:
        """
        Haalt de privilege op voor een gegeven token.
        Returned de privilege-string (bijv. 'admin' of 'H.BRUNET'), of None als niet gevonden.
        """
        sql = """
            SELECT priviledge
            FROM users
            WHERE token = %s
        """

        mycursor.execute(sql, (token,))
        row = mycursor.fetchone()
        # logging.debug(f"SQL Query: {sql} met token: {token}")
        return row['priviledge'] if row else None
    except Exception as e:
        logging.error(f"Fout bij het ophalen van privilege voor token {token}: {e}")
        return None
    finally:
        mycursor.close()
        conn.close()

def get_permissions_for_portfolio_manager(pm: str):
    mycursor, conn = get_cursor()
    try:
        """
        Haalt alle permissions op voor een gegeven portfolio manager.
        Retourneert een lijst strings, bijv. ['IA', 'TREECAP'].
        """
        sql = """
            SELECT permission
            FROM portfolio_permissions
            WHERE portfolio_manager = %s
        """


        mycursor.execute(sql, (pm,))
        results = mycursor.fetchall()
        # results is bijv. [(‘IA’,), (‘TREECAP’,)]
        return [row[0] for row in results]
    except Exception as e:
        logging.error(f"Fout bij het ophalen van permissions voor portfolio manager {pm}: {e}")
        return []
    finally:
        mycursor.close()
        conn.close()