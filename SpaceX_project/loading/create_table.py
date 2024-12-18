# loading/create_table.py
from psycopg2 import sql

def create_table_if_not_exists(conn, table_name):
    """Create the initial table if it doesn't exist, using TEXT for id."""
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {} (
                    id TEXT PRIMARY KEY,
                    data TEXT
                )
            """).format(sql.Identifier(table_name)))
            conn.commit()
            print(f"Table '{table_name}' created/verified.")
    except Exception as e:
        print(f"Error creating table: {e}")
        conn.rollback()