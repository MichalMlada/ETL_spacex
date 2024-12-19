# transformation/process_data.py
import json  # To work with JSON data
import uuid  # To work with UUIDs
from psycopg2 import sql
from psycopg2.extras import Json

POSTGRES_RESERVED_KEYWORDS = {"window"}

def escape_column_name(column_name):
    """Escape SQL column names to prevent conflicts with reserved keywords."""
    if column_name in POSTGRES_RESERVED_KEYWORDS:
        return sql.Identifier(column_name)
    return sql.Identifier(column_name)

def process_boolean_values(data):
    """Process boolean values to ensure they're correctly handled."""
    for key, value in data.items():
        if isinstance(value, bool):
            data[key] = 1 if value else 0  # Convert boolean to integer (1 or 0)
        elif isinstance(value, dict):
            process_boolean_values(value)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    process_boolean_values(item)
    return data


def flatten_json(data, prefix=''):
    """Flatten nested JSON data into a single dictionary, converting dicts/lists to JSONB."""
    flattened = {}
    for key, value in data.items():
        new_key = f"{prefix}{key}".lower()
        if isinstance(value, dict):
            flattened[new_key] = Json(value)  # Convert dict to JSONB
        elif isinstance(value, list):
            flattened[new_key] = Json(value)  # Convert list to JSONB
        elif isinstance(value, bool):
            flattened[new_key] = value  # Keep booleans as booleans
        else:
            flattened[new_key] = value
    return flattened

def infer_column_type(value):
    """Infer the SQL column type based on the value."""
    if isinstance(value, int):
        return "INTEGER"
    elif isinstance(value, float):
        return "REAL"
    elif isinstance(value, str):
        if value.lower() in ('true', 'false'):
            return "BOOLEAN"
        return "TEXT"
    elif isinstance(value, bool):
        return "BOOLEAN"
    elif isinstance(value, dict) or isinstance(value, list):
        return "JSONB"
    elif value is None:
        return "TEXT"
    else:
        return "TEXT"
    

def remove_data_column_if_exists(conn, table_name):
    """Remove the 'data' column from the table if it exists."""
    with conn.cursor() as cursor:
        cursor.execute(sql.SQL(
            "SELECT column_name FROM information_schema.columns WHERE table_name = %s;"
        ), [table_name])
        existing_columns = {row[0] for row in cursor.fetchall()}

        if 'data' in existing_columns:
            try:
                cursor.execute(sql.SQL(
                    "ALTER TABLE {} DROP COLUMN {};"
                ).format(
                    sql.Identifier(table_name),
                    sql.Identifier('data')
                ))
                conn.commit()
                print("Successfully dropped 'data' column.")
            except Exception as e:
                print(f"Error dropping 'data' column: {e}")
                conn.rollback()