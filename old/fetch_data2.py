import requests
import json
import psycopg2
from psycopg2 import sql
import os
from uuid import UUID

DB_PARAMS = {
    'dbname': 'spacex',
    'user': 'postgres',
    'password': 'xxx',
    'host': '127.0.0.1',
    'port': '5432'
}

SPACEX_API_URL = "https://api.spacexdata.com/v4/"

POSTGRES_RESERVED_KEYWORDS = {"window"}

def fetch_data(api_url):
    """Fetch data from the SpaceX API."""
    print(f"Fetching data from {api_url}...")
    response = requests.get(api_url)
    response.raise_for_status()
    print("Data fetched successfully.")
    return response.json()

def save_data_to_file(data, save_path, table_name):
    """Save fetched JSON data to a file in the specified path."""
    if not os.path.exists(save_path):
        os.makedirs(save_path)
    file_path = os.path.join(save_path, f"{table_name}.json")
    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4)
    print(f"Data saved to {file_path}")

def flatten_json(data, prefix=''):
    """Flatten nested JSON data into a single dictionary."""
    flattened = {}
    for key, value in data.items():
        new_key = f"{prefix}{key}".lower()
        if isinstance(value, dict):
            flattened.update(flatten_json(value, prefix=new_key + '_'))
        elif isinstance(value, list):
            flattened[new_key] = json.dumps(value)  # Store list as JSON string
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

def process_boolean_values(data):
    """Convert boolean values in the data to integers (0 or 1)."""
    processed_data = {}
    for key, value in data.items():
        if isinstance(value, str):
            lower_value = value.lower()
            if lower_value == 'true':
                processed_data[key] = 1  # Integer representation of True
            elif lower_value == 'false':
                processed_data[key] = 0  # Integer representation of False
            else:
                processed_data[key] = value
        else:
            processed_data[key] = 1 if value else 0 if isinstance(value, bool) else value
    return processed_data

def escape_column_name(column_name):
    """Escape SQL column names to prevent conflicts with reserved keywords."""
    if column_name in POSTGRES_RESERVED_KEYWORDS:
        return sql.Identifier(column_name)
    return sql.Identifier(column_name)

def add_missing_columns(conn, table_name, record):
    """Dynamically add missing columns to the table based on the flattened record."""
    with conn.cursor() as cursor:
        cursor.execute(sql.SQL(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s;"
        ), [table_name])
        existing_columns = {row[0]: row[1] for row in cursor.fetchall()}

        for key, value in record.items():
            column_name = key.lower()
            if column_name not in existing_columns:
                column_type = infer_column_type(value)
                alter_query = sql.SQL("ALTER TABLE {} ADD COLUMN {} {};").format(
                    sql.Identifier(table_name),
                    sql.Identifier(column_name),
                    sql.SQL(column_type)
                )
                try:
                    cursor.execute(alter_query)
                    conn.commit()
                    print(f"Added missing column: {column_name} ({column_type})")
                except Exception as e:
                    print(f"Error adding column {column_name}: {e}")
                    conn.rollback()

                    
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


def insert_or_update_data(conn, table_name, data):
    """Insert or update records dynamically into the table."""
    with conn.cursor() as cursor:
        for record in data:
            record_id = record.get("id")
            if not record_id:
                print("Skipping record without 'id'.")
                continue

            # Flatten the record
            flattened_record = flatten_json(record)
            flattened_record = process_boolean_values(flattened_record)

            # Add any missing columns dynamically
            add_missing_columns(conn, table_name, flattened_record)

            # Prepare column names and values
            columns = []
            values = []
            update_pairs = []

            for key, value in flattened_record.items():
                if key != "id":  # Skip id in the SET clause
                    columns.append(sql.Identifier(key.lower()))
                    values.append(value)
                    update_pairs.append(
                        sql.SQL("{} = EXCLUDED.{}").format(
                            sql.Identifier(key.lower()),
                            sql.Identifier(key.lower())
                        )
                    )

            # Construct the query
            query = sql.SQL("""
                INSERT INTO {table} ({id_col}, {columns})
                VALUES (%s, {placeholders})
                ON CONFLICT (id) DO UPDATE
                SET {updates}
            """).format(
                table=sql.Identifier(table_name),
                id_col=sql.Identifier('id'),
                columns=sql.SQL(', ').join(columns),
                placeholders=sql.SQL(', ').join([sql.SQL('%s')] * len(values)),
                updates=sql.SQL(', ').join(update_pairs)
            )

            try:
                cursor.execute(query, [str(record_id)] + values)
                print(f"Inserted/updated record with id: {record_id}")
            except Exception as e:
                print(f"Error inserting/updating record with id {record_id}: {e}")
                conn.rollback()

        conn.commit()

def fetch_and_process_data(source_url, table_name, save_path, conn):
    try:
        full_url = SPACEX_API_URL + source_url
        data = fetch_data(full_url)
        save_data_to_file(data, save_path, table_name)
        insert_or_update_data(conn, table_name, data)
        print(f"Data successfully inserted into {table_name}")
    except Exception as e:
        print(f"Error during data processing for {table_name}: {e}")

def main():
    datasets = [
        {
            'table_name': 'launches',
            'source_url': 'launches',
            'save_path': 'data'
        },
        {
            'table_name': 'payloads',
            'source_url': 'payloads',
            'save_path': 'data'
        }
    ]

    try:
        conn = psycopg2.connect(**DB_PARAMS)
        print("Database connected successfully.")
    except Exception as e:
        print(f"Failed to connect to the database: {e}")
        return

    for dataset in datasets:
        table_name = dataset['table_name']
        source_url = dataset['source_url']
        save_path = dataset['save_path']

        create_table_if_not_exists(conn, table_name)
        fetch_and_process_data(source_url, table_name, save_path, conn)

    conn.close()

if __name__ == '__main__':
    main()
