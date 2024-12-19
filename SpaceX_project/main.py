# main.py
import psycopg2
from extraction.fetch_data import fetch_data
from extraction.save_data import save_data_to_file
from loading.create_table import create_table_if_not_exists
from loading.database_operations import insert_or_update_data
import os

DB_PARAMS = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

SPACEX_API_URL = "https://api.spacexdata.com/v4/"

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
        {'table_name': 'launches', 'source_url': 'launches', 'save_path': 'data'},
        {'table_name': 'payloads', 'source_url': 'payloads', 'save_path': 'data'}
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