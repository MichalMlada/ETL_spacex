import psycopg2
import json
import os
import pandas as pd
from psycopg2 import sql

# Configurations
DATABASE_CONFIG = {
    'user': 'postgres',
    'password': 'draks317',
    'host': '127.0.0.1',
    'port': 5432,
    'database': 'spacex'
}
DATA_PATH = 'data'

# Create PostgreSQL connection
def connect_to_db():
    return psycopg2.connect(
        DATABASE_CONFIG)

datasets = [
    {
        'table_name': 'launches',
        'save_path': 'data/launches.json'
    },
    {
        'table_name': 'payloads',
        'save_path': 'data/payloads.json'
    }
]
def create_table_from_df(table_name, df, conn):
    columns = df.columns
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (" + ", ".join(
        [f"{col} {get_postgres_type(df[col])}" for col in columns]) + ");"
    
    with conn.cursor() as cursor:
        cursor.execute(create_table_query)
        conn.commit()

def get_postgres_type(series):
    if pd.api.types.is_integer_dtype(series):
        return 'INTEGER'
    elif pd.api.types.is_float_dtype(series):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(series):
        return 'BOOLEAN'
    else:
        return 'TEXT'

def insert_data_to_db(table_name, df, conn):
    with conn.cursor() as cursor:
        for _, row in df.iterrows():
            insert_query = sql.SQL(
                f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['%s'] * len(row))})"
            )
            cursor.execute(insert_query, tuple(row))
        conn.commit()

def parse_nested_data(df):
    nested_columns = [col for col in df.columns if isinstance(df[col].iloc[0], dict)]
    
    for col in nested_columns:
        nested_df = pd.json_normalize(df[col])
        nested_table_name = f"{col}_nested"
        
        # Create nested table and insert data
        create_table_from_df(nested_table_name, nested_df, conn)
        insert_data_to_db(nested_table_name, nested_df, conn)
        
        # Remove the original nested column
        df.drop(columns=[col], inplace=True)
    
    return df

def upload_data_to_db():
    conn = connect_to_db()

    for dataset in datasets:
        with open(dataset['save_path'], 'r') as f:
            data = json.load(f)
        
        # Convert to DataFrame
        df = pd.json_normalize(data)

        # Parse and handle nested columns
        df = parse_nested_data(df)

        # Create table and insert data
        create_table_from_df(dataset['table_name'], df, conn)
        insert_data_to_db(dataset['table_name'], df, conn)

        print(f"Data for {dataset['table_name']} uploaded to PostgreSQL.")

    conn.close()

# Run the function to upload data
upload_data_to_db()