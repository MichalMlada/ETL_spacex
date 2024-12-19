# transformation/table_splitter.py
import json
from psycopg2 import sql
from psycopg2.extras import Json
from typing import List, Dict, Any, Tuple
from transformation.process_data import infer_column_type


def get_id_column_type(conn, table_name: str) -> str:
    """Get the data type of the 'id' column from the parent table."""
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT data_type 
            FROM information_schema.columns 
            WHERE table_name = %s AND column_name = 'id';
        """, [table_name])
        result = cursor.fetchone()
        return result[0] if result else 'text'  # default to text if not found

def is_valid_json(text: str) -> bool:
    """Check if a string is valid JSON."""
    try:
        if text is None:
            return False
        json.loads(text)
        return True
    except (json.JSONDecodeError, TypeError):
        return False

def identify_json_columns(conn, table_name: str) -> List[Tuple[str, Dict]]:
    """
    Identify columns containing JSON data in a given table, including TEXT columns.
    Returns list of (column_name, parsed_json_data) tuples.
    """
    with conn.cursor() as cursor:
        # Get all text and jsonb columns
        cursor.execute(sql.SQL("""
            SELECT column_name, data_type
            FROM information_schema.columns 
            WHERE table_name = %s 
            AND (data_type = 'text' OR data_type = 'jsonb');
        """), [table_name])
        potential_json_columns = cursor.fetchall()
        
        json_column_data = []
        for column, data_type in potential_json_columns:
            # Get sample data for each column
            cursor.execute(sql.SQL("""
                SELECT {} 
                FROM {} 
                WHERE {} IS NOT NULL 
                LIMIT 1;
            """).format(
                sql.Identifier(column),
                sql.Identifier(table_name),
                sql.Identifier(column)
            ))
            result = cursor.fetchone()
            
            if result and result[0]:
                sample_data = result[0]
                # For text columns, check if it's valid JSON
                if data_type == 'text' and is_valid_json(sample_data):
                    try:
                        parsed_json = json.loads(sample_data)
                        if isinstance(parsed_json, dict):  # Only process dictionary-like JSON
                            json_column_data.append((column, parsed_json))
                    except json.JSONDecodeError:
                        continue
                # For jsonb columns, use directly
                elif data_type == 'jsonb' and isinstance(sample_data, dict):
                    json_column_data.append((column, sample_data))
                    
        return json_column_data

def create_related_table(conn, parent_table: str, column_name: str, sample_data: Dict[str, Any]):
    """
    Create a new table for the nested JSON data with a foreign key to the parent table.
    """
    new_table_name = f"{parent_table}_{column_name}"
    
    # Get parent table's ID column type
    id_type = get_id_column_type(conn, parent_table)
    
    # Extract column definitions from sample data
    columns = []
    for key, value in sample_data.items():
        col_type = infer_column_type(value)
        columns.append(f"{key} {col_type}")
    
    with conn.cursor() as cursor:
        # Create new table
        create_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                id TEXT PRIMARY KEY DEFAULT md5(random()::text),
                parent_id {} REFERENCES {} (id),
                {}
            );
        """).format(
            sql.Identifier(new_table_name),
            sql.SQL(id_type),
            sql.Identifier(parent_table),
            sql.SQL(',\n').join(map(sql.SQL, columns))
        )
        
        cursor.execute(create_table_query)
        conn.commit()
        
    return new_table_name

def migrate_json_data(conn, parent_table: str, column_name: str, new_table_name: str):
    """
    Migrate data from JSON column to the new related table.
    """
    with conn.cursor() as cursor:
        # Get all records with JSON data
        cursor.execute(sql.SQL("""
            SELECT id, {}
            FROM {}
            WHERE {} IS NOT NULL;
        """).format(
            sql.Identifier(column_name),
            sql.Identifier(parent_table),
            sql.Identifier(column_name)
        ))
        
        records = cursor.fetchall()
        
        for record in records:
            parent_id, json_data = record
            
            # Parse JSON if it's a string
            if isinstance(json_data, str):
                try:
                    json_data = json.loads(json_data)
                except json.JSONDecodeError:
                    print(f"Failed to parse JSON for record {parent_id}")
                    continue
            
            # Skip if not a dictionary
            if not isinstance(json_data, dict):
                continue
                
            # Insert data into new table
            columns = list(json_data.keys())
            values = list(json_data.values())
            
            insert_query = sql.SQL("""
                INSERT INTO {} (parent_id, {})
                VALUES (%s, {});
            """).format(
                sql.Identifier(new_table_name),
                sql.SQL(', ').join(map(sql.Identifier, columns)),
                sql.SQL(', ').join(sql.Placeholder() * len(values))
            )
            
            try:
                cursor.execute(insert_query, [parent_id] + values)
            except Exception as e:
                print(f"Error inserting data for record {parent_id}: {e}")
                conn.rollback()
                continue
        
        conn.commit()

def split_json_columns(conn, table_name: str):
    """
    Main function to identify and split JSON columns into related tables.
    """
    # Get JSON columns and their sample data
    json_columns = identify_json_columns(conn, table_name)
    
    for column_name, sample_data in json_columns:
        print(f"Processing JSON column: {column_name}")
        
        # Create new table for the JSON data
        new_table_name = create_related_table(conn, table_name, column_name, sample_data)
        print(f"Created new table: {new_table_name}")
        
        # Migrate data to the new table
        migrate_json_data(conn, table_name, column_name, new_table_name)
        print(f"Migrated data to {new_table_name}")
        
        # Optionally: Remove the original JSON column
        with conn.cursor() as cursor:
            cursor.execute(sql.SQL("""
                ALTER TABLE {} DROP COLUMN IF EXISTS {};
            """).format(
                sql.Identifier(table_name),
                sql.Identifier(column_name)
            ))
            conn.commit()
            print(f"Removed original JSON column: {column_name}")

def process_nested_json(conn):
    """
    Process all tables to split out nested JSON columns.
    """
    # Get all tables in the database
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public';
        """)
        tables = [row[0] for row in cursor.fetchall()]
    
    for table_name in tables:
        print(f"\nProcessing table: {table_name}")
        split_json_columns(conn, table_name)