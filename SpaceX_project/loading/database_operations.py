# loading/database_operations.py
import psycopg2
from psycopg2 import sql
from transformation.process_data import infer_column_type, flatten_json, process_boolean_values, create_nested_table
import Json

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

def insert_or_update_data(conn, table_name, data):
    with conn.cursor() as cursor:
        for record in data:
            record_id = record.get("id")
            if not record_id:
                print("Skipping record without 'id'.")
                continue

            flattened_record = flatten_json(record)
            flattened_record = process_boolean_values(flattened_record)
            add_missing_columns(conn, table_name, flattened_record)

            # Process nested columns
            process_nested_columns(conn, table_name, record)

            columns = []
            values = []
            update_pairs = []

            for key, value in flattened_record.items():
                if key != "id":
                    columns.append(sql.Identifier(key.lower()))
                    values.append(value)
                    update_pairs.append(
                        sql.SQL("{} = EXCLUDED.{}").format(
                            sql.Identifier(key.lower()),
                            sql.Identifier(key.lower())
                        )
                    )

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

            processed_values = [
                Json(value) if isinstance(value, (dict, list)) else value
                for value in values
            ]

            try:
                cursor.execute(query, [str(record_id)] + processed_values)
                print(f"Inserted/updated record with id: {record_id}")
            except Exception as e:
                print(f"Error inserting/updating record with id {record_id}: {e}")
                conn.rollback()

        conn.commit()

    def process_nested_columns(conn, table_name, record, parent_key='id'):
        """
        Process nested JSON fields and create corresponding tables.
        """
        nested_data = {}
        for key, value in record.items():
            if isinstance(value, (dict, list)):  # Identify nested JSON columns
                nested_data[key] = value

        # For each nested column, create and populate a child table
        for nested_column, data in nested_data.items():
            parent_id = record.get(parent_key)
            if parent_id:
                create_nested_table(conn, table_name, nested_column, [(parent_id, data)])