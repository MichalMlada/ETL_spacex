# loading/database_operations.py
import psycopg2
from psycopg2 import sql
from transformation.process_data import infer_column_type, flatten_json, process_boolean_values


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
    """Insert or update records dynamically into the table."""
    with conn.cursor() as cursor:
        for record in data:
            record_id = record.get("id")
            if not record_id:
                print("Skipping record without 'id'.")
                continue

            # Flatten the record and process boolean values
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
                    values.append(value)  # Use value directly (which is now JSONB if needed)
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

            # Ensure JSON data is passed as JSONB (use Json to handle JSONB columns)
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