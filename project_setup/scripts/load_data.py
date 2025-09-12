import os
import sys
import csv
import psycopg2
from psycopg2 import sql
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.db_config import DB_CONFIG

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')

# Define table load order based on foreign key dependencies
TABLE_LOAD_ORDER = [
    'customers',
    'sellers',
    'products',
    'marketing_campaigns',
    'orders',
    'order_items',
    'inventory',
    'campaign_performance',
    'cart_events',
]


def load_csv_to_table(cursor, table_name, csv_path):
    with open(csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        rows = list(reader)
        if not rows:
            print(f"No data in {csv_path}, skipping.")
            return
        columns = rows[0].keys()
        placeholders = ','.join(['%s'] * len(columns))
        insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(table_name),
            sql.SQL(',').join(map(sql.Identifier, columns)),
            sql.SQL(placeholders)
        )
        values = [tuple(row[col] for col in columns) for row in rows]
        cursor.executemany(insert_query.as_string(cursor), values)
        print(f"Loaded {len(rows)} rows into {table_name}.")


def main():
    db_config = DB_CONFIG.copy()
    db_config["database"] = "ecommerce"
    data_files = {f.replace('.csv', ''): os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.endswith('.csv')}
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            for table_name in TABLE_LOAD_ORDER:
                if table_name not in data_files:
                    print(f"No CSV file for table {table_name}, skipping.")
                    continue
                csv_path = data_files[table_name]
                print(f"Loading {csv_path} into {table_name}...")
                try:
                    load_csv_to_table(cur, table_name, csv_path)
                except Exception as e:
                    print(f"Error loading {csv_path} into {table_name}: {e}")
                    conn.rollback()
                    sys.exit(1)
            conn.commit()
    print("All data loaded successfully.")


if __name__ == "__main__":
    main()
