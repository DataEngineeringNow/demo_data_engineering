import os
import sys
import psycopg2
from psycopg2 import sql, errors
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.db_config import DB_CONFIG

SQL_DIR = os.path.join(os.path.dirname(__file__), '..', 'sql')


def create_database_if_not_exists():
    temp_config = DB_CONFIG.copy()
    temp_config["database"] = "defaultdb"  # Connect to defaultdb to create new db
    try:
        conn = psycopg2.connect(**temp_config)
        conn.autocommit = True  # Set autocommit before creating cursor
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM pg_database WHERE datname = 'ecommerce'")
                exists = cur.fetchone()
                if not exists:
                    print("Creating database 'ecommerce'...")
                    cur.execute("CREATE DATABASE ecommerce")
                else:
                    print("Database 'ecommerce' already exists.")
        finally:
            conn.close()
    except Exception as e:
        print(f"Error creating database: {e}")
        sys.exit(1)


def execute_sql_scripts():
    db_config = DB_CONFIG.copy()
    db_config["database"] = "ecommerce"
    sql_files = sorted([f for f in os.listdir(SQL_DIR) if f.endswith('.sql')])
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                for sql_file in sql_files:
                    path = os.path.join(SQL_DIR, sql_file)
                    with open(path, 'r') as f:
                        sql_content = f.read()
                    print(f"Executing {sql_file}...")
                    try:
                        cur.execute(sql_content)
                        print(f"{sql_file} executed successfully.")
                    except Exception as e:
                        print(f"Error executing {sql_file}: {e}")
                        conn.rollback()
                        break
            print("All scripts processed.")
    except Exception as e:
        print(f"Database connection or execution error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    create_database_if_not_exists()
    execute_sql_scripts()
