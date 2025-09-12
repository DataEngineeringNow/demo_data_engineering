import os
import sys
import psycopg2
from psycopg2 import sql, errors
from pathlib import Path

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from common.config import PIPELINE_CONFIG

SQL_DIR = os.path.join(os.path.dirname(__file__), '..', 'sql')


def create_database_if_not_exists():
    # Get database configuration
    db_config = PIPELINE_CONFIG['oltp_db'].copy()
    db_config["database"] = "defaultdb"  # Connect to defaultdb to create new db
    
    try:
        # Connect to the default database
        conn = psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            database=db_config['database'],
            user=db_config['user'],
            password=db_config['password']
        )
        conn.autocommit = True  # Required for creating databases
        
        try:
            with conn.cursor() as cur:
                # Check if database exists
                cur.execute("SELECT 1 FROM pg_database WHERE datname = 'ecommerce'")
                exists = cur.fetchone()
                if not exists:
                    print("Creating database 'ecommerce'...")
                    cur.execute("CREATE DATABASE ecommerce")
                    print("Database 'ecommerce' created successfully.")
                else:
                    print("Database 'ecommerce' already exists.")
        finally:
            conn.close()
    except Exception as e:
        print(f"Error creating database: {e}")
        sys.exit(1)


def execute_sql_scripts():
    # Get database configuration
    db_config = PIPELINE_CONFIG['oltp_db'].copy()
    db_config["database"] = "ecommerce"
    
    # Get all SQL files in the directory and sort them
    sql_files = sorted([f for f in os.listdir(SQL_DIR) if f.endswith('.sql')])
    
    try:
        # Connect to the database
        conn = psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            database=db_config['database'],
            user=db_config['user'],
            password=db_config['password']
        )
        
        with conn.cursor() as cur:
            for sql_file in sql_files:
                file_path = os.path.join(SQL_DIR, sql_file)
                print(f"Executing {sql_file}...")
                try:
                    with open(file_path, 'r') as f:
                        cur.execute(f.read())
                    print(f"Successfully executed {sql_file}")
                    conn.commit()
                except Exception as e:
                    print(f"Error executing {sql_file}: {e}")
                    conn.rollback()
                    raise
    except Exception as e:
        print(f"Database connection error: {e}")
        sys.exit(1)
    finally:
        if 'conn' in locals() and conn is not None:
            conn.close()
    print("All scripts processed.")


if __name__ == "__main__":
    create_database_if_not_exists()
    execute_sql_scripts()
