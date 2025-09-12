import os
import sys
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import Conflict
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.bigquery_config import PROJECT_ID, KEY_PATH, SQL_DIR, DATASET_ID


def run_ddl_files():
    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    # Create dataset if not exists
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {DATASET_ID} already exists.")
    except Exception:
        client.create_dataset(dataset_ref)
        print(f"Created dataset {DATASET_ID}.")
    sql_files = sorted([f for f in os.listdir(SQL_DIR) if f.endswith('.sql')])
    for sql_file in sql_files:
        sql_path = os.path.join(SQL_DIR, sql_file)
        with open(sql_path, 'r') as f:
            ddl = f.read()
        print(f"Running {sql_file}...")
        try:
            client.query(ddl).result()
            print(f"{sql_file} executed successfully.")
        except Conflict as e:
            print(f"Table already exists, skipping {sql_file}.")
            continue
        except Exception as e:
            print(f"Error executing {sql_file}: {e}")
            sys.exit(1)
    print("All BigQuery tables created successfully.")


if __name__ == "__main__":
    run_ddl_files()
