from google.cloud import bigquery
from google.oauth2 import service_account
from common.config import PIPELINE_CONFIG
import pandas_gbq


def get_bq_client():
    credentials = service_account.Credentials.from_service_account_file(
        PIPELINE_CONFIG['bigquery']['key_path']
    )
    return bigquery.Client(credentials=credentials, project=PIPELINE_CONFIG['bigquery']['project_id'])

def load_to_bq(client, df, dataset, table):
    table_id = f"{PIPELINE_CONFIG['bigquery']['project_id']}.{dataset}.{table}"
    job = client.load_table_from_dataframe(df, table_id)
    job.result()
    print(f"Loaded {len(df)} rows to {table_id}")
