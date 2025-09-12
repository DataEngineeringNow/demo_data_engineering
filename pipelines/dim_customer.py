import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from common.bigquery_client import get_bq_client, load_to_bq
from common.logging_utils import get_logger
from common.dq_checks import run_dq_checks
from common.config import PIPELINE_CONFIG
import pandas as pd
import psycopg2

def extract():
    conn = psycopg2.connect(**PIPELINE_CONFIG['oltp_db'])
    query = "SELECT customer_id, customer_name AS name, email, phone, location, acquisition_channel, signup_date AS created_at FROM customers"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def transform(df):
    df['customer_id'] = df['customer_id'].astype(str)
    if 'created_at' in df:
        df['created_at'] = pd.to_datetime(df['created_at'])
    df['loyalty_segment'] = (
        pd.cut(df['loyalty_points'], bins=[0, 100, 200, 1000], labels=['Bronze', 'Silver', 'Gold'])
        if 'loyalty_points' in df else 'Bronze'
    )
    df['updated_at'] = pd.Timestamp.now()
    return df


def load(df):
    client = get_bq_client()
    load_to_bq(client, df, PIPELINE_CONFIG['bigquery']['dataset'], 'dim_customer')

def run():
    logger = get_logger("dim_customer")
    df = extract()
    df_t = transform(df)
    run_dq_checks(df_t, required_columns=['customer_id', 'email'])
    load(df_t)
    logger.info("dim_customer pipeline completed.")

if __name__ == "__main__":
    run()
