import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from common.bigquery_client import get_bq_client, load_to_bq
from common.logging_utils import get_logger
from common.dq_checks import run_dq_checks
from common.config import PIPELINE_CONFIG
import pandas as pd
from sqlalchemy import create_engine

def extract():
    db_conf = PIPELINE_CONFIG['oltp_db']
    user = db_conf['user']
    password = db_conf['password']
    host = db_conf['host']
    port = db_conf['port']
    database = db_conf['database']
    engine_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(engine_str)
    query = '''SELECT * FROM sellers'''
    df = pd.read_sql(query, engine)
    engine.dispose()
    return df

def transform(df):
    # Ensure seller_id is string for BigQuery compatibility
    if 'seller_id' in df.columns:
        df['seller_id'] = df['seller_id'].astype(str)
    # Ensure join_date is datetime for BigQuery compatibility
    if 'join_date' in df.columns:
        df['join_date'] = pd.to_datetime(df['join_date'])
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str)
    for col in df.select_dtypes(include=['datetime64[ns]', 'datetime64[ns, UTC]']).columns:
        df[col] = pd.to_datetime(df[col])
    df = df.reset_index(drop=True)
    return df

def load(df):
    client = get_bq_client()
    load_to_bq(client, df, PIPELINE_CONFIG['bigquery']['dataset'], 'dim_seller')

def run():
    logger = get_logger("dim_seller")
    df = extract()
    df_t = transform(df)
    run_dq_checks(df_t)
    load(df_t)
    logger.info("dim_seller pipeline completed.")

if __name__ == "__main__":
    run()
