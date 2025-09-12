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
    query = '''SELECT * FROM marketing_campaigns'''
    df = pd.read_sql(query, engine)
    engine.dispose()
    return df

def transform(df):
    # Ensure campaign_id is string for BigQuery compatibility
    if 'campaign_id' in df.columns:
        df['campaign_id'] = df['campaign_id'].astype(str)
    # Ensure date columns are datetime
    for col in ['start_date', 'end_date']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str)
    for col in df.select_dtypes(include=['datetime64[ns]', 'datetime64[ns, UTC]']).columns:
        df[col] = pd.to_datetime(df[col])
    df = df.reset_index(drop=True)
    return df

def load(df):
    client = get_bq_client()
    load_to_bq(client, df, PIPELINE_CONFIG['bigquery']['dataset'], 'dim_campaign')

def run():
    logger = get_logger("dim_campaign")
    df = extract()
    df_t = transform(df)
    run_dq_checks(df_t)
    load(df_t)
    logger.info("dim_campaign pipeline completed.")

if __name__ == "__main__":
    run()
