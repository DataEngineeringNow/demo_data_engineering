import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from common.bigquery_client import get_bq_client, load_to_bq
from common.logging_utils import get_logger
from common.dq_checks import run_dq_checks
from common.config import PIPELINE_CONFIG
from common.db_utils import get_sqlalchemy_engine
import pandas as pd

def extract():
    """Extract product data from the OLTP database."""
    engine = get_sqlalchemy_engine(PIPELINE_CONFIG['oltp_db'])
    query = 'SELECT * FROM products'
    df = pd.read_sql(query, engine)
    engine.dispose()
    return df

def transform(df):
    # Ensure all string columns are string
    string_cols = ['product_id', 'name', 'category', 'sub_category', 'brand']
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype(str)
    # Ensure numeric columns are float
    for col in ['cost_price', 'unit_price']:
        if col in df.columns:
            df[col] = df[col].astype(float)
    # Ensure timestamp columns are datetime
    for col in ['created_at', 'updated_at']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])
    df = df.reset_index(drop=True)
    return df

def load(df):
    client = get_bq_client()
    load_to_bq(client, df, PIPELINE_CONFIG['bigquery']['dataset'], 'dim_product')

def run():
    logger = get_logger("dim_product")
    df = extract()
    df_t = transform(df)
    run_dq_checks(df_t)
    load(df_t)
    logger.info("dim_product pipeline completed.")

if __name__ == "__main__":
    run()
