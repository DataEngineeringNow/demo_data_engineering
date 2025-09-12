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
    query = '''
    SELECT oi.order_item_id AS sales_id, o.order_id, o.customer_id, oi.product_id, o.seller_id, o.order_date AS date_id,
           oi.quantity, (oi.quantity * oi.unit_price) AS gross_value, oi.discount, oi.tax,
           ((oi.quantity * oi.unit_price) - oi.discount + oi.tax) AS net_revenue,
           (oi.quantity * p.cost_price) AS cost, (((oi.quantity * oi.unit_price) - oi.discount + oi.tax) - (oi.quantity * p.cost_price)) AS profit,
           o.order_date, o.delivery_date, (o.delivery_date - o.order_date) AS fulfillment_time
    FROM order_items oi
    JOIN orders o ON oi.order_id = o.order_id
    JOIN products p ON oi.product_id = p.product_id
    '''
    df = pd.read_sql(query, engine)
    engine.dispose()
    return df


def transform(df):
    df['sales_id'] = df['sales_id'].astype(str)
    df['order_id'] = df['order_id'].astype(str)
    df['customer_id'] = df['customer_id'].astype(str)
    df['product_id'] = df['product_id'].astype(str)
    df['seller_id'] = df['seller_id'].astype(str)
    df['date_id'] = pd.to_datetime(df['date_id'])
    df['order_date'] = pd.to_datetime(df['order_date'])
    df['delivery_date'] = pd.to_datetime(df['delivery_date'])
    # Robustly decode any bytes columns to string
    for col in df.columns:
        if df[col].dtype == object:
            if df[col].apply(lambda x: isinstance(x, bytes)).any():
                df[col] = df[col].apply(lambda x: x.decode('utf-8') if isinstance(x, bytes) else x)
    for col in df.columns:
        if str(df[col].dtype) == 'bytes' or df[col].apply(lambda x: isinstance(x, bytes)).any():
            df[col] = df[col].apply(lambda x: x.decode('utf-8') if isinstance(x, bytes) else str(x))
    if 'fulfillment_time' in df:
        if pd.api.types.is_timedelta64_dtype(df['fulfillment_time']):
            df['fulfillment_time'] = df['fulfillment_time'].dt.days
        else:
            df['fulfillment_time'] = df['fulfillment_time'].astype('Int64')
    df = df.reset_index(drop=True)
    return df


def load(df):
    client = get_bq_client()
    load_to_bq(client, df, PIPELINE_CONFIG['bigquery']['dataset'], 'fact_sales')


def run():
    logger = get_logger("fact_sales")
    df = extract()
    df_t = transform(df)
    run_dq_checks(df_t, required_columns=['sales_id', 'order_id', 'customer_id', 'product_id'])
    load(df_t)
    logger.info("fact_sales pipeline completed.")


if __name__ == "__main__":
    run()
