import sys
import os
from datetime import datetime, timedelta
import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from common.bigquery_client import get_bq_client, load_to_bq
from common.logging_utils import get_logger
from common.dq_checks import run_dq_checks
from common.config import PIPELINE_CONFIG

def extract():
    # Generate date range for last 3 years
    end_date = datetime.today().date()
    start_date = end_date - timedelta(days=3*365)
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    df = pd.DataFrame({'date_id': dates.date})
    return df

def transform(df):
    # Add common date dimension columns
    df['year'] = df['date_id'].apply(lambda x: x.year)
    df['month'] = df['date_id'].apply(lambda x: x.month)
    df['day'] = df['date_id'].apply(lambda x: x.day)
    df['day_of_week'] = df['date_id'].apply(lambda x: x.weekday() + 1)  # Monday=1
    df['day_name'] = df['date_id'].apply(lambda x: x.strftime('%A'))
    df['month_name'] = df['date_id'].apply(lambda x: x.strftime('%B'))
    df['quarter'] = df['date_id'].apply(lambda x: (x.month-1)//3 + 1)
    df['is_weekend'] = df['day_of_week'].apply(lambda x: x >= 6)
    df = df.reset_index(drop=True)
    return df

def load(df):
    client = get_bq_client()
    table_id = f"{PIPELINE_CONFIG['bigquery']['dataset']}.dim_date"
    # Get existing table schema
    table = client.get_table(table_id)
    bq_columns = [field.name for field in table.schema]
    # Select only columns that exist in the table
    df_to_load = df[[col for col in bq_columns if col in df.columns]]
    load_to_bq(client, df_to_load, PIPELINE_CONFIG['bigquery']['dataset'], 'dim_date')

def run():
    logger = get_logger("dim_date")
    df = extract()
    df_t = transform(df)
    run_dq_checks(df_t, required_columns=['date_id'])
    load(df_t)
    logger.info("dim_date pipeline completed.")

if __name__ == "__main__":
    run()
