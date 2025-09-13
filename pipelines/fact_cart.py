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
    """Extract cart event data from the OLTP database."""
    engine = get_sqlalchemy_engine(PIPELINE_CONFIG['oltp_db'])
    query = """
    SELECT 
        ce.cart_event_id,
        ce.customer_id::text,
        ce.product_id::text,
        ce.event_time::date as date_id,
        ce.event_type,
        ce.quantity
    FROM cart_events ce
    """
    df = pd.read_sql(query, engine)
    engine.dispose()
    return df

def transform(df):
    """Transform cart event data."""
    # Ensure proper data types
    df['cart_event_id'] = df['cart_event_id'].astype(str)
    df['customer_id'] = df['customer_id'].astype(str)
    df['product_id'] = df['product_id'].astype(str)
    
    # Add cart_session_id with a default value since it's not in the source
    # In a real scenario, you would get this from the session tracking system
    df['cart_session_id'] = 'session_' + df['cart_event_id']
    
    # Select and order columns to match the target table
    result_df = df[[
        'cart_event_id', 'customer_id', 'product_id', 
        'date_id', 'event_type', 'quantity', 'cart_session_id'
    ]]
    
    # Ensure required columns are present
    required_columns = ['cart_event_id', 'customer_id', 'product_id', 'date_id', 'event_type']
    for col in required_columns:
        if col not in result_df.columns:
            raise ValueError(f"Missing required column: {col}")
    
    return result_df

def load(df):
    """Load data to BigQuery."""
    client = get_bq_client()
    load_to_bq(client, df, PIPELINE_CONFIG['bigquery']['dataset'], 'fact_cart')

def run():
    """Run the cart events pipeline."""
    logger = get_logger("fact_cart")
    try:
        logger.info("Starting cart events pipeline...")
        df = extract()
        df_transformed = transform(df)
        run_dq_checks(df_transformed, required_columns=['cart_event_id', 'customer_id', 'date_id'])
        load(df_transformed)
        logger.info("Cart events pipeline completed successfully.")
    except Exception as e:
        logger.error(f"Error in cart events pipeline: {str(e)}")
        raise

if __name__ == "__main__":
    run()
