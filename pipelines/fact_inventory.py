import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from common.bigquery_client import get_bq_client, load_to_bq
from common.logging_utils import get_logger
from common.dq_checks import run_dq_checks
from common.config import PIPELINE_CONFIG
from common.db_utils import get_sqlalchemy_engine
import pandas as pd

# Initialize logger
logger = get_logger(__name__)

def extract():
    """Extract inventory data from the OLTP database."""
    engine = get_sqlalchemy_engine(PIPELINE_CONFIG['oltp_db'])
    query = """
    SELECT 
        i.inventory_id::text,
        i.product_id::text,
        i.last_updated::date as date_id,
        i.stock_available as closing_stock,
        i.stock_threshold,
        i.restock_date,
        i.last_updated
    FROM inventory i
    """
    df = pd.read_sql(query, engine)
    engine.dispose()
    return df

def transform(df):
    """Transform inventory data."""
    # Ensure proper data types
    df['inventory_id'] = df['inventory_id'].astype(str)
    df['product_id'] = df['product_id'].astype(str)
    
    # Add seller_id with a default value since there's no direct relationship in OLTP
    # In a real scenario, you would join with order history to determine the most common seller
    # or use a separate product-seller mapping table
    logger.warning("No direct seller-product relationship in OLTP. Using default seller 'unknown'")
    df['seller_id'] = 'unknown'
    
    # Calculate derived fields
    # Note: This is a simplified example - adjust calculations based on your business logic
    df['opening_stock'] = 0  # You might need to calculate this from previous day's closing
    df['stock_in'] = 0       # You might need to calculate this from restock events
    df['stock_sold'] = 0     # You might need to calculate this from sales data
    df['stockout_flag'] = df['closing_stock'] <= 0
    
    # Select and order columns to match the target table
    result_df = df[[
        'inventory_id', 'product_id', 'seller_id', 'date_id',
        'opening_stock', 'stock_in', 'stock_sold', 'closing_stock',
        'stockout_flag', 'restock_date'
    ]]
    
    return result_df

def load(df):
    """Load data to BigQuery."""
    client = get_bq_client()
    load_to_bq(client, df, PIPELINE_CONFIG['bigquery']['dataset'], 'fact_inventory')

def run():
    """Run the inventory pipeline."""
    logger = get_logger("fact_inventory")
    try:
        logger.info("Starting inventory pipeline...")
        df = extract()
        df_transformed = transform(df)
        run_dq_checks(df_transformed, required_columns=['inventory_id', 'product_id', 'date_id'])
        load(df_transformed)
        logger.info("Inventory pipeline completed successfully.")
    except Exception as e:
        logger.error(f"Error in inventory pipeline: {str(e)}")
        raise

if __name__ == "__main__":
    run()
