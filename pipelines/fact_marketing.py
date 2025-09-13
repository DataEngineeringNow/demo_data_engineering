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
    """Extract marketing data from the OLTP database."""
    engine = get_sqlalchemy_engine(PIPELINE_CONFIG['oltp_db'])
    query = """
    SELECT 
        CONCAT('mkt_', cp.campaign_id::text, '_', cp.customer_id::text, '_', REPLACE(cp.date::text, '-', '')) as marketing_id,
        cp.campaign_id::text,
        cp.customer_id::text,
        cp.date as date_id,
        cp.impressions,
        cp.clicks,
        cp.conversions,
        cp.cost_spent as spend,
        CASE 
            WHEN cp.clicks > 0 THEN cp.cost_spent / NULLIF(cp.clicks, 0) 
            ELSE 0 
        END as cpc,
        CASE 
            WHEN cp.conversions > 0 THEN cp.cost_spent / NULLIF(cp.conversions, 0)
            ELSE 0 
        END as cpa,
        CASE 
            WHEN cp.impressions > 0 THEN cp.clicks::float / NULLIF(cp.impressions, 0)
            ELSE 0 
        END as ctr
    FROM campaign_performance cp
    """
    df = pd.read_sql(query, engine)
    engine.dispose()
    return df

def transform(df):
    """Transform marketing data with strict type conversion."""
    import numpy as np
    
    # Create a copy to avoid SettingWithCopyWarning
    df = df.copy()
    
    # Generate marketing_id
    df['marketing_id'] = 'mkt_' + df['campaign_id'].astype(str) + '_' + df['customer_id'].astype(str) + '_' + df['date_id'].astype(str).str.replace('-', '')
    
    # Ensure string types
    df['campaign_id'] = df['campaign_id'].astype(str).str.strip()
    df['customer_id'] = df['customer_id'].astype(str).str.strip()
    
    # Ensure date is in proper format
    df['date_id'] = pd.to_datetime(df['date_id']).dt.date
    
    # Define numeric columns and their target types
    numeric_cols = {
        'impressions': 'int64',
        'clicks': 'int64',
        'conversions': 'int64',
        'spend': 'float64',
        'cpc': 'float64',
        'cpa': 'float64',
        'ctr': 'float64'
    }
    
    # Convert numeric columns
    for col, dtype in numeric_cols.items():
        if col in df.columns:
            # Convert to numeric, coerce errors to NaN, then fill NaN with 0 for int columns
            df[col] = pd.to_numeric(df[col], errors='coerce')
            if 'int' in dtype:
                df[col] = df[col].fillna(0).astype('int64')
            else:
                df[col] = df[col].fillna(0.0)
    
    # Select and order columns to match the target table
    result_df = df[[
        'marketing_id', 'campaign_id', 'customer_id', 'date_id',
        'impressions', 'clicks', 'conversions', 'spend', 'cpc', 'cpa', 'ctr'
    ]].copy()
    
    # Ensure required columns are present and have non-null values
    required_columns = ['marketing_id', 'campaign_id', 'date_id', 'impressions', 'clicks', 'conversions']
    for col in required_columns:
        if col not in result_df.columns:
            raise ValueError(f"Missing required column: {col}")
        if result_df[col].isnull().any():
            raise ValueError(f"Column {col} contains null values")
    
    return result_df

def load(df):
    """Load data into BigQuery with explicit schema and detailed error handling."""
    import logging
    from google.cloud import bigquery
    from google.cloud.bigquery import SchemaField
    from common.bigquery_client import get_bq_client
    
    logger = logging.getLogger("fact_marketing")
    
    try:
        # Log dataframe info before processing
        logger.info(f"DataFrame columns: {df.columns.tolist()}")
        logger.info(f"DataFrame dtypes:\n{df.dtypes}")
        
        # Explicitly define schema for BigQuery
        schema = [
            SchemaField("marketing_id", "STRING", mode="REQUIRED"),
            SchemaField("campaign_id", "STRING", mode="REQUIRED"),
            SchemaField("customer_id", "STRING", mode="REQUIRED"),
            SchemaField("date_id", "DATE", mode="REQUIRED"),
            SchemaField("impressions", "INT64", mode="REQUIRED"),
            SchemaField("clicks", "INT64", mode="REQUIRED"),
            SchemaField("conversions", "INT64", mode="REQUIRED"),
            SchemaField("spend", "FLOAT64", mode="REQUIRED"),
            SchemaField("cpc", "FLOAT64", mode="REQUIRED"),
            SchemaField("cpa", "FLOAT64", mode="REQUIRED"),
            SchemaField("ctr", "FLOAT64", mode="REQUIRED")
        ]
        
        # Ensure all required columns are present and have correct types
        required_columns = [
            'marketing_id', 'campaign_id', 'customer_id', 'date_id',
            'impressions', 'clicks', 'conversions', 'spend', 'cpc', 'cpa', 'ctr'
        ]
        
        # Check for missing columns
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
            
        df = df[required_columns].copy()
        
        # Log sample data
        logger.info("Sample data before conversion:")
        logger.info(df.head(2).to_dict('records'))
        
        # Convert date string to datetime if it's not already
        if not pd.api.types.is_datetime64_any_dtype(df['date_id']):
            df['date_id'] = pd.to_datetime(df['date_id'], errors='coerce')
        df['date_id'] = df['date_id'].dt.date
        
        # Ensure numeric types
        int_cols = ['impressions', 'clicks', 'conversions']
        float_cols = ['spend', 'cpc', 'cpa', 'ctr']
        
        for col in int_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')
            
        for col in float_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
        
        logger.info("Data types after conversion:")
        logger.info(df.dtypes)
        
        # Get BigQuery client and table reference
        client = get_bq_client()
        table_id = f"{PIPELINE_CONFIG['bigquery']['dataset']}.fact_marketing"
        
        # Configure load job
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        
        logger.info(f"Starting BigQuery load to {table_id}")
        
        # Load data in smaller chunks to avoid memory issues
        chunk_size = 1000
        total_rows = len(df)
        
        for i in range(0, total_rows, chunk_size):
            chunk = df.iloc[i:i + chunk_size]
            logger.info(f"Loading chunk {i//chunk_size + 1} of {(total_rows-1)//chunk_size + 1}")
            
            job = client.load_table_from_dataframe(
                chunk, table_id, job_config=job_config
            )
            job.result()  # Wait for the job to complete
            
            if job.errors:
                logger.error(f"Error in chunk {i//chunk_size + 1}: {job.errors}")
                raise RuntimeError(f"Error loading chunk {i//chunk_size + 1}: {job.errors}")
                
        logger.info(f"Successfully loaded {total_rows} rows to {table_id}")
        
    except Exception as e:
        logger.error(f"Error in load function: {str(e)}")
        logger.exception("Full traceback:")
        
        # Log problematic rows if possible
        if 'df' in locals():
            try:
                logger.info("Sample of problematic data:")
                logger.info(df.head(2).to_dict('records'))
            except Exception as log_e:
                logger.error(f"Could not log sample data: {str(log_e)}")
                
        raise  # Re-raise the exception after logging

def run():
    """Run the marketing pipeline."""
    logger = get_logger("fact_marketing")
    try:
        logger.info("Starting marketing pipeline...")
        df = extract()
        df_transformed = transform(df)
        run_dq_checks(df_transformed, required_columns=['marketing_id', 'campaign_id', 'date_id'])
        load(df_transformed)
        logger.info("Marketing pipeline completed successfully.")
    except Exception as e:
        logger.error(f"Error in marketing pipeline: {str(e)}")
        raise

if __name__ == "__main__":
    run()
