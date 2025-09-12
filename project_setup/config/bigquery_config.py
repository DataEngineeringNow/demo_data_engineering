import os
import yaml
from pathlib import Path

# Path to the secrets directory
SECRETS_DIR = os.path.join(Path(__file__).parent.parent.parent, 'secrets')

# BigQuery configuration
PROJECT_ID = 'ngds-data-engineer'
KEY_PATH = os.path.join(SECRETS_DIR, 'ngds_bigquery_service_account.json')
SQL_DIR = 'bigquery_sql'
DATASET_ID = 'ecommerce_dw'

def get_bigquery_credentials():
    """Load BigQuery credentials from the secrets directory."""
    if not os.path.exists(KEY_PATH):
        raise FileNotFoundError(
            f"BigQuery service account key not found at {KEY_PATH}. "
            "Please place your service account key file in the secrets directory."
        )
    return KEY_PATH
