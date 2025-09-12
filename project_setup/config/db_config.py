import os
import yaml
from pathlib import Path

# Path to the credentials file
CREDENTIALS_PATH = os.path.join(
    Path(__file__).parent.parent.parent,  # Go up to project root
    'secrets',
    'db_credentials.yaml'
)

# Load database configuration
try:
    with open(CREDENTIALS_PATH, 'r') as f:
        credentials = yaml.safe_load(f)
    
    DB_CONFIG = {
        "host": "demo-ecommerce-demo-dataengineering.k.aivencloud.com",
        "port": 17613,
        "database": "defaultdb",
        "user": credentials.get('user'),
        "password": credentials.get('password'),
        "sslmode": "require"
    }
except FileNotFoundError:
    raise RuntimeError(f"Database credentials not found at {CREDENTIALS_PATH}. "
                      "Please create the file with your database credentials.")
except Exception as e:
    raise RuntimeError(f"Error loading database configuration: {str(e)}")
