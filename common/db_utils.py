"""Database utility functions for the data pipeline."""
from typing import Dict, Any
import psycopg2
from sqlalchemy import create_engine

def get_db_connection(db_config: Dict[str, Any]) -> psycopg2.extensions.connection:
    """Create a PostgreSQL database connection using psycopg2.
    
    Args:
        db_config: Dictionary containing database connection parameters
        
    Returns:
        A psycopg2 connection object
    """
    return psycopg2.connect(
        host=db_config['host'],
        port=db_config['port'],
        database=db_config['database'],
        user=db_config['user'],
        password=db_config['password']
    )

def get_sqlalchemy_engine(db_config: Dict[str, Any]) -> 'sqlalchemy.engine.Engine':
    """Create a SQLAlchemy engine for the database.
    
    Args:
        db_config: Dictionary containing database connection parameters
        
    Returns:
        A SQLAlchemy engine instance
    """
    db_url = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    return create_engine(db_url)
