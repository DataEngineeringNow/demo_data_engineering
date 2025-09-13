# Airflow Orchestration for E-commerce Data Pipeline

This directory contains the Airflow DAGs and configuration for orchestrating the e-commerce data pipeline.

## Directory Structure

```
airflow/
├── dags/                       # Airflow DAG definitions
│   └── ecommerce_data_pipeline.py  # Main DAG for e-commerce ETL
└── requirements.txt            # Python dependencies for Airflow
```

## Prerequisites

1. Install Docker and Docker Compose
2. Python 3.8+ with pip

## Setup Instructions

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Initialize Airflow Database**:
   ```bash
   airflow db init
   ```

3. **Create an Airflow User** (if not already created):
   ```bash
   airflow users create \
       --username admin \
       --firstname Admin \
       --lastname User \
       --role Admin \
       --email admin@example.com
   ```

4. **Set Environment Variables**:
   Create a `.env` file in the `airflow` directory with the following variables:
   ```
   AIRFLOW_UID=$(id -u)
   AIRFLOW_GID=0
   ```

5. **Start Airflow using Docker Compose**:
   ```bash
   # Navigate to the airflow directory
   cd /path/to/airflow
   
   # Start all services in detached mode
   docker-compose up -d
   
   # View logs (follow mode)
   docker-compose logs -f
   
   # Stop all services
   docker-compose down
   
   # Restart a specific service (e.g., webserver)
   docker-compose restart airflow-webserver
   
   # View running containers
   docker-compose ps
   ```
   
   Access the Airflow web interface at: http://localhost:8080

## DAG Details

- **DAG ID**: `ecommerce_data_pipeline`
- **Schedule**: Daily
- **Default Arguments**:
  - Retries: 2
  - Retry Delay: 5 minutes
  - Email on failure: True

## Task Dependencies

The DAG follows this execution order:

1. `dim_date`
2. `dim_customer`, `dim_product`, `dim_seller` (parallel)
3. `dim_campaign`
4. `fact_sales`
5. `fact_inventory`, `fact_cart`, `fact_marketing` (parallel)

## Monitoring

Access the Airflow web interface at `http://localhost:8080` to:
- Monitor DAG runs
- View task logs
- Trigger manual runs
- Check task status

## Troubleshooting

- If tasks fail, check the logs in the Airflow web interface
- Ensure all required environment variables are set
- Verify database connections are properly configured
