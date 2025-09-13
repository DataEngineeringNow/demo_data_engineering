# E-Commerce Data Engineering Project

## Project Overview

This is an end-to-end data engineering project that demonstrates how to build a modern data pipeline for an e-commerce platform. The project showcases:

- **Data Ingestion**: Extracting data from operational databases
- **ETL Processing**: Transforming raw data into a dimensional model
- **Data Warehousing**: Loading processed data into BigQuery
- **Data Quality**: Implementing data quality checks
- **Orchestration**: Managing pipeline dependencies and scheduling

## Learning Objectives

By exploring this project, new data engineers will learn:

1. **Data Modeling**:
   - Star schema design with fact and dimension tables
   - Handling slowly changing dimensions (SCDs)
   - Managing relationships between business entities

2. **ETL Development**:
   - Building idempotent data pipelines
   - Implementing incremental data loading
   - Handling data quality issues

3. **BigQuery Integration**:
   - Schema design in BigQuery
   - Loading data into BigQuery
   - Optimizing queries for performance

4. **Best Practices**:
   - Code organization and modularity
   - Logging and error handling
   - Configuration management
   - Dependency management

## Project Architecture

The project follows a modular architecture:

```
┌───────────────────────────────────────────────────────────────┐
│                      ETL Pipelines                            │
├───────────────┬───────────────────┬───────────────────────────┤
│  Extraction   │   Transformation  │         Loading           │
└───────┬───────┴────────┬──────────┴───────────────┬───────────┘
        │                 │                          │
        ▼                 ▼                          ▼
┌───────────────┐  ┌──────────────┐        ┌──────────────────┐
│   Source      │  │  Data       │        │  Data Warehouse  │
│   Systems     │  │  Processing  │        │  (BigQuery)      │
│  (PostgreSQL) │  │  (Pandas)    │        │                  │
└───────────────┘  └──────────────┘        └──────────────────┘
```

### Data Flow

1. **Source Data**:
   - Customer information
   - Product catalog
   - Sales transactions
   - Marketing campaign data
   - User activity logs

2. **ETL Process**:
   - Extract data from source systems
   - Clean and validate the data
   - Transform into dimensional model
   - Load into data warehouse

3. **Data Warehouse**:
   - Optimized for analytical queries
   - Historical tracking of changes
   - Business-friendly schema

## Project Structure

```
demo_data_engineering/
├── pipelines/                     # ETL pipeline scripts
│   ├── dim_*.py                  # Dimension table pipelines
│   ├── fact_*.py                 # Fact table pipelines
│   └── run_pipelines.py          # Main pipeline runner
├── common/                       # Shared utilities
│   ├── bigquery_client.py
│   ├── config.py
│   ├── db_utils.py
│   └── dq_checks.py
├── project_setup/                # Database setup scripts
│   ├── bigquery_sql/             # BigQuery DDL scripts
│   ├── config/                   # Configuration files
│   ├── data/                     # Sample data
│   └── scripts/                  # Setup scripts
├── pyproject.toml
└── README.md
```

## Setup

### Prerequisites

- Docker and Docker Compose
- Python 3.8+
- Google Cloud SDK (for BigQuery integration)

### Running with Docker

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd demo_data_engineering
   ```

2. **Navigate to the Airflow directory**:
   ```bash
   cd airflow
   ```

3. **Start all services**:
   ```bash
   docker-compose up -d
   ```

4. **Monitor logs**:
   ```bash
   # View all logs
   docker-compose logs -f
   
   # View specific service logs (e.g., scheduler)
   docker-compose logs -f airflow-scheduler
   ```

5. **Access Airflow UI**:
   - Open http://localhost:8080
   - Default credentials: admin/admin

6. **Common Docker commands**:
   ```bash
   # Stop all services
   docker-compose down
   
   # Restart a specific service
   docker-compose restart airflow-webserver
   
   # View running containers
   docker-compose ps
   
   # Remove all containers and volumes (warning: deletes data)
   docker-compose down -v
   ```

### Manual Setup (Alternative)

1. Install dependencies:
   ```sh
   pip install -r pyproject.toml
   ```

2. Set up the database and load initial data:
   ```sh
   cd project_setup
   python scripts/run_sql_scripts.py
   python scripts/create_bigquery_tables.py
   python scripts/load_data.py
   ```

## Running Pipelines

Run all data pipelines in the correct order:
```sh
# From the project root directory
python pipelines/run_pipelines.py
```

### Running Specific Pipelines

Run only specific pipelines (dependencies will be handled automatically):
```sh
# Run only dimension pipelines
python pipelines/run_pipelines.py --pipelines dim_customer dim_product

# Run only fact pipelines
python pipelines/run_pipelines.py --pipelines fact_sales fact_marketing
```

### Pipeline Dependencies

```
                 ┌───────────────┐
                 │   dim_date    │
                 └───────┬───────┘
                         │
    ┌────────────────────┼────────────────────┐
    │                    │                    │
┌───▼────────┐     ┌─────▼───────┐      ┌─────▼───────┐
│ dim_customer│     │ dim_product │      │  dim_seller  │
└────┬────────┘     └─────┬───────┘      └─────┬───────┘
     │                    │                    │
     │                    │                    │
     │             ┌──────▼───────┐             │
     │             │ dim_campaign │             │
     │             └──────┬───────┘             │
     │                    │                    │
     │                    │                    │
     │      ┌─────────────▼──────────────┐      │
     │      │         fact_sales         │      │
     │      └─────────────┬──────────────┘      │
     │                    │                    │
     │   ┌────────────────┼──────────────┐     │
     │   │                │              │     │
┌────▼───▼─────┐   ┌──────▼───────┐  ┌───▼─────────┐
│ fact_inventory│   │ fact_cart    │  │fact_marketing│
└───────────────┘   └──────────────┘  └─────────────┘
```
