# Demo Project: PostgreSQL Table Creation

## Project Structure

```
demo_project/
├── config/
│   └── db_config.py
├── sql/
│   ├── 01_create_transactions.sql
│   ├── 02_create_inventory.sql
│   ├── 03_create_sellers.sql
│   ├── 04_create_products.sql
│   └── 05_create_customers.sql
├── scripts/
│   └── run_sql_scripts.py
├── pyproject.toml
└── README.md
```

## Setup

1. Install dependencies (using uv):
   ```sh
   uv pip install -r pyproject.toml
   ```

2. Configure your database credentials in `config/db_config.py`.

3. Run all SQL scripts:
   ```sh
   python scripts/run_sql_scripts.py
   ```

This will execute all SQL files in the `sql/` directory in order.
