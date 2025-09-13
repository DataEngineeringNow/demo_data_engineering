#!/bin/bash

# Wait for the database to be ready
until psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c 'SELECT 1' >/dev/null 2>&1; do
  echo "Waiting for database to be ready..."
  sleep 1
done

# Check if admin user already exists
USER_EXISTS=$(psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT 1 FROM ab_user WHERE username='admin'")

if [ -z "$USER_EXISTS" ]; then
  echo "Creating admin user..."
  airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
  echo "Admin user created successfully!"
else
  echo "Admin user already exists, skipping creation."
fi
