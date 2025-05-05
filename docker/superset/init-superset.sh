#!/bin/bash
echo "Waiting for PostgreSQL to be ready..."
until pg_isready -h postgres -p 5432 -U airflow -d superset_db > /dev/null 2>&1; do
  echo "PostgreSQL is not ready yet. Retrying in 2 seconds..."
  sleep 2
done

echo "Initializing Superset database..."
superset db upgrade

echo "Creating admin user..."
superset fab create-admin \
    --username admin \
    --firstname Superset \
    --lastname Admin \
    --email admin@superset.com \
    --password admin

echo "Initializing Superset roles and permissions..."
superset init

echo "Starting Superset..."
exec gunicorn --bind 0.0.0.0:8088 --workers 4 --timeout 120 superset:app