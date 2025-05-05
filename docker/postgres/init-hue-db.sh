#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  -- Tạo user hue, đặt password là "hue"
  CREATE USER hue WITH PASSWORD 'hue';

  -- Tạo schema huedb trong database airflow_db
  CREATE SCHEMA IF NOT EXISTS huedb;

  -- Cấp quyền USAGE và CREATE trên schema huedb cho user hue
  GRANT USAGE ON SCHEMA huedb TO hue;
  GRANT CREATE ON SCHEMA huedb TO hue;
EOSQL