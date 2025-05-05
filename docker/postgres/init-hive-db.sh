#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  -- Xóa role hive nếu tồn tại
  DROP ROLE IF EXISTS hive;

  -- Tạo user hive, đặt password là "hive"
  CREATE USER hive WITH PASSWORD 'hive';

  -- Tạo schema metastore trong database airflow_db
  CREATE SCHEMA IF NOT EXISTS metastore;

  -- Cấp quyền USAGE và CREATE trên schema metastore cho user hive
  GRANT USAGE ON SCHEMA metastore TO hive;
  GRANT CREATE ON SCHEMA metastore TO hive;
EOSQL