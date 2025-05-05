#!/bin/bash

# Kiểm tra xem schema Hive đã tồn tại hay chưa
/opt/hive/bin/schematool -dbType postgres -info 2>&1 | grep -q "Metastore schema version"

if [ $? -eq 0 ]; then
  echo "[INFO] Metastore schema đã tồn tại, bỏ qua initSchema."
else
  echo "[INFO] Metastore schema chưa tồn tại, tiến hành initSchema..."
  /opt/hive/bin/schematool -dbType postgres -initSchema
fi

# Bắt đầu chạy Hive Metastore
/opt/hive/bin/hive --service metastore
