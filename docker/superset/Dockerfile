FROM apache/superset:latest

# Cài đặt psycopg2 và các phụ thuộc
USER root
RUN pip install --no-cache-dir psycopg2-binary trino
# Chuyển về user mặc định của Superset
USER superset