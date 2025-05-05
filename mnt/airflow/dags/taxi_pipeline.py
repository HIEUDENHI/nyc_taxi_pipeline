from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    "taxi_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    params={"year": "2025", "month": "01"}
) as dag:

    ingest_to_hdfs = BashOperator(
        task_id="ingest_to_hdfs",
        bash_command="""
        hdfs dfs -mkdir -p /datalake/bronze/{{ params.year }}/{{ params.month }}/ &&
        hdfs dfs -mkdir -p /datalake/silver/{{ params.year }}/{{ params.month }}/ &&
        hdfs dfs -put -f /opt/bronze_input/green_tripdata_{{ params.year }}-{{ params.month }}.parquet /datalake/bronze/{{ params.year }}/{{ params.month }}/ &&
        hdfs dfs -put -f /opt/bronze_input/yellow_tripdata_{{ params.year }}-{{ params.month }}.parquet /datalake/bronze/{{ params.year }}/{{ params.month }}/
        """
    )

    transform_to_silver = SparkSubmitOperator(
        task_id="transform_to_silver",
        application="/opt/airflow/dags/scripts/transform_to_silver.py",
        application_args=["--year", "{{ params.year }}", "--month", "{{ params.month }}"],
        conn_id="spark_conn",
        executor_memory="6G",
        executor_cores=4,

        verbose=False
    )

    transform_to_gold = SparkSubmitOperator(
        task_id="transform_to_gold",
        application="/opt/airflow/dags/scripts/transform_to_gold.py",
        application_args=["--year", "{{ params.year }}", "--month", "{{ params.month }}"],
        conn_id="spark_conn",
        executor_memory="6G",
        executor_cores=4,
        verbose=False
    )

    update_hive_partitions = HiveOperator(
        task_id="update_hive_partitions",
        hql="""
        ALTER TABLE fact_trips ADD IF NOT EXISTS PARTITION (year={{ params.year }}, month={{ params.month }})
        LOCATION '/datalake/gold/fact_trips/year={{ params.year }}/month={{ params.month }}';
        """,
        hive_cli_conn_id="hive_conn"
    )

    ingest_to_hdfs >> transform_to_silver >> transform_to_gold >> update_hive_partitions