# NYC Taxi Pipeline

## 1. Introduction

This project implements a complete ETL pipeline for NYC taxi data, addressing both **batch** and **streaming** use-cases with Apache Spark.  
- **Batch pipeline** Analyze historical taxi-payment data to understand and influence customer payment behavior. By identifying when and how riders pay (credit card vs. cash), we can design targeted fare-discount promotions for credit-card users.  

- **Streaming pipeline** Ingest live taxi‐trip events, compute 15-minute sliding-window pickup counts per zone in real time, and persist results to Cassandra. These real-time metrics enable downstream systems (e.g., dispatch, dynamic pricing) to balance supply and demand.  

For full details on architecture, design decisions, and results, see the report below.


## 2. Dataset Used

Here is the dataset I used: [NYC DATASET](https://drive.google.com/drive/folders/1eXP7BeyS9-4pOA7n7weqU6-IomX2w4Be?usp=sharing)

**More Info About Dataset**  
- **Original Data Source**  
  https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page  
- **Data Dictionary**  
  https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf  


## 3. Report

[Read the Full Report](https://github.com/HIEUDENHI/nyc_taxi_pipeline/blob/main/NYC_Taxi_Pipeline_Report.pdf)


## 4. Scripts for Project

1. [Spark Streaming App](https://github.com/HIEUDENHI/nyc_taxi_pipeline/blob/main/mnt/spark/apps/stream_cash_payments.py)  

2. [Airflow DAG: Taxi Pipeline Orchestration](https://github.com/HIEUDENHI/nyc_taxi_pipeline/blob/main/mnt/airflow/dags/taxi_pipeline.py)  

3. [Transform to Silver](https://github.com/HIEUDENHI/nyc_taxi_pipeline/blob/main/mnt/airflow/dags/scripts/transform_to_silver.py)  

4. [Transform to Gold](https://github.com/HIEUDENHI/nyc_taxi_pipeline/blob/main/mnt/airflow/dags/scripts/transform_to_gold.py)  

