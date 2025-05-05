import argparse
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, DoubleType, TimestampType, StringType

def process_file(file_path, spark):
    df = spark.read.parquet(file_path)
    
    # Rename datetime columns
    if 'lpep_pickup_datetime' in df.columns:
        df = df.withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
               .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')
    elif 'tpep_pickup_datetime' in df.columns:
        df = df.withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
               .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')

    # Cast columns
    df = df \
        .withColumn("VendorID", df["VendorID"].cast(LongType())) \
        .withColumn("pickup_datetime", df["pickup_datetime"].cast(TimestampType())) \
        .withColumn("dropoff_datetime", df["dropoff_datetime"].cast(TimestampType())) \
        .withColumn("RatecodeID", df["RatecodeID"].cast(DoubleType())) \
        .withColumn("PULocationID", df["PULocationID"].cast(LongType())) \
        .withColumn("DOLocationID", df["DOLocationID"].cast(LongType())) \
        .withColumn("passenger_count", df["passenger_count"].cast(LongType())) \
        .withColumn("trip_distance", df["trip_distance"].cast(DoubleType())) \
        .withColumn("fare_amount", df["fare_amount"].cast(DoubleType())) \
        .withColumn("extra", df["extra"].cast(DoubleType())) \
        .withColumn("mta_tax", df["mta_tax"].cast(DoubleType())) \
        .withColumn("tip_amount", df["tip_amount"].cast(DoubleType())) \
        .withColumn("tolls_amount", df["tolls_amount"].cast(DoubleType())) \
        .withColumn("improvement_surcharge", df["improvement_surcharge"].cast(DoubleType())) \
        .withColumn("total_amount", df["total_amount"].cast(DoubleType())) \
        .withColumn("payment_type", df["payment_type"].cast(LongType())) \
        .withColumn("congestion_surcharge", df["congestion_surcharge"].cast(DoubleType())) \
        .withColumn("phonenumber", df["phonenumber"].cast(StringType())) \
        .withColumn("full_name", df["full_name"].cast(StringType()))

    # Filter invalid data
    important_columns = ["pickup_datetime", "dropoff_datetime", "trip_distance", "passenger_count"]
    df = df.filter("trip_distance > 0") \
           .filter("passenger_count > 0") \
           .dropna(subset=important_columns)

    return df

def main(year, month):
    spark = SparkSession.builder \
        .master("spark://spark-master:7077") \
        .getOrCreate() \



    bronze_partition_path = f"/datalake/bronze/{year}/{month}"
    silver_partition_path = f"/datalake/silver/{year}/{month}"

    common_columns = [
        'VendorID', 'pickup_datetime', 'dropoff_datetime', 'store_and_fwd_flag',
        'RatecodeID', 'PULocationID', 'DOLocationID', 'passenger_count',
        'trip_distance', 'fare_amount', 'extra', 'mta_tax', 'tip_amount',
        'tolls_amount', 'improvement_surcharge', 'total_amount',
        'payment_type', 'congestion_surcharge', 'phonenumber', 'full_name'
    ]

    # Process files
    dfs = []
    for service_type in ['yellow', 'green']:
        file_path = f"{bronze_partition_path}/{service_type}_tripdata_{year}-{month:02d}.parquet"
        try:
            df_file = process_file(file_path, spark)
            df_file_sel = df_file.select(common_columns) \
                .withColumn('service_type', F.lit(service_type))
            dfs.append(df_file_sel)
           
        except Exception as e:
            print(f"Error processing {file_path}: {str(e)}")

    # Combine DataFrames
    if dfs:
        df_combined = dfs[0]
        for df in dfs[1:]:
            df_combined = df_combined.unionAll(df)
        
        # Write to silver layer
        df_combined.write \
            .mode("overwrite") \
            .parquet(silver_partition_path)
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process taxi data from Bronze to Silver")
    parser.add_argument("--year", type=int, required=True, help="Year to process")
    parser.add_argument("--month", type=int, required=True, help="Month to process")
    args = parser.parse_args()

    main(args.year, args.month)