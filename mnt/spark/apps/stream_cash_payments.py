#!/usr/bin/env python3
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, broadcast
from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, DoubleType
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType
)
from pyspark.sql.functions import col
def upsert_to_cassandra(batch_df, batch_id):
    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(keyspace="taxi", table="zone_counts") \
        .mode("append") \
        .save()

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("NYC Taxi Zone Counts → Cassandra") \
        .config("spark.cassandra.connection.host", os.getenv("CASSANDRA_HOST", "cassandra")) \
        .config("spark.cassandra.connection.port", os.getenv("CASSANDRA_PORT", "9042")) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # 1) Schema JSON
    jschema = StructType([
    StructField("VendorID",              StringType(),  True),
    StructField("tpep_pickup_datetime",  TimestampType(), True),  
    StructField("passenger_count",       StringType(),  True),  
    StructField("trip_distance",         DoubleType(),  True), 
    StructField("RatecodeID",            StringType(),  True),
    StructField("store_and_fwd_flag",    StringType(),  True),
    StructField("PULocationID",          StringType(),  True),   
    StructField("DOLocationID",          StringType(),  True),
    StructField("phonenumber",           StringType(),  True)
    ])


    zoneSchema = StructType([
    StructField("LocationID",   IntegerType(), nullable=False),
    StructField("Borough",      StringType(),  nullable=True),
    StructField("Zone",         StringType(),  nullable=True),
    StructField("service_zone", StringType(),  nullable=True)
    ])

    # 2) Load lookup zones
    zones_df = (
    spark.read
         .format("csv")
         .option("header", "true")
         .schema(zoneSchema)
         .load("file:///opt/spark-data/taxi_zone_lookup.csv")
         .select(
             col("LocationID").alias("zone_id"),
             col("Zone").alias("zone_name")
        )
    )

    # 3) Read Stream
    raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP", "kafka1:19092")) \
        .option("subscribe", "taxi-stream") \
        .option("startingOffsets", "latest") \
        .load()

    # 4) Parse + watermark
    trips = raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), jschema).alias("d")) \
    .select(
        col("d.tpep_pickup_datetime"),
        col("d.PULocationID").cast("int").alias("PULocationID")
    ) \
    .withWatermark("tpep_pickup_datetime", "20 minutes")

    # 5) Enrich + filter null zones
    trips_enriched = trips.join(
        broadcast(zones_df),
        trips.PULocationID == zones_df.zone_id,
        how="left"
    ).select(
        col("tpep_pickup_datetime"),
        col("zone_name").alias("Zone")
    ).filter(
        col("Zone").isNotNull()
    )

    # 6) Windowed count & rename
    agg = trips_enriched.groupBy(
        window(col("tpep_pickup_datetime"), "15 minutes", "5 minutes"),
        col("Zone")
    ).count() \
     .selectExpr(
         "window.start as window_start",
         "window.end   as window_end",
         "Zone as zone",
         "count as trip_count"
     )

    # 7) Write to Cassandra
    query = agg.writeStream \
        .foreachBatch(upsert_to_cassandra) \
        .outputMode("update") \
        .option("checkpointLocation", "/opt/spark-checkpoints/zone_counts") \
        .start()

    query.awaitTermination()
