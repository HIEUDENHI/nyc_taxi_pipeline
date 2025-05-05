import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, DoubleType, TimestampType, StringType, DateType

def create_date_dim(spark, df):
    pickup_dates = df.select(F.to_date("pickup_datetime").alias("date"))
    dropoff_dates = df.select(F.to_date("dropoff_datetime").alias("date"))
    all_dates = pickup_dates.union(dropoff_dates).distinct()
    
    dim_date = all_dates.select(
        "date",
        F.dayofmonth("date").alias("day"),
        F.month("date").alias("month"),
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.dayofweek("date").alias("day_of_week"),
      
    )
    
    return dim_date

def create_location_dim(spark, lookup_path):
    df_lookup = spark.read.csv(lookup_path, header=True, inferSchema=True)
    
    dim_location = df_lookup.select(
        F.col("LocationID").alias("location_id"),
        F.col("Borough").alias("borough"),
        F.col("Zone").alias("zone"),
        F.col("service_zone")
    )
    
    return dim_location


def create_vendor_dim(spark):
    vendor_data = [
        (1, "Creative Mobile Technologies, LLC"),
        (2, "Curb Mobility, LLC"),
        (6, "Myle Technologies Inc"),
        (7, "Helix")
    ]
    return spark.createDataFrame(vendor_data, ["vendor_id", "vendor_name"])


def create_ratecode_dim(spark):
    ratecode_data = [
        (1, "Standard rate"),
        (2, "JFK"),
        (3, "Newark"),
        (4, "Nassau or Westchester"),
        (5, "Negotiated fare"),
        (6, "Group ride"),
        (99, "Null/unknown")
    ]
    return spark.createDataFrame(ratecode_data, ["ratecode_id", "ratecode_description"])

def create_payment_type_dim(spark):
    payment_type_data = [
        (0, "Flex Fare"),
        (1, "Credit card"),
        (2, "Cash"),
        (3, "No charge"),
        (4, "Dispute"),
        (5, "Unknown"),
        (6, "Voided trip")
    ]
    return spark.createDataFrame(payment_type_data, ["payment_type_id", "payment_type_description"])


def create_customer_dim(spark, df, year, month):
    # Select customer attributes from the silver data, only phonenumber and full_name
    new_customers = df.select(
        "phonenumber",
        "full_name"
    ).distinct()
    
    existing_path = f"/datalake/gold/dim_customer"
    try:
        existing_customers = spark.read.parquet(existing_path)
        final_customers = new_customers.join(
            existing_customers,
            ["phonenumber"],
            "left_outer"
        ).select(
            new_customers["phonenumber"],
            F.coalesce(existing_customers["full_name"], new_customers["full_name"]).alias("full_name")
        )
    except:
        # If no existing dimension exists, just use the new customers
        final_customers = new_customers
    
    return final_customers

def create_fact_table(spark, df, dim_date, dim_location, dim_customer, dim_vendor, dim_ratecode, dim_payment_type, year, month):
    silver_df = df.alias("silver")
    pickup_date_dim = dim_date.alias("pickup_date_dim")
    dropoff_date_dim = dim_date.alias("dropoff_date_dim")
    dim_location_pu = dim_location.alias("pu_location")
    dim_location_do = dim_location.alias("do_location")
    dim_customer_alias = dim_customer.alias("dim_customer")
    dim_vendor_alias = dim_vendor.alias("dim_vendor")
    dim_ratecode_alias = dim_ratecode.alias("dim_ratecode")
    dim_payment_type_alias = dim_payment_type.alias("dim_payment_type")
    
    silver_columns = df.columns
    
    df_with_pickup_date = silver_df.join(pickup_date_dim, 
                                        F.to_date(F.col("silver.pickup_datetime")) == F.col("pickup_date_dim.date"), 
                                        "left")
    
    # Explicitly select silver columns and the renamed pickup_date
    df_with_pickup_date = df_with_pickup_date.select(
        *[F.col(f"silver.{col}") for col in silver_columns],
        F.col("pickup_date_dim.date").alias("pickup_date")
    ).alias("with_pickup_date")
    
    # Join with date dimensions for dropoff date
    df_with_dates = df_with_pickup_date.join(dropoff_date_dim,
                                            F.to_date(F.col("with_pickup_date.dropoff_datetime")) == F.col("dropoff_date_dim.date"),
                                            "left")
    
    # Explicitly select silver columns, pickup_date, and the renamed dropoff_date
    df_with_dates = df_with_dates.select(
        *[F.col(f"with_pickup_date.{col}") for col in silver_columns],
        F.col("with_pickup_date.pickup_date"),
        F.col("dropoff_date_dim.date").alias("dropoff_date")
    ).alias("with_dates")
    
    # Join with location dimensions for pickup location
    df_with_pu = df_with_dates.join(dim_location_pu,
                                   F.col("with_dates.PULocationID") == F.col("pu_location.location_id"),
                                   "left")
    
    df_with_pu = df_with_pu.select(
        *[F.col(f"with_dates.{col}") for col in silver_columns],
        F.col("with_dates.pickup_date"),
        F.col("with_dates.dropoff_date"),
        F.col("pu_location.location_id").alias("pickup_location_id")
    ).alias("with_pu")
    
    df_with_locations = df_with_pu.join(dim_location_do,
                                       F.col("with_pu.DOLocationID") == F.col("do_location.location_id"),
                                       "left")
    
    # Select necessary columns and rename location_id to dropoff_location_id
    df_with_locations = df_with_locations.select(
        *[F.col(f"with_pu.{col}") for col in silver_columns],
        F.col("with_pu.pickup_date"),
        F.col("with_pu.dropoff_date"),
        F.col("with_pu.pickup_location_id"),
        F.col("do_location.location_id").alias("dropoff_location_id")
    ).alias("with_locations")
    
    # Join with customer dimension using phonenumber
    df_with_customer = df_with_locations.join(dim_customer_alias,
                                             F.col("with_locations.phonenumber") == F.col("dim_customer.phonenumber"),
                                             "left")
    
    # Select necessary columns, keeping phonenumber from the silver data
    df_with_customer = df_with_customer.select(
        *[F.col(f"with_locations.{col}") for col in silver_columns],
        F.col("with_locations.pickup_date"),
        F.col("with_locations.dropoff_date"),
        F.col("with_locations.pickup_location_id"),
        F.col("with_locations.dropoff_location_id")
    ).alias("with_customer")
    
    # Join with vendor dimension
    df_with_vendor = df_with_customer.join(dim_vendor_alias,
                                          F.col("with_customer.VendorID") == F.col("dim_vendor.vendor_id"),
                                          "left")
    
    # Select necessary columns, adding vendor_id
    df_with_vendor = df_with_vendor.select(
        *[F.col(f"with_customer.{col}") for col in silver_columns],
        F.col("with_customer.pickup_date"),
        F.col("with_customer.dropoff_date"),
        F.col("with_customer.pickup_location_id"),
        F.col("with_customer.dropoff_location_id"),
        F.col("dim_vendor.vendor_id").alias("vendor_id")
    ).alias("with_vendor")
    
    # Join with ratecode dimension
    df_with_ratecode = df_with_vendor.join(dim_ratecode_alias,
                                          F.col("with_vendor.RatecodeID") == F.col("dim_ratecode.ratecode_id"),
                                          "left")
    
    # Select necessary columns, adding ratecode_id
    df_with_ratecode = df_with_ratecode.select(
        *[F.col(f"with_vendor.{col}") for col in silver_columns],
        F.col("with_vendor.pickup_date"),
        F.col("with_vendor.dropoff_date"),
        F.col("with_vendor.pickup_location_id"),
        F.col("with_vendor.dropoff_location_id"),
        F.col("with_vendor.vendor_id"),
        F.col("dim_ratecode.ratecode_id").alias("ratecode_id")
    ).alias("with_ratecode")
    
    # Join with payment type dimension
    df_final = df_with_ratecode.join(dim_payment_type_alias,
                                    F.col("with_ratecode.payment_type") == F.col("dim_payment_type.payment_type_id"),
                                    "left")
    
    # Select necessary columns, adding payment_type_id
    df_final = df_final.select(
        *[F.col(f"with_ratecode.{col}") for col in silver_columns],
        F.col("with_ratecode.pickup_date"),
        F.col("with_ratecode.dropoff_date"),
        F.col("with_ratecode.pickup_location_id"),
        F.col("with_ratecode.dropoff_location_id"),
        F.col("with_ratecode.vendor_id"),
        F.col("with_ratecode.ratecode_id"),
        F.col("dim_payment_type.payment_type_id").alias("payment_type_id")
    ).alias("final")
    
    # Create fact table with all measures and dimension references
    fact_trips = df_final.select(
        F.col("final.phonenumber"),
        F.col("final.vendor_id"),
        F.col("final.ratecode_id"),
        F.col("final.payment_type_id"),
        F.col("final.pickup_date"),
        F.col("final.dropoff_date"),
        F.col("final.pickup_location_id"),
        F.col("final.dropoff_location_id"),
        F.col("final.passenger_count"),
        F.col("final.trip_distance"),
        F.col("final.fare_amount"),
        F.col("final.extra"),
        F.col("final.mta_tax"),
        F.col("final.tip_amount"),
        F.col("final.tolls_amount"),
        F.col("final.improvement_surcharge"),
        F.col("final.total_amount"),
        F.col("final.congestion_surcharge"),
        F.col("final.store_and_fwd_flag"),
        F.col("final.service_type"),
        F.lit(year).alias("year"),
        F.lit(month).alias("month")
    )
    
    return fact_trips

def main(year, month):
    spark = SparkSession.builder \
        .master("spark://spark-master:7077") \
        .getOrCreate()


    silver_path = f"/datalake/silver/{year}/{month}"
    
    gold_path = f"/datalake/gold"
    lookup_path = "/datalake/bronze/taxi_zone_lookup/taxi_zone_lookup.csv"

  
    df = spark.read.parquet(silver_path)
    df.cache()  

    # Create dimensions
    dim_date = create_date_dim(spark, df)
    dim_location = create_location_dim(spark, lookup_path)
    dim_vendor = create_vendor_dim(spark)
    dim_ratecode = create_ratecode_dim(spark)
    dim_payment_type = create_payment_type_dim(spark)
    dim_customer = create_customer_dim(spark, df, year, month)
    fact_trips = create_fact_table(spark, df, dim_date, dim_location, dim_customer, 
                                 dim_vendor, dim_ratecode, dim_payment_type, year, month)

    # Write dimensions and fact table
    dim_date.write.mode("append").parquet(f"{gold_path}/dim_date")
    dim_location.write.mode("overwrite").parquet(f"{gold_path}/dim_location")
    dim_vendor.write.mode("overwrite").parquet(f"{gold_path}/dim_vendor")
    dim_ratecode.write.mode("overwrite").parquet(f"{gold_path}/dim_ratecode")
    dim_payment_type.write.mode("overwrite").parquet(f"{gold_path}/dim_payment_type")
    dim_customer.write.mode("overwrite").parquet(f"{gold_path}/dim_customer")
    
    # Write fact table with partitioning by year and month
    fact_trips.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(f"{gold_path}/fact_trips")

    df.unpersist()

    spark.stop()

if __name__ == "__main__":
   parser = argparse.ArgumentParser(description="Process taxi data from Silver to Gold")
   parser.add_argument("--year", type=int, required=True, help="Year to process")
   parser.add_argument("--month", type=int, required=True, help="Month to process")
   args = parser.parse_args()


   main(args.year, args.month)

