# Databricks notebook source
from datetime import date
from dateutil.relativedelta import relativedelta

from pyspark.sql.functions import (
    col,
    when,
    timestamp_diff
)

# COMMAND ----------

# First day of the month two months ago
two_months_ago_start = (
    date.today().replace(day=1)
    - relativedelta(months=2)
)

# First day of the previous month
one_month_ago_start = (
    date.today().replace(day=1)
    - relativedelta(months=1)
)

print("Start:", two_months_ago_start)
print("End:", one_month_ago_start)

# COMMAND ----------

# Read the yellow_trips_raw table from the bronze schema
# Filter rows for the month two months ago

df = spark.read.table(
    "nyctaxi.01_bronze.yellow_trips_raw"
).filter(
    (col("tpep_pickup_datetime") >= str(two_months_ago_start)) &
    (col("tpep_pickup_datetime") < str(one_month_ago_start))
)

display(df)

# COMMAND ----------

df = df.select(
    when(col("VendorID") == 1, "Creative Mobile Technologies, LLC")
        .when(col("VendorID") == 2, "Curb Mobility, LLC")
        .when(col("VendorID") == 6, "Myle Technologies Inc")
        .when(col("VendorID") == 7, "Helix")
        .otherwise("Unknown")
        .alias("vendor"),

    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",

    timestamp_diff(
        "MINUTE",
        df.tpep_pickup_datetime,
        df.tpep_dropoff_datetime
    ).alias("trip_duration"),

    "passenger_count",
    "trip_distance",

    when(col("RatecodeID") == 1, "Standard Rate")
        .when(col("RatecodeID") == 2, "JFK")
        .when(col("RatecodeID") == 3, "Newark")
        .when(col("RatecodeID") == 4, "Nassau or Westchester")
        .when(col("RatecodeID") == 5, "Negotiated Fare")
        .when(col("RatecodeID") == 6, "Group Ride")
        .otherwise("Unknown")
        .alias("rate_type"),

    "store_and_fwd_flag",

    col("PULocationID").alias("pu_location_id"),
    col("DOLocationID").alias("do_location_id"),

    when(col("payment_type") == 0, "Flex Fare trip")
        .when(col("payment_type") == 1, "Credit card")
        .when(col("payment_type") == 2, "Cash")
        .when(col("payment_type") == 3, "No charge")
        .when(col("payment_type") == 4, "Dispute")
        .when(col("payment_type") == 6, "Voided trip")
        .otherwise("Unknown")
        .alias("payment_type"),

    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "congestion_surcharge",

    col("Airport_fee").alias("airport_fee"),

    "cbd_congestion_fee",
    "processed_timestamp"
)

# COMMAND ----------

# Write cleansed data to a Unity Catalog managed Delta table

df.write.mode("append").saveAsTable(
    "nyctaxi.02_silver.yellow_trips_cleansed"
)