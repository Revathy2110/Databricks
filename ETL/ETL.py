# Databricks notebook source
from pyspark.sql import SparkSession

# Create a Spark Session
spark = SparkSession.builder.appName("MedallionArchitecture").getOrCreate()

# Ingest raw data into the Bronze Layer
bronze_df = spark.read.json("path_to_raw_data.json")  # Replace with your source path

# Save the raw data into Delta Lake (Bronze Table)
bronze_df.write.format("delta").mode("overwrite").save("/mnt/delta/bronze_table")

# COMMAND ----------

from pyspark.sql.functions import col, when

# Read data from Bronze Table
bronze_df = spark.read.format("delta").load("/mnt/delta/bronze_table")

# Data Cleaning: Remove duplicates and filter out bad records
silver_df = bronze_df.dropDuplicates().filter(col("status") == "valid")

# Handle nulls
silver_df = silver_df.fillna({"city": "Unknown", "state": "Unknown"})

# Save the cleaned data into Delta Lake (Silver Table)
silver_df.write.format("delta").mode("overwrite").save("/mnt/delta/silver_table")

# COMMAND ----------

# Read data from Silver Table
silver_df = spark.read.format("delta").load("/mnt/delta/silver_table")

# Aggregate or Transform data for reporting
gold_df = silver_df.groupBy("city").count()  # Example aggregation

# Save the aggregated data into Delta Lake (Gold Table)
gold_df.write.format("delta").mode("overwrite").save("/mnt/delta/gold_table")

