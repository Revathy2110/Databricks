# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

#Initialize Spark session
def create_spark_session(app_name = "ETL"):
    return SparkSession.builder.appName(app_name).getOrCreate()

#Bronze Layer: Load raw data into bronze layer
def load_bronze_layer(spark,raw_data_path,bronze_table_path):
    #Load raw data
    bronze_df = spark.read.format("csv").option("header", "true").load(raw_data_path)

    #Save data to delta lake (bronze table)
    bronze_df_write = bronze_df.write.format("delta").mode("overwrite").save(bronze_table_path)

#Silver layer:Clean and transform data
def process_silver_layer(spark,bronze_table_path,silver_table_path):
    #Read data from bronze table
    bronze_df = spark.read.format("delta").load(bronze_table_path)

    #Clean dara: remove duplicates, filter valid records, handle nulls
    silver_df = bronze_df
                .dropDuplicates()
                .filter("age is not null").na.drop()
                .fillna({"city": "Unknown", "state": "Unknown"})
                .withColumn("age", col("age").cast("int"))
                .withColumn("gender", when(col("gender") == "M", "Male").otherwise("Female"))
                .withColumn("state", col("state").cast("string"))

    #save cleaned data to delta lake (silver table)
    silver_df_write = silver_df.write.format("delta").mode("overwrite").save(silver_table_path)
    print(f"Silver layer data saved to {silver_table_path}")

#Gold layer: Aggregate data
def process_gold_layer(spark,silver_table_path,gold_table_path):
    #Read data from silver table
    silver_df = spark.read.format("delta").load(silver_table_path)

    #Aggregate or transform data
    gold_df = silver_df.groupBy("state")
                .agg(avg("age").alias("avg_age") 
                .count("age").alias("count"))
                .orderBy(col("avg_age").desc())

    #save aggregated data to delta lake (gold table)
    gold_df_write = gold_df.write.format("delta").mode("overwrite").save(gold_table_path)

#Pipeline orchestration
def medallion_pipeline(raw_data_path,bronze_table_path,silver_table_path,gold_table_path):
    #Create SparkSession
    spark = create_spark_session()

    #execute each layer
    load_bronze_layer(spark,raw_data_path,bronze_table_path)
    process_silver_layer(spark,bronze_table_path,silver_table_path)
    process_gold_layer(spark,silver_table_path,gold_table_path)

    print("Pipeline completed successfully")

#Main Function
if__name__ == "__main__":
    raw_data_path = "/FileStore/tables/medallion_data.csv"
    bronze_table_path = "/FileStore/tables/bronze"
    silver_table_path = "/FileStore/tables/silver"
    gold_table_path = "/FileStore/tables/gold"

    #Run the pipeline
    medallion_pipeline(raw_data_path,bronze_table_path,silver_table_path,gold_table_path)

                


