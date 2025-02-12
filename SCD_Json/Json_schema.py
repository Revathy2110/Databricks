# Databricks notebook source
# DBTITLE 1,importing libraries
from pyspark.sql.functions import col, lit, current_timestamp, when,explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, ArrayType
from delta.tables import DeltaTable

# COMMAND ----------

schema = StructType([
    StructField("country", StringType(), True),
    StructField("data", ArrayType(
        StructType([
            StructField("date_time", StringType(), True),
            StructField("Key_in_date", StringType(), True),
            StructField("lastmodified_date", StringType(), True),
            StructField("task_id", StringType(), True),
            StructField("store-id", StringType(), True),
            StructField("member_login_id", StringType(), True),
            StructField("check_in", StringType(), True),
            StructField("check_out", StringType(), True),
            StructField("user_location", ArrayType(DoubleType()), True),
            StructField("distance", StringType(), True),
            StructField("questions", ArrayType(
                StructType([
                    StructField("type", StringType(), True),
                    StructField("question", StringType(), True),
                    StructField("question_id", StringType(), True),
                    StructField("answer", ArrayType(
                        StructType([
                            StructField("product_id", StringType(), True),
                            StructField("product_code", StringType(), True),
                            StructField("price_gross", DoubleType(), True),
                            StructField("price_net", DoubleType(), True),
                            StructField("price_discount", DoubleType(), True),
                            StructField("price_incentive", DoubleType(), True),
                            StructField("sales_unit", DoubleType(), True),
                            StructField("sales_gross", DoubleType(), True),
                            StructField("sales_net", DoubleType(), True),
                            StructField("promotion", StringType(), True),
                            StructField("answer_id", StringType(), True)
                        ])
                    ), True)
                ])
            ), True)
        ])
    ), True),
    StructField("total", StringType(), True),
    StructField("is_end", StringType(), True)
])



# COMMAND ----------

# raw_df = spark.read.format("json").option("multiline", "true").schema(schema).load("dbfs:/FileStore/shared_uploads/revathy.s@diggibyte.com/data_2-1.json")

# COMMAND ----------

# # Explode the `data` array
# data_df = raw_df.select(
#     col("country"),
#     explode(col("data")).alias("data")
# )

# display(data_df)

# COMMAND ----------

# # Extract fields from the exploded `data`
# data_flat = data_df.select(
#     col("country"),
#     col("data.date_time"),
#     col("data.Key_in_date"),
#     col("data.lastmodified_date"),
#     col("data.task_id"),
#     col("data.store-id").alias("store_id"),
#     col("data.member_login_id"),
#     col("data.check_in"),
#     col("data.check_out"),
#     col("data.user_location").alias("user_location"),
#     col("data.distance"),
#     explode(col("data.questions")).alias("questions")
# )

# display(data_flat)

# COMMAND ----------

# df_task_id = data_df.select(
#     col("country"),
#     col("data.*"),
#     explode(col("data.questions")).alias("questions")
# )
# display(df_task_id)

# COMMAND ----------

# # Check for duplicates
# task_id_duplicates = df_task_id.groupBy("task_id").agg(count("*").alias("count")).filter(col("count") > 1)

# if task_id_duplicates.count() > 0:
#     print("Duplicate task_id found:")
#     task_id_duplicates.show()
# else:
#     print("All task_id values are unique.")

# # Check for null or missing task_id values
# task_id_nulls = df_task_id.filter(col("task_id").isNull())



# COMMAND ----------

# # Extract fields from the `questions` array
# questions_flat = data_flat.select(
#     col("country"),
#     col("date_time"),
#     col("Key_in_date"),
#     col("lastmodified_date"),
#     col("task_id"),
#     col("store_id"),
#     col("member_login_id"),
#     col("check_in"),
#     col("check_out"),
#     col("user_location"),
#     col("distance"),
#     col("questions.type").alias("question_type"),
#     col("questions.question"),
#     col("questions.question_id"),
#     explode(col("questions.answer")).alias("answers")
# )
# display(questions_flat)

# COMMAND ----------

# # Extract fields from the `answers` array
# final_flattened_df = questions_flat.select(
#     col("country"),
#     col("date_time"),
#     col("Key_in_date"),
#     col("lastmodified_date"),
#     col("task_id"),
#     col("store_id"),
#     col("member_login_id"),
#     col("check_in"),
#     col("check_out"),
#     col("user_location"),
#     col("distance"),
#     col("question_type"),
#     col("question"),
#     col("question_id"),
#     col("answers.product_id"),
#     col("answers.product_code"),
#     col("answers.price_gross"),
#     col("answers.price_net"),
#     col("answers.price_discount"),
#     col("answers.price_incentive"),
#     col("answers.sales_unit"),
#     col("answers.sales_gross"),
#     col("answers.sales_net"),
#     col("answers.promotion"),
#     col("answers.answer_id"))

# display(final_flattened_df)   

# COMMAND ----------

# # Show the final flattened DataFrame
# final_flattened_df.show(truncate=False)

# # Save to a file if needed
# # final_flattened_df.write.csv("flattened_data.csv", header=True)

# COMMAND ----------

# display(final_flattened_df)

# COMMAND ----------

# # Simulate Gold table structure with additional SCD columns
# gold_table_schema = StructType([
#     StructField("task_id", StringType(), True),
#     StructField("answer_id", StringType(), True),
#     StructField("product_id", StringType(), True),
#     StructField("price_net", DoubleType(), True),
#     StructField("sales_net", DoubleType(), True),
#     StructField("is_current", StringType(), True),
#     StructField("valid_from", TimestampType(), True),
#     StructField("valid_to", TimestampType(), True)
# ])

# # Create an empty Gold table DataFrame
# gold_table = spark.createDataFrame([], schema=gold_table_schema)

# COMMAND ----------

# def delta_upload(source_data, df_transformed, target_table, database, unique_key_column, join_cols, write_type):
#     mount_point = f"/mnt/bronze-sales-view"
#     output_path = f"{mount_point}/output/silver/{target_table}"

#     if not DeltaTable.isDeltaTable(spark, output_path) or write_type == "overwrite":
#         df_transformed.write.format("delta")\
#                   .mode("overwrite")\
#                   .option("path", output_path)\
#                   .saveAsTable(f"{database}.{target_table}")
    
#     else:
#         old_delta_table = DeltaTable.forPath(spark, output_path)
#         join_condition = " AND ".join([f"old_data.{col} = new_data.{col}" for col in join_cols])
#         old_delta_table.alias("old_data").merge(
#                                             df_transformed.alias("new_data"), join_condition
#                                             )\
#                                             .whenMatchedUpdateAll() \
#                                             .whenNotMatchedInsertAll()\
#                                             .execute()

# delta_upload(source_data, df_transformed, target_table, database, unique_key_column, join_cols, write_type)

# COMMAND ----------

# # Add a load timestamp to the incoming data
# incoming_data = flattened_data.withColumn("load_time", current_timestamp())

# # Step 1: Identify matches between Gold table and incoming data
# matched_data = gold_table.join(
#     incoming_data,
#     on="task_id",
#     how="outer"
# ).select(
#     gold_table["*"],
#     incoming_data["answer_id"].alias("new_answer_id"),
#     incoming_data["product_id"].alias("new_product_id"),
#     incoming_data["price_net"].alias("new_price_net"),
#     incoming_data["sales_net"].alias("new_sales_net")
# )

# # Step 2: Close outdated records
# to_close = matched_data.filter(
#     (col("is_current") == "1") & (col("answer_id") != col("new_answer_id"))
# ).withColumn(
#     "is_current", lit("0")
# ).withColumn(
#     "valid_to", current_timestamp()
# )

# # Step 3: Insert new records
# new_records = incoming_data.join(
#     gold_table.filter(col("is_current") == "1"),
#     on="task_id",
#     how="leftanti"
# ).select(
#     col("task_id"),
#     col("answer_id"),
#     col("product_id"),
#     col("price_net"),
#     col("sales_net"),
#     lit("1").alias("is_current"),
#     current_timestamp().alias("valid_from"),
#     lit(None).cast(TimestampType()).alias("valid_to")
# )

# # Combine updates with existing Gold table
# gold_table = gold_table.union(to_close).union(new_records)


# COMMAND ----------

# # Save to a persistent storage (e.g., Delta Lake, Parquet, or Hive)
# gold_table.write.format("parquet").mode("overwrite").save("gold_table.parquet")

# # Query current records
# current_records = gold_table.filter(col("is_current") == "1")
# current_records.show()
