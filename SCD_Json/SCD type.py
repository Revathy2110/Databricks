# Databricks notebook source
# MAGIC %run ./Json_schema

# COMMAND ----------

# DBTITLE 1,Reading the data
df_json = spark.read.format("json").option("multiline", "true").schema(schema).load("dbfs:/FileStore/shared_uploads/revathy.s@diggibyte.com/data_2-1.json")

# COMMAND ----------

df_flat = df_json.select(
    col("country"),
    explode(col("data")).alias("data")
).select(
    col("country"),
    col("data.date_time"),
    col("data.Key_in_date"),
    col("data.lastmodified_date"),
    col("data.task_id"),
    col("data.store-id").alias("store_id"),
    col("data.member_login_id"),
    col("data.check_in"),
    col("data.check_out"),
    col("data.user_location").alias("user_location"),
    col("data.distance"),
    explode(col("data.questions")).alias("questions")
)

display(df_flat)

# COMMAND ----------

# DBTITLE 1,Flattening the Data
# semi flatten json data
semi_flatten_json = df_flat.select(
    col("country"),
    col("date_time"),
    col("Key_in_date"),
    col("lastmodified_date"),
    col("task_id"),
    col("store_id"),
    col("member_login_id"),
    col("check_in"),
    col("check_out"),
    col("user_location"),
    col("distance"),
    col("questions.type"),
    col("questions.question"),
    col("questions.question_id"),
    explode(col("questions.answer")).alias("answers")
)
display(semi_flatten_json)

# COMMAND ----------

silver_df = semi_flatten_json.dropDuplicates().filter(col("task_id").isNotNull())


# COMMAND ----------

silver_table = silver_df.write.format("parquet").option("path", "shared_uploads/revathy.s@diggibyte.com/scd/silver").saveAsTable("Silver_data")

# COMMAND ----------

gold_df = spark.read.table("Silver_data")
display(gold_df)

# COMMAND ----------

final_flattened_df = gold_df.select(
    col("country"),
    col("date_time"),
    col("Key_in_date"),
    col("lastmodified_date"),
    col("task_id"),
    col("store_id"),
    col("member_login_id"),
    col("check_in"),
    col("check_out"),
    col("user_location"),
    col("distance"),
    col("type"),
    col("question"),
    col("question_id"),
    col("answers.product_id"),
    col("answers.product_code"),
    col("answers.price_gross"),
    col("answers.price_net"),
    col("answers.price_discount"),
    col("answers.price_incentive"),
    col("answers.sales_unit"),
    col("answers.sales_gross"),
    col("answers.sales_net"),
    col("answers.promotion"),
    col("answers.answer_id"))

display(final_flattened_df) 

# COMMAND ----------

final_df = final_flattened_df.withColumn("is_current", lit(True).cast(StringType())) \
    .withColumn("valid_from", current_timestamp()) \
    .withColumn("valid_to", lit(None).cast(StringType())) \
    .dropDuplicates() \
    .filter(col("answer_id").isNotNull())
display(final_df)

# COMMAND ----------

# Define current timestamp for SCD tracking
current_time = current_timestamp()
output_path = "/dbfs/shared_uploads/revathy.s@diggibyte.com/scd/gold"

if not DeltaTable.isDeltaTable(spark, output_path):
        final_df.write.format("delta")\
                  .mode("overwrite")\
                  .option("path", output_path)\
                  .saveAsTable(f"gold.gold_data")


# Merge to implement SCD Type 2
else:
    old_delta_table = DeltaTable.forPath(spark, output_path)
    old_delta_table.alias("target").merge(
    final_df.alias("source"),
    "target.answer_id = source.answer_id"
).whenMatchedUpdate(
    condition="target.is_current = true",
    set={
        "current": lit(False),
        "valid_to": current_time
    }
).whenNotMatchedInsertAll()\
.execute()


# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit

# Define current timestamp for SCD tracking
current_time = current_timestamp()
output_path = "/dbfs/shared_uploads/revathy.s@diggibyte.com/scd/gold"

# Check if Spark session is active
try:
    spark.version
    print("Spark session is active.")
except:
    raise Exception("Spark session is not active. Please initialize the Spark session.")

# Check if final_df is not empty
if final_df is None or final_df.rdd.isEmpty():
    raise ValueError("final_df is empty or not defined.")

# Option to force recreate the Delta table (use with caution)
force_recreate = True

if force_recreate:
    print("Forcing the recreation of the Delta table...")
    final_df.write.format("delta")\
                  .mode("overwrite")\
                  .option("overwriteSchema", "true")\
                  .option("path", output_path)\
                  .saveAsTable("gold.gold_data")
else:
    print("Checking if Delta table exists...")
    if not DeltaTable.isDeltaTable(spark, output_path):
        print("Creating new Delta table...")
        final_df.write.format("delta")\
                      .mode("overwrite")\
                      .option("path", output_path)\
                      .saveAsTable("gold.gold_data")
    else:
        print("Merging with existing Delta table...")
        old_delta_table = DeltaTable.forPath(spark, output_path)
        old_delta_table.alias("target").merge(
            final_df.alias("source"),
            "target.answer_id = source.answer_id"
        ).whenMatchedUpdate(
            condition="target.is_current == true",
            set={
                "is_current": lit(False),
                "valid_to": current_time
            }
        ).whenNotMatchedInsertAll()\
        .execute()

print("Operation completed.")

