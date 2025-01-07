# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

df_json = spark.read.format("json").option("multiline", "true").load("dbfs:/FileStore/shared_uploads/revathy.s@diggibyte.com/data_2-1.json")
df_json.printSchema()
display(df_json)

# COMMAND ----------

silver_df = df_json.select(explode(col("data")).alias("data")).select("data.*")

silver_df = silver_df.dropDuplicates().filter(col("task_id").isNotNull())

display(silver_df)

silver_table = silver_df.write.format("delta").mode("overwrite").saveAsTable("silver_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_table

# COMMAND ----------


flatten_df = spark.table("silver_table")
display(flatten_df)
flatten_df.printSchema()

# COMMAND ----------

questions_df = flatten_df.select(
    col("date_time"),
    col("key_in_date"),
    col("lastmodified_date"),
    col("task_id"),
    col("store-id"),
    col("member_login_id"),
    col("check_in"),
    col("check_out"),
    col("user_location"),
    col("distance"),
    col("questions.type").alias("question_type"),
    col("questions.question").alias("question"),
    col("questions.question_id").alias("question_id"),
    col("questions.answer").alias("answer")  
)

display(questions_df)



# COMMAND ----------

answers_df = questions_df.withColumn("answer", explode(col("answer"))) 
display(answers_df)

# COMMAND ----------


from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

answer_schema = StructType([
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

final_df = answers_df.select(
    col("date_time"),
    col("key_in_date"),
    col("lastmodified_date"),
    col("task_id"),
    col("store-id"),
    col("member_login_id"),
    col("check_in"),
    col("check_out"),
    col("user_location"),
    col("distance"),
    col("question_type"),
    col("question"),
    col("question_id"),
    col("answer.product_id").alias("product_id"),
    col("answer.product_code").alias("product_code"),
    col("answer.price_gross").alias("price_gross"),
    col("answer.price_net").alias("price_net"),
    col("answer.price_discount").alias("price_discount"),
    col("answer.price_incentive").alias("price_incentive"),
    col("answer.sales_unit").alias("sales_unit"),
    col("answer.sales_gross").alias("sales_gross"),
    col("answer.sales_net").alias("sales_net"),
    col("answer.promotion").alias("promotion"),
    col("answer.answer_id").alias("answer_id")
)

display(final_df)

# COMMAND ----------

gold_df = flattened_df.dropDuplicates().filter(col("answer_id").isNotNull())

display(gold_df)

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS gold_table")

gold_df.write.format("delta").mode("overwrite").saveAsTable("gold_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_table

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_date, lit

delta_table = DeltaTable.forName(spark, "hive_metastore.default.gold_table")

new_data = flattened_df.withColumn("effective_date", current_date()) \
                       .withColumn("is_active", lit(True)) \
                       .withColumn("expiry_date", lit(None).cast("date"))

delta_table.alias("existing").merge(new_data.alias("incoming"),"existing.answer_id = incoming.answer_id"
).whenMatchedUpdate(
    condition="existing.is_active = True",
    set={"is_active": lit(False), "expiry_date": current_date()}
).whenNotMatchedInsertAll().execute()
