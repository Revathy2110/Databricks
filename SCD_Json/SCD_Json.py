# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

df_json = spark.read.format("json").option("multiline", "true").load("dbfs:/FileStore/shared_uploads/revathy.s@diggibyte.com/data_2-1.json")
df_json.printSchema()
display(df_json)

# COMMAND ----------

# MAGIC %md
# MAGIC **Task_id is the PK in silver table (semi flatten table)**

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

# COMMAND ----------

# MAGIC %md
# MAGIC **Answer_id is the PK in gold table (fully flatten table)**

# COMMAND ----------


answer_schema = schema_of_json(
    '[{"answer_id": "string", "product_id": "string", "product_code": "string", "price_net": "double", "sales_net": "double"}]'
)

flattened_df = (
    flatten_df.select(
        col("task_id"),
        array(col("questions")).alias("questions_array")
    )
    .select(
        col("task_id"),
        explode(col("questions_array")).alias("question"),
    )
    .select(
        col("task_id"),
        col("question.type").alias("question_type"),
        col("question.question_id").alias("question_id"),
        concat_ws(",", col("question.answer")).alias("answer_string")
    )
    .select(
        col("task_id"),
        col("question_id"),
        from_json(col("answer_string"), answer_schema).alias("answer_array"),
    )
    .select(
        col("task_id"),
        col("question_id"),
        explode(col("answer_array")).alias("answer"),
    )
    .select(
        col("task_id"),
        col("question_id"),
        col("answer.answer_id"),
        col("answer.product_id"),
        col("answer.product_code"),
        col("answer.price_net"),
        col("answer.sales_net"),
    )
)

display(flattened_df)

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

# MAGIC %md
# MAGIC **when there is change in record value answer_id also changes
# MAGIC   In SCD we need to handle such case without having old and new value leading to wrong sellout**

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
