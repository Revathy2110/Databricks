# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("SCD").getOrCreate()

df= spark.read.format("json").options(multiline="true", inferSchema="true").load("dbfs:/FileStore/shared_uploads/revathy.s@diggibyte.com/data_2.json")

# COMMAND ----------

df.show(truncate = False)

# COMMAND ----------


silver_df = df.select(explode(col("data")).alias("record")).select("record.*")

display(silver_df)

silver_table = silver_df.write.mode("overwrite").saveAsTable("silver_table")

# COMMAND ----------

exploded_df = df.select(explode(col("data")).alias("data"))

flattened_df = exploded_df.select(
    col("data.date_time").alias("date_time"),
    col("data.Key_in_date").alias("key_in_date"),
    col("data.lastmodified_date").alias("lastmodified_date"),
    col("data.task_id").alias("task_id"),  # Primary Key
    col("data.store-id").alias("store_id"),
    col("data.member_login_id").alias("member_login_id"),
    col("data.check_in").alias("check_in"),
    col("data.check_out").alias("check_out"),
    col("data.user_location").alias("user_location"),
    col("data.distance").alias("distance")
)

window = Window.partitionBy("task_id").orderBy(col("lastmodified_date").desc())

deduplicated_df = flattened_df.withColumn(
    "row_num", row_number().over(window)
).filter(col("row_num") == 1).drop("row_num")

df_silver = deduplicated_df.write.format("json").mode("overwrite").save("dbfs:/FileStore/silver/")


# COMMAND ----------

display(df_silver)

# COMMAND ----------

df_silver = spark.read.format("json").option("multiline", "true").option("inferSchema", "true").load("dbfs:/FileStore/silver/")

# COMMAND ----------

# MAGIC %md
# MAGIC **Flatten json file**

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

#Flatten array of structs and structs
def flatten(df_silver):
   #compute Complex Fields (Lists and Structs) in Schema   
   complex_fields = dict([(field.name, field.dataType)
                             for field in df_silver.schema.fields
                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
   while len(complex_fields)!=0:
      col_name=list(complex_fields.keys())[0]
      print ("Processing :"+col_name+" Type : "+str(type(complex_fields[col_name])))
    
      #if StructType then convert all sub element to columns.
      #i.e. flatten structs
      if (type(complex_fields[col_name]) == StructType):
         expanded = [col(col_name+'.'+k).alias(col_name+'_'+k) for k in [n.name for n in  complex_fields[col_name]]]
         df_silver=df_silver.select("*", *expanded).drop(col_name)
    
      # if ArrayType then add the Array Elements as Rows using the explode function
      # i.e. explode Arrays
      elif (type(complex_fields[col_name]) == ArrayType):    
         df_silver=df_silver.withColumn(col_name,explode_outer(col_name))
    
      # recompute remaining Complex Fields in Schema       
      complex_fields = dict([(field.name, field.dataType)
                             for field in df_silver.schema.fields
                             if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
   return df_silver

# COMMAND ----------

flatten_df = flatten(df_silver)
display(flatten_df)

# COMMAND ----------

df_gold = flatten_df.write.format("delta").mode("overwrite").save("dbfs:/FileStore/gold_delta/")

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_date, lit

new_data = flatten_df.withColumn("effective_date", current_date()).withColumn("is_active", lit(True))

delta_table = DeltaTable.forPath(spark, "dbfs:/FileStore/gold_delta/")

# Perform the merge operation
delta_table.alias("existing").merge(
    new_data.alias("incoming"),
    "existing.answer_id = incoming.answer_id"
).whenMatchedUpdate(
    condition="existing.is_active = True AND existing.hash != incoming.hash",
    set={"is_active": lit(False), "expiry_date": current_date()}
).whenNotMatchedInsertAll().execute()

# COMMAND ----------

from pyspark.sql.functions import sha2, concat_ws

gold_with_hash = flatten_df.withColumn(
    "hash", sha2(concat_ws(",", col("product_id"), col("product_code"), col("price_net")), 256)
)

existing_data = delta_table.toDF()

updates_df = gold_with_hash.join(existing_data, "hash", "left_anti")

delta_table.alias("existing").merge(
    updates_df.alias("incoming"),
    "existing.answer_id = incoming.answer_id"
).whenMatchedUpdate(
    set={"is_active": False, "expiry_date": current_date()}
).whenNotMatchedInsertAll().execute()
