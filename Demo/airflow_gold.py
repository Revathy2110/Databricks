# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

mount_point = f"/mnt/bronze-sales-view"
df = spark.read.format("delta").options(header ="true", inferSchema="true").load(f"{mount_point}/output/silver/sales")

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df_concat = df.withColumn("concat", concat(col("city"), lit(" - "), col("category")))
display(df_concat)

# COMMAND ----------

# df_gold_customer = spark.read.table("silver.customer")
# df_gold_product = spark.read.table("silver.product")
# df_gold_store = spark.read.table("silver.store")
# df_gold_sales = spark.read.table("silver.sales")


# COMMAND ----------

# MAGIC %md
# MAGIC **Joining the dataframe**

# COMMAND ----------

df_product_store = df_gold_product.alias("product").join(
    df_gold_store.alias("store"),
    col("product.store_id") == col("store.store_id"),
    "leftouter"
).select(
    col("store.store_id"),          
    col("store.store_name"),
    col("store.location"),
    col("store.manager_name"),
    col("store.phone_number"),
    col("store.store_type"),
    col("product.product_id"),
    col("product.product_name"),
    col("product.product_code"),
    col("product.category_id"),
    col("product.sub_category"),
    col("product.price"),
    col("product.stock_quantity"),
    col("product.supplier_id"),
    col("product.created_at"),
    col("product.updated_at"),
    col("product.image_url"),
    col("product.weight"),
    col("product.tax_rate")
)



# COMMAND ----------

df_StoreProductSalesAnalysis = (df_product_store.alias("ps").join(
    df_gold_sales.alias("sales"),
    col("ps.product_id") == col("sales.product_id"),
    "leftouter").join(df_gold_customer.alias("customer"), "customer_id", "leftouter")
).select(
    col("sales.order_date"),
    col("sales.category"),
    col("sales.city"),
    col("sales.customer_id"),
    col("sales.order_id"),
    col("sales.profit"),
    col("sales.region"),
    col("sales.sales"),
    col("sales.segment"),
    col("sales.ship_date"),
    col("sales.ship_mode"),
    col("sales.latitude"),
    col("sales.longitude"),
    col("ps.*"),
    col("customer.first_name"),
    col("customer.last_name"),
    col("customer.email_id"),
    col("customer.address"),
    col("customer.gender"),
    col("customer.age"),
    col("customer.domain")

)

# COMMAND ----------

df_gold = df_StoreProductSalesAnalysis.write.format("delta")\
                  .mode("overwrite")\
                  .option("path", output_path)\
                  .saveAsTable(f"{database}.{target_table}")

# COMMAND ----------

# df_gold = df_StoreProductSalesAnalysis.write.format("delta").mode("overwrite").saveAsTable("gold.StoreProductSalesAnalysis")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.storeproductsalesanalysis;

# COMMAND ----------

df = spark.read.table("gold.StoreProductSalesAnalysis")
df.printSchema()
