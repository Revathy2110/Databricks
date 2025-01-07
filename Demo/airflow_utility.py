# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType,TimestampType, DoubleType, BooleanType
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
import re

# COMMAND ----------

# MAGIC %md
# MAGIC **Schema Defining**

# COMMAND ----------

schema_customer = StructType([
    StructField("Customer Id", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("Email Id", StringType(), True),
    StructField("address", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("Joining Date", StringType(), True),
    StructField("registered", StringType(), True),
    StructField("order ID", StringType(), True),
    StructField("orders", IntegerType(), True),
    StructField("spent", DoubleType(), True),
    StructField("job", StringType(), True),
    StructField("hobbies", StringType(), True),
    StructField("is_married", StringType(), True)
])

# COMMAND ----------


schema_product = StructType([
    StructField("product ID", StringType(), True),
    StructField("product name", StringType(), True),
    StructField("product code", StringType(), True),
    StructField("description", StringType(), True),
    StructField("category_id", IntegerType(), True),
    StructField("price", FloatType(), True),
    StructField("stock_quantity", IntegerType(), True),
    StructField("supplier_id", IntegerType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("image_url", StringType(), True),
    StructField("weight", StringType(), True),
    StructField("expiry_date", StringType(), True),
    StructField("is_active", IntegerType(), True),
    StructField("tax_rate", FloatType(), True),
    StructField("store_id", StringType(), True),
])



# COMMAND ----------


schema_store = StructType([
    StructField("store ID", StringType(), True),
    StructField("store name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("manager_name", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("email_address", StringType(), True),
    StructField("opening_date", IntegerType(), True),
    StructField("store_type", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True)
])



# COMMAND ----------


schema_sales = StructType([
    StructField("OrderDate", TimestampType(), True),
    StructField("Category", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("CustomerID", StringType(), True),
    StructField("OrderID", StringType(), True),
    StructField("PostalCode", IntegerType(), True),
    StructField("Product Id", StringType(), True),
    StructField("Profit", IntegerType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("Region", StringType(), True),
    StructField("Sales", IntegerType(), True),
    StructField("Segment", StringType(), True),
    StructField("ShipDate", TimestampType(), True),
    StructField("ShipMode", StringType(), True),
    StructField("State", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True)
])



# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists bronze;
# MAGIC create database if not exists silver;
# MAGIC create database if not exists gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop schema silver cascade;
# MAGIC -- Drop schema gold cascade;
# MAGIC
