# Databricks notebook source
# MAGIC %md
# MAGIC - Calculate Total Sales and Profit by Region
# MAGIC - Identify Top 5 Stores with total Sales
# MAGIC - Analyze Sales Performance by Product Category
# MAGIC - Determine Shipping Performance
# MAGIC - Segment-Wise Sales Distribution
# MAGIC - Geographic Sales Analysis
# MAGIC - Store Type Performance
# MAGIC - Profit Margin Analysis
# MAGIC - Customer Purchase Frequency
# MAGIC - Store Location Insights
# MAGIC - Sales Analysis by Region and Category
# MAGIC - Customer Segmentation by Age and Gender
# MAGIC - Delivery Time Analysis
# MAGIC - Store Performance Analysis
# MAGIC - Tax Revenue Calculation
# MAGIC - Customer Spending Behavior
# MAGIC - Product Popularity
# MAGIC - Store Geographical Distribution
# MAGIC - Revenue by Product Category

# COMMAND ----------

df = spark.read.table("gold.StoreProductSalesAnalysis")

# COMMAND ----------

# MAGIC %md
# MAGIC Calculate Total Sales and Profit by Region

# COMMAND ----------

from pyspark.sql.functions import *

sales_profit_region = df.groupBy("region") \
    .agg(
        sum("sales").alias("total_sales"),
        sum("profit").alias("total_profit")
    )

sales_profit_region.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Identify Top 5 Stores with Maximum Sales

# COMMAND ----------


top_stores = df.groupBy("store_id", "store_name") \
    .agg(sum("sales").alias("total_sales")) \
    .orderBy(desc("total_sales")) \
    .limit(5)

top_stores.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Analyze Sales Performance by Product Category

# COMMAND ----------


sales_category = df.groupBy("category") \
    .agg(
        sum("sales").alias("total_sales"),
        sum("profit").alias("total_profit")
    )

sales_category.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Determine Shipping Performance

# COMMAND ----------

shipping_performance = df.withColumn("delivery_time", datediff("ship_date", "order_date")) \
    .groupBy("ship_mode").agg(
    round(avg("delivery_time"), 2).alias("avg_delivery_time")
)
shipping_performance.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Segment-Wise Sales Distribution

# COMMAND ----------

# Calculate total sales by customer segment
segment_sales = df.groupBy("segment") \
    .agg(sum("sales").alias("total_sales"))

# segment_sales.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Geographic Sales Analysis

# COMMAND ----------

# Calculate total sales and profit by city
geo_sales = df.groupBy("city") \
    .agg(
        sum("sales").alias("total_sales"),
        sum("profit").alias("total_profit")
    )

geo_sales.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Store Type Performance

# COMMAND ----------

store_type_performance = df.groupBy("store_type") \
    .agg(
        sum("sales").alias("total_sales"),
        sum("profit").alias("total_profit")
    )

store_type_performance.show()


# COMMAND ----------

# MAGIC %md
# MAGIC  Profit Margin Analysis

# COMMAND ----------

profit_margin = df.withColumn("profit_margin", round((col("profit") / col("sales")) * 100,2)) \
    .select("store_id", "store_name", "sales", "profit", "profit_margin")

profit_margin.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Customer Purchase Frequency

# COMMAND ----------

customer_orders = df.groupBy("customer_id") \
    .agg(count("order_id").alias("order_count"))

customer_orders.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Store Location Insights

# COMMAND ----------


store_locations = df.select("store_id", "store_name", "location", "latitude", "longitude") \
    .distinct()

store_locations.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Sales Analysis by Region and Category

# COMMAND ----------


sales_analysis = df.groupBy("region", "category") \
    .agg(sum("sales").alias("total_sales"), sum("profit").alias("total_profit"))

sales_analysis.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Customer Segmentation by Age and Gender

# COMMAND ----------


df = df.withColumn("age_group", when(col("age") < 18, "Under 18")
                   .when((col("age") >= 18) & (col("age") < 35), "18-34")
                   .when((col("age") >= 35) & (col("age") < 50), "35-49")
                   .otherwise("50+"))

customer_segmentation = df.groupBy("gender", "age_group").count()

customer_segmentation.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Delivery Time Analysis

# COMMAND ----------

df = df.withColumn("delivery_time", round(datediff(col("ship_date"), col("order_date")),1))

delivery_analysis = df.groupBy("ship_mode") \
    .agg(avg("delivery_time").alias("avg_delivery_time"))

delivery_analysis.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Store Performance Analysis

# COMMAND ----------

store_performance = df.groupBy("store_id", "store_name", "region") \
    .agg(sum("sales").alias("total_sales"), sum("profit").alias("total_profit"))

store_performance.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Tax Revenue Calculation

# COMMAND ----------

df = df.withColumn("tax_revenue", (col("sales") * col("tax_rate")) / 100)

total_tax = df.agg(sum("tax_revenue").alias("total_tax_revenue"))

total_tax.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Customer Spending Behavior

# COMMAND ----------

customer_spending = df.groupBy("customer_id") \
    .agg(sum("sales").alias("total_spending")) \
    .withColumn("spending_category", when(col("total_spending") < 1000, "Low")
                .when((col("total_spending") >= 1000) & (col("total_spending") < 5000), "Medium")
                .otherwise("High"))

customer_spending.show()


# COMMAND ----------

# MAGIC %md
# MAGIC  Product Popularity

# COMMAND ----------

top_products = df.groupBy("product_id", "product_name") \
    .agg(sum("sales").alias("total_sales")) \
    .orderBy(col("total_sales").desc()) \
    .limit(5)

top_products.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Store Geographical Distribution

# COMMAND ----------

store_distribution = df.groupBy("region") \
    .count() \
    .withColumnRenamed("count", "store_count")

store_distribution.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Revenue by Product Category

# COMMAND ----------

category_revenue = df.groupBy("category") \
    .agg(sum("sales").alias("total_revenue"))

category_revenue.show()

