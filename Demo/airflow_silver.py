# Databricks notebook source
# MAGIC %run "./airflow_utility"

# COMMAND ----------

source_data = dbutils.widgets.text("source_data", "", "source_data")
target_table = dbutils.widgets.text("target_table", "", "target_table")
unique_key_column = dbutils.widgets.text("unique_key_column", "", "unique_key_column")
database = dbutils.widgets.text("database","", "database")
join_cols = dbutils.widgets.text("join_cols", "", "join_cols")
schedule = dbutils.widgets.text("schedule", "", "schedule")
write_type = dbutils.widgets.text("write_type", "", "write_type")

# COMMAND ----------

target_table = dbutils.widgets.get("target_table")
unique_key_column = dbutils.widgets.get("unique_key_column")
source_data = dbutils.widgets.get("source_data")
database = dbutils.widgets.get("database") 
join_cols = dbutils.widgets.get("join_cols")
schedule = dbutils.widgets.get("schedule")
write_type = dbutils.widgets.get("write_type")

# COMMAND ----------

def read_data(source_data):
    mount_point = "/mnt/bronze-sales-view"
    df = spark.read.format("csv").options(header ="true", inferSchema="true").load(f"{mount_point}/{source_data}")
    return df

# COMMAND ----------

def to_snake_case(col_name):
    return col_name.lower().replace(" ", "_")
def get_transformed_dataframe(source_data):
    df = read_data(source_data)
    if source_data == "customer":
        df_transformed_customer = df.toDF(*[to_snake_case(col) for col in df.columns])\
            .withColumn("is_married", col("is_married").cast(BooleanType()))\
            .withColumnRenamed("employee_id", "customer_id")\
            .withColumn("first_name", split(col("name"), " ")[0])\
            .withColumn("last_name", split(col("name"), " ")[1])\
            .withColumn("domain", regexp_extract("email_id", r'([a-zA-Z]+)\.com$', 1))\
            .withColumn("gender", when(col("gender") == "male", "M").when(col("gender") == "female", "F")
            .otherwise("None"))\
            .withColumn("date", to_date(split(col("joining_date"), " ")[0], "dd-MM-yyyy")) \
            .withColumn("time", split(col("joining_date"), " ")[1])\
            .withColumn("registered", to_date(col("registered"),"dd-MM-yyyy"))\
            .withColumn("expenditure_status", when(col("spent") < 200, "MINIMUM").otherwise("MAXIMUM"))\
            .withColumn("load_date", lit(schedule))\
            .filter("customer_id IS NOT NULL").drop("name", "order_id","joining_date").distinct()
        return df_transformed_customer 
     
    elif source_data == "product":
        df_transformed_product = df.toDF(*[to_snake_case(col) for col in df.columns]) \
                .withColumn("sub_category", when(col("category_id") == 1, "phone")
                .when(col("category_id") == 2, "laptop")
                .when(col("category_id") == 3, "playstation")
                .when(col("category_id") == 4, "e-device")
                .otherwise("None"))\
                .withColumn("created_at", to_date(col("created_at"),"dd-MM-yyyy"))\
                .withColumn("updated_at", to_date(col("updated_at"),"dd-MM-yyyy"))\
                .withColumn("load_date", lit(schedule))\
                .filter("product_id IS NOT NULL").drop("expiry_date").distinct()
        return df_transformed_product 
     
    elif source_data == "sales":
        df_transformed_sales = df.toDF(*[to_snake_case(col) for col in df.columns])\
                        .withColumn("order_date", to_date(col("order_date").cast(TimestampType()),"yyyy-MM-dd"))\
                        .withColumn("ship_date", to_date(col("ship_date").cast(TimestampType()),"yyyy-MM-dd"))\
                        .withColumn("load_date", lit(schedule))\
                        .filter("order_id IS NOT NULL").distinct()
        return df_transformed_sales 
    
    elif source_data == "store":
        df_transformed_store = (df.toDF(*[to_snake_case(col) for col in df.columns]) \
                        .withColumn("store_category",regexp_extract(col("email_address"), r'([a-zA-Z]+)\.com$', 1))\
                        .withColumn("created_at",to_date(col("created_at"), "dd-MM-yyyy"))\
                        .withColumn("updated_at",to_date(col("updated_at"), "dd-MM-yyyy")))\
                        .withColumn("load_date", lit(schedule))\
                        .filter("store_id IS NOT NULL").distinct()
        return df_transformed_store
    else:
        raise ValueError(f"Unknown source_data: {source_data}")
 
df_transformed = get_transformed_dataframe(source_data)

# COMMAND ----------

# spark.sql(f"DROP TABLE IF EXISTS silver.customer")
# spark.sql(f"DROP TABLE IF EXISTS silver.product")

# COMMAND ----------

def delta_upload(source_data, df_transformed, target_table, database, unique_key_column, join_cols, write_type):
    mount_point = f"/mnt/bronze-sales-view"
    output_path = f"{mount_point}/output/silver/{target_table}"

    if not DeltaTable.isDeltaTable(spark, output_path) or write_type == "overwrite":
        df_transformed.write.format("delta")\
                  .mode("overwrite")\
                  .option("path", output_path)\
                  .saveAsTable(f"{database}.{target_table}")
    
    else:
        old_delta_table = DeltaTable.forPath(spark, output_path)
        join_condition = " AND ".join([f"old_data.{col} = new_data.{col}" for col in join_cols])
        old_delta_table.alias("old_data").merge(
                                            df_transformed.alias("new_data"), join_condition
                                            )\
                                            .whenMatchedUpdateAll() \
                                            .whenNotMatchedInsertAll()\
                                            .execute()

delta_upload(source_data, df_transformed, target_table, database, unique_key_column, join_cols, write_type)
