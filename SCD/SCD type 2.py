# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE scd2Demo (
# MAGIC                         pk1 INT,
# MAGIC                         pk2 STRING,
# MAGIC                         dim1 INT,
# MAGIC                         dim2 INT,
# MAGIC                         dim3 INT,
# MAGIC                         dim4 INT,
# MAGIC                         active_status STRING,
# MAGIC                         start_date TIMESTAMP,
# MAGIC                         end_date TIMESTAMP
# MAGIC
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/FileStore/tables/scd2Demo';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE scd2Demo (
# MAGIC                         pk1 INT,
# MAGIC                         pk2 STRING,
# MAGIC                         dim1 INT,
# MAGIC                         dim2 INT,
# MAGIC                         dim3 INT,
# MAGIC                         dim4 INT,
# MAGIC                         active_status STRING,
# MAGIC                         start_date TIMESTAMP,
# MAGIC                         end_date TIMESTAMP
# MAGIC
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/FileStore/tables/scd2Demo';

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into scd2Demo values(111, 'Unit1',200,500,800,400,'Y', current_timestamp(),'9999-12-31'),
# MAGIC                             (222, 'Unit2',900,Null,700,100,'Y', current_timestamp(),'9999-12-31'),
# MAGIC                             (333, 'Unit3',300,900,250,650,'Y', current_timestamp(),'9999-12-31');

# COMMAND ----------

from delta import *
targetTable = DeltaTable.forPath(spark, 'dbfs:/FileStore/tables/scd2Demo')
targetDF = targetTable.toDF()
display(targetDF)

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

schema = StructType([StructField("pk1",IntegerType(),True),\
                      StructField("pk2",StringType(),True),\
                        StructField("dim1",IntegerType(),True),\
                        StructField("dim2",IntegerType(),True),\
                        StructField("dim3",IntegerType(),True),\
                        StructField("dim4",IntegerType(),True)])

# COMMAND ----------

data = [(111,'Unit1',200,500,800,400),
        (222,'Unit2',800,1300,800,500),
        (444,'Unit4',100,None,700,300)]

sourceDF = spark.createDataFrame(data,schema)
display(sourceDF)

# COMMAND ----------

joinDF = sourceDF.join(targetDF,
                        (sourceDF.pk1 == targetDF.pk1) & \
                        (sourceDF.pk2 == targetDF.pk2) & \
                        (targetDF.active_status == 'Y'),
                        "leftouter")\
                                .select(sourceDF["*"],
                                        targetDF.pk1.alias("target_pk1"),\
                                        targetDF.pk2.alias("target_pk2"),\
                                        targetDF.dim1.alias("target_dim1"),\
                                        targetDF.dim2.alias("target_dim2"),\
                                        targetDF.dim3.alias("target_dim3"),\
                                        targetDF.dim4.alias("target_dim4"))

display(joinDF)

# COMMAND ----------

filterDF = joinDF.filter(xxhash64(joinDF.dim1, joinDF.dim2, joinDF.dim3, joinDF.dim4)
                         != xxhash64(joinDF['target_dim1'], joinDF['target_dim2'], joinDF['target_dim3'], joinDF['target_dim4']))

display(filterDF)

# COMMAND ----------

mergeDF = filterDF.withColumn("MERGEKEY", concat(filterDF.pk1,filterDF.pk2))
display(mergeDF)

# COMMAND ----------

dummyDF = filterDF.filter("target_pk1 is not null").withColumn("MERGEKEY",lit(None))
display(dummyDF)


# COMMAND ----------

scdDF = mergeDF.union(dummyDF)
display(scdDF)

# COMMAND ----------

targetTable.alias("target").merge(
    source=scdDF.alias("source"),
    condition="concat(target.pk1, target.pk2) = source.MERGEKEY and target.active_status = 'Y'"
).whenMatchedUpdate(
    set={
        "active_status": "'N'",
        "end_date": "current_date()"
    }
).whenNotMatchedInsert(
    values={
        "pk1": "source.pk1",
        "pk2": "source.pk2",
        "dim1": "source.dim1",
        "dim2": "source.dim2",
        "dim3": "source.dim3",
        "dim4": "source.dim4",
        "active_status": "'Y'",
        "start_date": "current_date()",
        "end_date": "to_date('9999-12-31', 'yyyy-MM-dd')"
    }

).execute()


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from scd2Demo
