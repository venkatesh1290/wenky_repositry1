# Databricks notebook source
df = spark.read.option("header", "true").option("inferschema", "true").csv("/Volumes/pyspark_catalogue/venkat/venkat_volume/BigMart Sales.csv")
display(df)

# COMMAND ----------

df.sparkSession.conf.get("spark.sql.shuffle.partitions")


# COMMAND ----------

df.explain(True)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Find Partitions

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Change the partition
# MAGIC

# COMMAND ----------

spark.conf.set("spark.sql.files.maxPartitionBytes",131072)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Decreased the time to fetch the data

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Changing again the partition size

# COMMAND ----------

spark.conf.set("spark.sql.files.maxPartitionBytes",134217728)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Repartitioning

# COMMAND ----------

df = df.repartition(7)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get partition Info

# COMMAND ----------

from pyspark.sql.functions import *
df2 = df.withColumn("Partition_Information",spark_partition_id())
display(df2)

# COMMAND ----------

display(df2)

# COMMAND ----------

df.write.format("parquet")\
    .mode("append")\
    .option("path","/Volumes/pyspark_catalogue/venkat/venkat_volume/BigMart Sales.parquet/")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## New data reading

# COMMAND ----------

df1 = spark.read.option("header", "true").option("inferschema", "true").parquet("/Volumes/pyspark_catalogue/venkat/venkat_volume/BigMart Sales.parquet")
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save file with Partition By

# COMMAND ----------

df2.write.format("parquet")\
    .mode("append")\
        .partitionBy("Partition_Information")\
            .option("path","/Volumes/pyspark_catalogue/venkat/venkat_volume/BigMart Sales One.parquet")\
                .save()

# COMMAND ----------

df3 = spark.read.option("header","true").option("inferschema","true").parquet("/Volumes/pyspark_catalogue/venkat/venkat_volume/BigMart Sales One.parquet")
df3 = df3.filter(col("Outlet_Location_Type") == "Tier 1")
display(df3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Broadcast Join

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions 

spark = SparkSession.builder.getOrCreate()

employees_data = [
    (1, "Alice", 10),
    (2, "Bob", 20),
    (3, "Carol", 10),
    (4, "David", 30),
    (5, "Eva", 20)
]

employees_df = spark.createDataFrame(
    employees_data,
    ["emp_id", "emp_name", "dept_id"]
)

employees_df.show()


# COMMAND ----------

departments_data = [
    (10, "HR"),
    (20, "IT"),
    (30, "Finance")
]

departments_df = spark.createDataFrame(
    departments_data,
    ["dept_id", "dept_name"]
)

departments_df.show()


# COMMAND ----------

from pyspark.sql.functions import *
df_broad = employees_df.join(broadcast(departments_df),employees_df.dept_id == departments_df.dept_id,how = "inner")
display(df_broad)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Views

# COMMAND ----------

df_broad.createOrReplaceTempView("First_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from First_view

# COMMAND ----------

# MAGIC %md
# MAGIC ## AQE

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled","false")


# COMMAND ----------

df = spark.read.option("header", "true").option("inferschema", "true").csv("/Volumes/pyspark_catalogue/venkat/venkat_volume/BigMart Sales.csv")
display(df)

# COMMAND ----------

from pyspark.sql.functions import *
df1 = df.groupBy(col("Item_Fat_Content")).count().orderBy(col("count"))
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## salting technique

# COMMAND ----------

from pyspark.sql.functions import * 
from pyspark.sql.types import * 
from pyspark.sql import SparkSession 

spark = SparkSession.builder.getOrCreate()
orders_data = [
    (1, 101, 500),
    (1, 102, 700),
    (1, 103, 200),
    (1, 104, 900),
    (2, 105, 300),
    (3, 106, 400)
]

orders_df = spark.createDataFrame(
    orders_data,
    ["customer_id", "order_id", "amount"]
)

orders_df.show()


# COMMAND ----------

orders_data = [
    (1, 101, 500),
    (1, 102, 700),
    (1, 103, 200),
    (1, 104, 900),
    (2, 105, 300),
    (3, 106, 400)
]

orders_df = spark.createDataFrame(
    orders_data,
    ["customer_id", "order_id", "amount"]
)

orders_df.show()


# COMMAND ----------

df = orders_df.withColumn("random_column",floor(rand()*3))
df = df.withColumn("concat_column",concat(col("customer_id"),lit("-"),col("random_column")))
df = df.groupBy(col("concat_column")).agg(count("*"))
display(df)