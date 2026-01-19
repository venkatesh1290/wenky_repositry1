# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Person_1 (
# MAGIC     PersonId  INT,
# MAGIC     FirstName STRING,
# MAGIC     LastName  STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO Person_1 VALUES
# MAGIC (1, 'John', 'Doe'),
# MAGIC (2, 'Jane', 'Smith'),
# MAGIC (3, 'Mike', 'Brown');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Address_1 (
# MAGIC     AddressId INT,
# MAGIC     PersonId  INT,
# MAGIC     City      STRING,
# MAGIC     State     STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO Address_1 VALUES
# MAGIC (101, 1, 'New York', 'NY'),
# MAGIC (102, 2, 'Los Angeles', 'CA');
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from datetime import datetime,date

df1 = spark.table("Person_1")
df2 = spark.table("Address_1")
df = df1.join(df2,df1.PersonId == df2.PersonId,how = "left")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## TWO

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Employee (
# MAGIC     Id INT,
# MAGIC     Name STRING,
# MAGIC     Dept STRING,
# MAGIC     Salary INT
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO Employee VALUES
# MAGIC (1, 'John',  'IT',  5000),
# MAGIC (2, 'Jane',  'IT',  7000),
# MAGIC (3, 'Mike',  'IT',  7000),
# MAGIC (4, 'Sara',  'HR',  4000),
# MAGIC (5, 'Paul',  'HR',  6000),
# MAGIC (6, 'Rita',  'HR',  8000);
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from datetime import datetime,date
from pyspark.sql.window import Window

df = spark.table("Employee")
df1 = Window.partitionBy("Dept").orderBy(col("Salary").desc())
df = df.withColumn('RowNum',dense_rank().over(df1)).filter(col("RowNum") == 2).select("Id","Name","Dept","Salary","RowNum")
#df = df.select("Id","Name","Dept","Salary","RowNum")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Three

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE Scores (
# MAGIC     Id INT,
# MAGIC     Score DOUBLE
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO Scores VALUES
# MAGIC (1, 3.50),
# MAGIC (2, 3.65),
# MAGIC (3, 4.00),
# MAGIC (4, 3.85),
# MAGIC (5, 4.00),
# MAGIC (6, 3.65);
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from datetime import datetime,date
from pyspark.sql.window import Window

df = spark.table("Scores")
window_spec = Window.orderBy(col("Score").desc())
df = df.withColumn("Rank",dense_rank().over(window_spec))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Missing Letters

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window 
from pyspark.sql import SparkSession 

spark = SparkSession.builder.getOrCreate()

data = [(1,), (2,), (3,), (5,), (6,), (9,), (10,)]

df = spark.createDataFrame(data, ["id"])
df.show()

# COMMAND ----------

min_value = df.agg(min("id")).collect()[0][0]
max_value = df.agg(max("id")).collect()[0][0]
display(min_value)
display(max_value)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window 
from pyspark.sql import SparkSession 

spark = SparkSession.builder.getOrCreate()
df1 = spark.range(min_value,max_value+1).toDF("id")
df2 = df1.subtract(df)
display(df2)