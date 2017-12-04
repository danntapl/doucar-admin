# # Reading and Writing Data

# Copyright © 2010–2017 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.

# In this module we demonstrate how to read and write a variety of data formats
# into and out of Apache Spark.

# * Working with delimited text files
# * Working with files in Amazon S3
# * Working with Parquet files
# * Working with Hive tables
# * Working with pandas DataFrames


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("read").enableHiveSupport().getOrCreate()


# ## Working with delimited text files

# The rider data is actually a comma-delimited text file.  The `csv` method of
# the `DataFrameReader` class reads a delimited text file:
spark \
  .read \
  .csv("/duocar/raw/riders/", sep=",", header=True, inferSchema=True) \
  .show(5)

# This is actually a convenience function for the more general syntax:
riders = spark \
  .read \
  .format("csv") \
  .option("sep", ",") \
  .option("header", True) \
  .option("inferSchema", True) \
  .load("/duocar/raw/riders/")

# **Note:** If you use either method with `header` set to `True`, then Spark
# assumes that a header row occurs in *every* file in the data directory you
# load.

# Spark does its best to infer the schema from the column names and values:
riders.printSchema()

# You can manually specify the schema instead of inferring it from the header
# row and column values:
from pyspark.sql.types import *

# Specify the schema:
schema = StructType([
    StructField("id", StringType()),
    StructField("birth_date", DateType()),
    StructField("start_date", DateType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("sex", StringType()),
    StructField("ethnicity", StringType()),
    StructField("student", IntegerType()),
    StructField("home_block", StringType()),
    StructField("home_lat", DoubleType()),
    StructField("home_lon", DoubleType()),
    StructField("work_lat", DoubleType()),
    StructField("work_lon", DoubleType())
])

# Pass the schema to the `DataFrameReader`:
riders2 = spark \
  .read \
  .format("csv") \
  .option("sep", ",") \
  .option("header", True) \
  .schema(schema) \
  .load("/duocar/raw/riders/")

# **Note:** We must include the header option otherwise Spark will read the
# header row as a valid record.

# Confirm the explicit schema:
riders2.printSchema()

# We can read files directly from Amazon S3:
demo = spark.read.csv("s3a://duocar/raw/demographics/", sep="\t", header=True, inferSchema=True)
demo.printSchema()
demo.show(5)

# If we have write permissions, then we can also write files to Amazon S3 using
# the `s3a` prefix.


# ## Working with Parquet files

# Note that the schema is stored with the data:
spark.read.parquet("practice/riders_parquet").printSchema()


# ## Working with Hive Tables

# Use the `sql` method of the `SparkSession` class to run Hive queries:
spark.sql("SHOW DATABASES").show()
spark.sql("USE duocar")
spark.sql("SHOW TABLES").show()
spark.sql("DESCRIBE riders").show()
spark.sql("SELECT * FROM riders LIMIT 10").show()

# Note that the result of a Hive query is simply a Spark DataFrame:
riders_via_sql = spark.sql("SELECT * FROM riders")
riders_via_sql.printSchema()
riders_via_sql.show(5)

# Use the `saveAsTable` method of the `DataFrameWriter` class to save a
# DataFrame as a Hive table:
import uuid
table_name = "riders_" + str(uuid.uuid4().hex)  # Create unique table name.
riders.write.saveAsTable(table_name)

# You can now manipulate this table in Hue.
query = "DESCRIBE %s" % table_name
spark.sql(query).show()


# ## Working with pandas DataFrames

# Import the pandas package:
import pandas as pd

# Use pandas to read a local file:
demographics_pd = pd.read_csv("data/demographics.txt", sep="\t")

# Use the pandas `head` method to view the data:
demographics_pd.head()

# Use the `createDataFrame` method of the `DataFrame` class to create a Spark
# DataFrame from a pandas DataFrame:
demo_via_pandas = spark.createDataFrame(demographics_pd)
demo_via_pandas.show(5)

# Use the `toPandas` method to read a Spark DataFrame into a pandas DataFrame.
riders_pd = riders.toPandas()
riders_pd.head()

# **WARNING:** Use this with caution as you may use all your available memory!

# **Note:** Column types may not convert as expected when reading a Spark
# DataFrame into a pandas DataFrame.  See the appendix `02_toPandas.py` for
# additional details.

# Stop the `SparkSession`:
spark.stop()


