# # Reading and Writing Data

# In this module we demonstrate how to read and write a variety of data formats
# into and out of Apache Spark.

# * Working with text files
# * Working with delimited text files
# * Working with Parquet files
# * Working with Hive tables
# * Working with pandas DataFrames
# * Generating a Spark DataFrame


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("read").enableHiveSupport().getOrCreate()

# **Note:** The subsequent Hive examples seem to work without the `enableHiveSupport` method.


# ## Working with text files

# The `text` method of the
# [DataFrameReader](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader)
# class reads each line of a text file into a row of a DataFrame with a single
# column named *value*.

riders_txt = spark.read.text("/duocar/raw/riders/")

# **Note:** The default filesystem in CDH (and by extension CDSW) is HDFS.

riders_txt.show(5, truncate=False)
riders_txt.head(5)

# In this case, we would need to apply further transformations to parse each
# line.

# Create a subdirectory in the user's home directory of HDFS:

!hdfs dfs -rm -r -skipTrash practice

!hdfs dfs -mkdir practice

# The `text` method of the
# [DataFrameWriter](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameWriter)
# class writes each row of a DataFrame with a single string column into a line
# of text file.

riders_txt.write.text("practice/riders_text")

# Note that this write is to a *relative path* in the user's home directory.

!hdfs dfs -cat practice/riders_text/* | head -n 5

# **Note:** Do not worry about the `cat: Unable to write to output steam.` message.

# The `text` method can also write a compressed file.

riders_txt.write.text("practice/riders_text_compressed", compression="bzip2")

!hdfs dfs -ls practice/riders_text

!hdfs dfs -ls practice/riders_text_compressed

# **Reference:**
# <https://www.cloudera.com/documentation/enterprise/latest/topics/introduction_compression.html>


# ## Working with delimited text files

# The rider data is actually a comma-delimited text file.  The `csv` method of
# the `DataFrameWriter` class reads a delimited text file.

spark.read\
  .csv("/duocar/raw/riders/", sep=",", header=True, inferSchema=True)\
  .show(5)

# This is actually a convenience function for the more general syntax:

riders = spark.read\
  .format("csv")\
  .option("sep", ",")\
  .option("header", True)\
  .option("inferSchema", True)\
  .load("/duocar/raw/riders/")

# **Note:** If you use either method with `header` set to `True`, then Spark
# assumes that a header row occurs in *every* file in the data directory you
# load.

riders.printSchema()

# You can manually specify the schema instead of inferring it from the header
# row and column values:

from pyspark.sql.types import *

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

riders2 = spark.read\
  .format("csv")\
  .option("sep", ",")\
  .schema(schema)\
  .load("/duocar/raw/riders/")

riders2.printSchema()

# Write the file to a tab-delimited file:

riders.write.csv("practice/riders_tsv", sep="\t")

!hdfs dfs -ls practice/riders_tsv

!hdfs dfs -cat practice/riders_tsv/* | head -n 5


# ## Working with Parquet files

# [Parquet](https://parquet.apache.org/) is a very popular columnar storage
# format for Hadoop.  Use the `parquet` method of the `DataFrameWriter` class
# to save a DataFrame in Parquet:

riders.write.parquet("practice/riders_parquet")

!hdfs dfs -ls practice/riders_parquet

# Note that the schema is stored with the data:

spark.read.parquet("practice/riders_parquet").printSchema()


# ## Working with Hive Tables

# Use the `sql` method of the `SparkSession` class to run Hive queries:

spark.sql("SHOW DATABASES").show()

spark.sql("SHOW TABLES").show()

spark.sql("DESCRIBE airlines").show()

spark.sql("SELECT * from airlines limit 10")

# Note that the results of a Hive query is simply a Spark DataFrame:

airlines = spark.sql("SELECT * FROM airlines")
airlines.printSchema()
airlines.show(5)

# Use the `saveAsTable` method of the `DataFrameWriter` class to save a
# DataFrame as a Hive table:

riders.write.saveAsTable("riders")

# You can now manipulate this table in Hue.  Note that all fields have been read in as strings.


# ## Working with pandas DataFrames

import pandas as pd

demographics_pd = pd.read_csv("data/demographics.txt", sep="\t")

# Note this read is from the local directory, not from HDFS.

demographics_pd.head()

# Use the `createDataFrame` method of the `DataFrame` class to create a Spark
# DataFrame from a pandas DataFrame:

spark.createDataFrame(demographics_pd).show(5)

# Use the `toPandas` method to read a Spark DataFrame into a pandas DataFrame.

riders_pd = riders.toPandas()

riders_pd.head()

# **WARNING:** Use this with caution as you may use all your available memory!


# ## Generating a Spark DataFrame

# Sometimes we need to generate a Spark DataFrame from scratch, for example, 
# for testing purposes.

# Use the `range` method to generate a sequence of integers and add new 
# columns as appropriate.

spark.range(1000).show(5)

# Use the `rand` function to generate a uniform random variable:

from pyspark.sql.functions import rand

spark.range(1000)\
  .withColumn("uniform", rand(12345))\
  .show(5)

# or a Bernoulli random variable with `p = 0.25`:

bern_df = spark.range(1000)\
  .withColumn("Bernoulli", (rand(12345) < 0.25).cast("int"))
  
# Generate a summary using the functional style:

bern_df.groupby("Bernoulli").count().show()

# Generate a summary using the SQL style:

bern_df.createOrReplaceTempView("bern")

spark.sql("SELECT Bernoulli, COUNT(*) AS count\
    FROM bern\
    GROUP BY Bernoulli")\
  .show()

# Use the `randn` function to generate a normal random variable:

from pyspark.sql.functions import randn

ran_df = spark.range(1000).withColumn("normal", 42 +  2 * randn(54321))
ran_df.show(5)
ran_df.describe("id", "normal").show()


# ## Exercises

# Read the raw driver file into a Spark DataFrame.

# Save the driver DataFrame as a JSON file in your CDSW practice directory.

# Read the driver JSON file into a Spark DataFrame.


# ## Cleanup

# Remove practice directory contents from HDFS:

!hdfs dfs -rm -r -skipTrash practice/riders_text
!hdfs dfs -rm -r -skipTrash practice/riders_tsv
!hdfs dfs -rm -r -skipTrash practive/riders_parquet
spark.sql("DROP TABLE IF EXISTS riders")

# Stop the `SparkSession`:

spark.stop()


# ## References

# [DataFrameReader class](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader)

# [DataFrameWriter class](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameWriter)
