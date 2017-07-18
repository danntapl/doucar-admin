# # Reading and Writing Data

# GAD NOTE: 1.
# GAD NOTE: Changed all generated HDFS files to relative path 'practice' instead
# GAD NOTE: of relative path 'duocar'--hopefully that's clearer.
# GAD NOTE: 2.
# GAD NOTE: The local directory data was missing.  Added it back, and changed
# GAD NOTE: the local file to read into pandas to demographics.txt.
# GAD NOTE: 3.
# GAD NOTE: Purging practice files output files after the run.
# GAD NOTE: 4.
# GAD NOTE: You'll see other minor changes inline.

# Spark can read and write data in a variety of formats.

# * Working with text files
# * Working with delimited text files
# * Working with Parquet files
# * Working with pandas DataFrames
# * Generating a Spark DataFrame


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('read').master('local').getOrCreate()

# **Note:** The default filesystem in CDH (and by extension CDSW) is HDFS.


# ## Working with text files

# The `text` method of the `DataFrameReader` class reads each line of a text file 
# into a row of a DataFrame with a single column named *value*.

riders_txt = spark.read.text('/duocar/raw/riders/')
riders_txt.show(5)
riders_txt.head(5)

# In this case, we would need to apply further transformations to parse each line.

# Create a subdirectory in the user's home directory of HDFS:

!hdfs dfs -rm -r -skipTrash practice

!hdfs dfs -mkdir practice

# The `text` method of the `DataFrameWriter` class writes each row of a DataFrame 
# with a single string column into a line of text file.

riders_txt.write.text('practice/riders_text')

# Note that this write is to a *relative path*, in the user's home directory.

!hdfs dfs -cat practice/riders_text/* | head -n 5

# **Note:** Do not worry about the `cat: Unable to write to output steam.` message.

# The `text` method can also write a compressed file.

riders_txt.write.text('practice/riders_text_compressed', compression='bzip2')

!hdfs dfs -ls practice/riders_text

!hdfs dfs -ls practice/riders_text_compressed

# **Reference:** <https://www.cloudera.com/documentation/enterprise/latest/topics/introduction_compression.html>


# ## Working with delimited text files

# The rider data is actually a comma-delimited text file.  The `csv` method of the 
# `DataFrameWriter` class reads a delimted text file.

spark.read.\
  csv('/duocar/raw/riders/', sep=',', header=True, inferSchema=True).\
  show(5)

# This is a convenience function for the more general syntax:

riders = spark.read.\
  format('csv').\
  option('sep', ',').\
  option('header', True).\
  option('inferSchema', True).\
  load('/duocar/raw/riders/')

# **Note:** If you use either method with `inferSchema` set to `True`, then Spark 
# assumes that a header row occurs in *every* file in the data directory you load.

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

spark.read.\
  format('csv').\
  option('sep', ',').\
  option('header', True).\
  schema(schema).\
  load('/duocar/raw/riders/').\
  show(5)


# Write the file to a comma-delimited file:

riders.write.csv('practice/riders_csv', sep=',')

!hdfs dfs -ls practice/riders_csv

!hdfs dfs -cat practice/riders_csv/* | head -n 5


# ## Working with Parquet files

riders.write.parquet('practice/riders_parquet')

!hdfs dfs -ls practice/riders_parquet


# ## Working with pandas DataFrames

import pandas as pd

demographics_pd = pd.read_csv('data/demographics.txt', sep='\t')

# Note this read is from the local directory, not from HDFS.

demographics_pd.head()

spark.createDataFrame(demographics_pd).show(5)

# **Developer Note:** This does not work in Python 3.
# Different versions of Python on driver (3.6) and worker (2.6).

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

spark.range(1000).\
  withColumn('uniform', rand(12345)).\
  show(5)

# or a Bernoulli random variable with `p = 0.25`:

bern_df = spark.range(1000).\
  withColumn('Bernoulli', (rand(12345) > 0.75).cast("int"))
  
# Summary using functional style:

bern_df.groupby('Bernoulli').count().show()

# Summary using SQL style:

bern_df.createOrReplaceTempView("bern")

spark.sql("SELECT Bernoulli, COUNT(*) AS count\
    FROM bern\
    GROUP BY Bernoulli").\
  show()

# Use the `randn` function to generate a normal random variable:

from pyspark.sql.functions import randn

ran_df = spark.range(1000).\
  withColumn('normal', 42 +  2 * randn(54321))
ran_df.show(5)
ran_df.describe('id', 'normal').show()

# ## Cleanup

# Remove practice directory from HDFS.

!hdfs dfs -rm -r -skipTrash practice/riders_text

# Stop the `SparkSession`.

spark.stop()