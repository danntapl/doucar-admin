# # Reading and Writing Data

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

# The `text` method of the `DataFrameReader` class reads each line of a text file into a row of a DataFrame with a single column named *value*.

riders_txt = spark.read.text('duocar/riders_fargo.txt')
riders_txt.show(5)
riders_txt.head(5)

# In this case, we would need to apply further transformations to parse each line.

# The `text` method of the `DataFrameWriter` class writes each row of a DataFrame with a single string column into a line of text file.

riders_txt.write.text('duocar/riders_text')
!hdfs dfs -cat duocar/riders_text/* | head -n 5

# **Note:** Do not worry about the `cat: Unable to write to output steam.` message.

# The `text` method can also write a compressed file.

riders_txt.write.text('duocar/riders_text_compressed', compression='bzip2')
!hdfs dfs -ls duocar/riders_text
!hdfs dfs -ls duocar/riders_text_compressed

# **Reference:** <https://www.cloudera.com/documentation/enterprise/latest/topics/introduction_compression.html>


# ## Working with delimited text files

# The rider data is actually a tab-delimited text file.  The `csv` method of the `DataFrameWriter` class reads a delimted text file.

spark.read.csv('duocar/riders_fargo.txt', sep='\t', header=True, inferSchema=True).show(5)

# This is a convenience function for the more general syntax:

riders = spark.read.format('csv').option('sep', '\t').option('header', True).option('inferSchema', True).load('duocar/riders_fargo.txt')

# Write the file to a comma-delimited file:
riders.write.csv('duocar/riders_csv', sep=',')
!hdfs dfs -ls duocar/riders_csv
!hdfs dfs -cat duocar/riders_csv/* | head -n 5


# ## Working with Parquet files

riders.write.parquet('duocar/riders_parquet')
!hdfs dfs -ls duocar/riders_parquet


# ## Working with pandas DataFrames

import pandas as pd
drivers_pd = pd.read_csv('data/drivers_fargo.txt', sep='\t')
drivers_pd.head()
spark.createDataFrame(drivers_pd).show(5)

# **Developer Note:** This does not work in Python 3.
# Different versions of Python on driver (3.6) and worker (2.6).

# Use the `toPandas` method to read a Spark DataFrame into a pandas DataFrame.

riders_pd = riders.toPandas()
riders_pd.head()

# **WARNING:** Use this with caution as you may use all your available memory!


# ## Generating a Spark DataFrame

# Sometimes we need to generate a Spark DataFrame from scratch, for example, for testing purposes.

# Use the `range` method to generate a sequence of integers and generate new columns as appropriate.
spark.range(1000).show(5)

# Use the `rand` function to generate a uniform random variable:
from pyspark.sql.functions import rand
spark.range(1000).withColumn('uniform', rand(12345)).show(5)

# or a Bernoulli random variable with `p = 0.25`:
spark.range(1000).withColumn('Bernoulli', (rand(12345) > 0.75).cast("int")).groupby('Bernoulli').count().show()

# Use the `randn` function to generate a normal random variable:
from pyspark.sql.functions import randn
spark.range(1000).withColumn('normal', 42 +  2 * randn(54321)).describe('normal').show()


# ## Cleanup

# Remove files.
!hdfs dfs -rm -r duocar/riders_text
!hdfs dfs -rm -r duocar/riders_text_compressed
!hdfs dfs -rm -r duocar/riders_csv
!hdfs dfs -rm -r duocar/riders_parquet

# Stop the `SparkSession`.
spark.stop()