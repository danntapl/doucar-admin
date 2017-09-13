# # Inspecting Data

# Copyright © 2010–2017 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.

# In this module we will take our first look at a Spark DataFrame.
# * Examining the schema
# * Viewing some data
# * Computing the number of rows (observations)
# * Computing summary statistics
# * Inspecting a column (variable)
#   * Inspecting a key variable
#   * Inspecting a categorical variable
#   * Inspecting a continuous numerical variable
#   * Inspecting a datetime variable


# ## Create a SparkSession

from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("inspect").getOrCreate()

# **Note:** We are using Spark in local mode to speed things up on this small
# dataset.


# ## Load the rider data from HDFS into a Spark DataFrame

riders = spark.read.csv("/duocar/raw/riders/", header=True, inferSchema=True)

# **Note:** The default filesystem is HDFS and Spark will attempt to read all
# files in the HDFS directory.


# ## Examining the schema

# Use the `printSchema` method of the
# [DataFrame](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame)
# class to examine the schema:

riders.printSchema()

# Use the `dtypes`, `columns`, and `schema` attributes to view the schema in
# other ways:

riders.dtypes
riders.columns
riders.schema

# **Note:** The `schema` attribute provides a programmatic version of the
# schema.


# ## Viewing some data

# Use the `show` method to get a SQL-like display of the data:

riders.show(5)

# Use the `head` or `take` method to get a display of the `Row` objects that is
# sometimes easier to read:

riders.head(5)

# **Notes:** 
# * `id` and `home_block` are long integers rather than key strings.
# * `birth_date` and `start_date` are Timestamps rather than Dates.
# * `ethnicity`, `work_lat`, and `work_lon` appear to have null (missing) values.
# * `student` is a boolean variable inefficiently encoded as an integer.


# ## Computing the number of rows (observations)

# Use the `count` method to compute the number of rows:

riders.count()


# ## Computing summary statistics

# Use the `describe` method to compute summary statistics for numeric and
# string columns:

riders.describe().show()

# **Notes:** 
# * The count in the `describe` method is a count of non-missing values.
# * `sex` and `ethnicity` seem to have some missing values.


# ## Inspecting a column (variable)

# We often want to inspect one or a few variables at a time.


# ### Inspect a key variable ###

# The variable `id` should be a unique value.  Let us confirm this.  Use the
# `select` method to select the `id` column:

riders.select("id").describe().show()

# Alternatively, pass the column name to the `describe` method:

riders.describe("id").show()

# Use the `countDistinct` function to determine the number of distinct values:

from pyspark.sql.functions import count, countDistinct
riders.select(count("id"), countDistinct("id")).show()

# **Note:** This can take quite a bit of time on large DataFrames.

# You can use functional style, as shown above, or SQL style for examination of
# DataFrames.  SQL style requires one preliminary step:

riders.createOrReplaceTempView("riders")

spark.sql("select count(id), count(distinct id) from riders").show()


# ### Inspect a categorical variable

# The variable `sex` is a categorical variable.  Let us examine it more
# carefully:

riders.select(count("*"), count("sex"), countDistinct("sex")).show()

# Use the `select` method to select a subset of columns:

riders.select("sex").show(5)

# Use the `distinct` method to determine the unique values of `sex`:

riders.select("sex").distinct().count()

# **Developer Note:** See https://jira.cloudera.com/browse/DSE-1159 for the
# print issue.

riders.select("sex").distinct().show()

# An alternative approach is to use an aggregation:

riders.groupby("sex").count().show()

# The same query in SQL style:

spark.sql("select sex, count(*) from riders group by 1").show()
  
# **Note:** `sex` contains null (missing) values that we may have to deal with.


# ### Inspecting a numerical variable

riders.select("home_lat", "home_lon").show(5)
riders.select("home_lat", "home_lon").describe().show(5)

# **Notes:**
# * No missing values
# * No extreme values (tight distribution about [Fargo, North Dakota](https://en.wikipedia.org/wiki/Fargo%2C_North_Dakota))

# Use the `approxQuantile` to get customized (approximate) quantiles:

riders.approxQuantile("home_lat", \
	probabilities=[0.0, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 1.0], \
	relativeError=0.1)

# **Note:** This method returns a Python list.  Spark 2.2 adds support for
# multiple columns.


# ### Inspecting a datetime variable

# Let us inspect `birth_date` and `start_date`, which are both Timestamp
# variables:

dates = riders.select("birth_date", "start_date")
dates.show(5, truncate=False)
dates.head(5)

# Note that the original data was in Date format, but Spark read the data in
# Timestamp format.  We will probably want to fix this.

# Note that the `describe` method does not work with Date or Timestamp
# variables:

dates.describe().show(5)


# ## Exercises

# (1) Read the raw driver data from HDFS into a Spark DataFrame.

# (2) Inspect the driver DataFrame.  Are the data types for each column
# appropriate?

# (3) Inspect the columns of the driver DataFrame.  Are there any issues with
# the data?


# ## Cleanup

# Stop the SparkSession:

spark.stop()


# ## References

# [Spark SQL, DataFrames, and Datasets Guide](http://spark.apache.org/docs/latest/sql-programming-guide.html)

# [DataFrame class](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame)

# [spark.sql.types module](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.types)
