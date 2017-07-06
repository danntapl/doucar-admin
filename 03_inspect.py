# # Inspecting Data

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


# ## Connect to the Spark cluster

from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local').appName('inspect').getOrCreate()

# ## Load the rider data from HDFS into a Spark DataFrame ##

riders = spark.read.csv('duocar/riders_fargo.txt', sep='\t', header=True, inferSchema=True)

# **Developer Note:** We are using a local Spark instance to speed things up on this small dataset.


# ## Examining the schema

# Use the `printSchema` method to examine the schema.
riders.printSchema()

# Use the `schema`, `dtypes`, and `columns` attributes to view the schema in other ways.
riders.schema
riders.dtypes
riders.columns


# ## Viewing some data

# Use the `show` method to get a SQL-like display of the data.
riders.show(5)

# Use the `head` or `take` method to get a display of the `Row` objects that is sometimes easier to read.
riders.head(5)

# **Notes:** 
# * `birth_date` and `start_date` are Timestamps rather than Dates.
# * `ethnicity` appears to have null (missing) values.
# * `home_block` in as a long integer rather than a string.


# ## Computing the number of rows (observations)

# Use the `count` method to compute the number of rows.
riders.count()


# ## Computing summary statistics

# Use the `describe` method to compute summary statistics for numeric and string columns.
riders.describe().show()

# **Notes:** 
# * The count in the `describe` method is a count of non-missing values.
# * `sex` and `ethnicity` seem to have some missing values.


# ## Inspecting a column (variable) ##

# We often want to inspect one variable at a time.

# ### Inspect a key variable ###

# The variable `id` should be a unique value.  Let us confirm this.

riders.describe('id').show()

# Use the `countDistinct` function to determine the number of distinct values:
from pyspark.sql.functions import count, countDistinct
riders.select(count('id'), countDistinct('id')).show()
# **Note:** This can take quite a bit of time on large DataFrames.

# ### Inspect a categorical variable

# The variable `sex` is a categorical variable.
riders.select(count('*'), count('sex'), countDistinct('sex')).show()

# Use the `select` method to select a subset of columns:
riders.select('sex').show(5)

# Use the `distinct` method to determine the uniques values of `sex`:
riders.select('sex').distinct().count()

# **Developer Note:** See https://jira.cloudera.com/browse/DSE-1159 for the print issue.

riders.select('sex').distinct().show()

# An alternative approach is use an aggregation.

riders.groupby('sex').count().show()

# **Note:** `sex` contains null (missing) values that we must deal with.

# ### Inspecting a numerical variable

riders.select('home_lat', 'home_lon').show(5)

riders.select('home_lat', 'home_lon').describe().show(5)

# **Notes:**
# * No missing values
# * No extreme values (tight distribution about [Fargo, North Dakota](https://en.wikipedia.org/wiki/Fargo%2C_North_Dakota))

# ### Inspecting a datetime variable

# Let us inspect `birth_date` and `start_date`, which are both Timestamp variables.

riders.select('birth_date', 'start_date').show(5)

riders.select('birth_date', 'start_date').head(5)

# Note that the original data was in Date format, but Spark read the data in Timestamp format.
# We will probably want to fix this.

# Note that the `describe` method does not work with Date or Timestamp variables.
riders.select('birth_date', 'start_date').describe().show(5)


# ## Cleanup

# Stop the SparkSession.
spark.stop()