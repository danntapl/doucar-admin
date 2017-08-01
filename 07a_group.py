# # Summarizing and grouping data

# In this module we demonstrate how to summarize and group data in a Spark
# DataFrame.

# * Summarizing data
# * Grouping data
# * Cross-tabulations
# * Pivoting


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("group").getOrCreate()

# Load the joined rides data:
rides = spark.read.parquet("/duocar/joined/")
rides.persist()


# ## Summarizing Data with Aggregation Functions

# Spark provides a number of summarization (aggregate) functions.  We have
# already seen the `describe` method:
rides.describe("distance").show()

# Use the
# [count](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.count),
# `countDistinct`, and `approx_count_distinct` functions to get more refined
# counts:
from pyspark.sql.functions import count, countDistinct, approx_count_distinct
rides.select(count("*"), count("distance"), countDistinct("distance"), approx_count_distinct("distance")).show()

# **Note:** The `count` function returns the number of rows with non-null values.

# **Note:** Use `count(lit(1))` rather than `count(1)` as an alternative to `count("*")`.

# Use the `agg` method to achieve the same results:
rides.agg(count("*"), count("distance"), countDistinct("distance"), approx_count_distinct("distance")).show()

# Use the `sum` and `sumDistinct` functions to compute a column sum:
from pyspark.sql.functions import sum, sumDistinct
rides.agg(sum("distance"), sumDistinct("distance")).show()

# **Question:** When would one use the `sumDistinct` function?

# Spark provides a number of summary statistics:
from pyspark.sql.functions import mean, stddev, variance, skewness, kurtosis
rides.agg(mean("distance"), stddev("distance"), variance("distance"), skewness("distance"), kurtosis("distance")).show()

# **Note:** `mean` is an alias for `avg`; `stddev` is an alias for the sample standard
# deviation `stddev_samp`; and
# `variance` is an alias for the sample variance `var_samp`.  The population standard deviation and
# population variance are available via `stddev_pop` and `var_pop`, respectively.

# Use the `min` and `max` functions to compute the minimum and maximum, respectively:
from pyspark.sql.functions import min, max
rides.agg(min("distance"), max("distance")).show()

# Use the `first` and `last` functions to compute the first and last values, respectively:
from pyspark.sql.functions import first, last
rides.agg(first("distance", ignorenulls=False), last("distance", ignorenulls=False)).show()

# Use the `corr`, `covar_samp`, or `covar_pop` method to measure the relationship
# between two columns:
from pyspark.sql.functions import corr, covar_samp, covar_pop
rides\
  .groupBy("rider_student")\
  .agg(corr("distance", "duration"), covar_samp("distance", "duration"), covar_pop("distance", "duration"))\
  .show()


# ## Grouping 

# Use the `agg` method with the `groupBy` (or `groupby`) method to refine your analysis:
rides\
  .groupBy("rider_student")\
  .agg(count("*"), count("distance"), mean("distance"), stddev("distance"))\
  .show()

# You can use more than one groupby column:
rides\
  .groupBy("rider_student", "service")\
  .agg(count("*"), count("distance"), mean("distance"), stddev("distance"))\
  .orderBy("rider_student", "service")\
  .show()

# Use the `rollup` method to get partial subtotals:
rides\
  .rollup("rider_student", "service")\
  .agg(count("*"), count("distance"), mean("distance"), stddev("distance"))\
  .orderBy("rider_student", "service")\
  .show()

# Consider the following example:
rides.rollup("rider_sex").agg(count("*")).orderBy("rider_sex").show()

# Which null is which?  Use the `grouping` function to distinguish
# between the null values:
from pyspark.sql.functions import grouping
rides.rollup("rider_sex").agg(grouping("rider_sex"), count("*")).orderBy("rider_sex").show()

# Use the `cube` method to get all subtotals:
rides\
  .cube("rider_student", "service")\
  .agg(count("*"), count("distance"), mean("distance"), stddev("distance"))\
  .orderBy("rider_student", "service")\
  .show()

# Use the `grouping_id` function to distinguish grouping levels:
from pyspark.sql.functions import grouping_id
rides\
  .cube("rider_student", "service")\
  .agg(grouping_id("rider_student", "service"), 
       count("*"), count("distance"), mean("distance"), stddev("distance"))\
  .orderBy("rider_student", "service")\
  .show()

  
# ## Cross-tabulations

# The following use case is common:
rides.groupby("rider_student", "service").count().orderBy("rider_student", "service").show()

# The `crosstab` method is a more direct way to get this result:
rides.crosstab("rider_student", "service").show()


# ## Pivoting

# We can also use the `pivot` method to produce a cross-tabulation:
rides.groupby("rider_student").pivot("service").count().show()

# We can also do other aggregations:
rides.groupby("rider_student").pivot("service").mean("distance").show()

rides.groupby("rider_student").pivot("service").agg(mean("distance")).show()

# You can explicity choose the values that are pivoted to columns:
rides.groupby("rider_student").pivot("service", ["Car", "Grand"]).agg(mean("distance")).show()

# Additional aggregation functions produce additional columns:
rides.groupby("rider_student").pivot("service", ["Car"]).agg(count("distance"), mean("distance")).show()

# **Note:** The `pivot` method does not like null values in the pivot column.
rides.groupBy("rider_sex", "rider_ethnicity").count().orderBy("rider_sex", "rider_ethnicity").show()

# So either explicitly specify the desired values:
rides.groupBy("rider_sex").pivot("rider_ethnicity", ["Asian", "Black", "Hispanic", "Native American", "White"]).count().show()

# or drop the null values:
rides.dropna(subset=["rider_ethnicity"]).groupBy("rider_sex").pivot("rider_ethnicity").count().show()


# ## Exercises

# Who are DuoCar's top 10 riders in terms of number of rides?

# Who are DuoCar's top 10 drivers in terms of total distance driven?

# Does star rating depend on vehicle make?

# How do star ratings depend on rider sex and student status?


# ## Cleanup

# Unpersist DataFrame:
rides.unpersist()

# Stop the SparkSession:
spark.stop()


# ## References

# [pyspark.sql.functions module](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions)
