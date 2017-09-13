# # User-Defined Functions

# Copyright © 2010–2017 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.

# In this module we demonstrate how to create and apply user-defined functions
# on Spark DataFrames.


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("udf").getOrCreate()

# Load the clean rides data from HDFS:
rides = spark.read.parquet("/duocar/clean/rides/")

# **Note:** The clean data is in parquet format.


# ## Example 1: Time of Day

# Define the Python function:
import datetime
def hour_of_day(timestamp):
  return timestamp.hour

# **Note:** The Spark TimestampType corresponds to Python `datetime.datetime` objects.

# Test the Python function:
dt = datetime.datetime(2017, 7, 21, 5, 51, 10)
hour_of_day(dt)

# Register the UDF as a DataFrame function:
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf
hour_of_day_udf = udf(hour_of_day, returnType=IntegerType())

# **Note:** We must explicitly specify the return type otherwise it defaults
# to StringType.

# Apply the UDF:
rides.select("date_time", hour_of_day_udf("date_time")).show(5, truncate=False)

# Compute number of rides by hour of day:
rides\
  .select(hour_of_day_udf("date_time").alias("hour"))\
  .groupBy("hour")\
  .count()\
  .orderBy("hour")\
  .show(25)


# ## Example 2: Haversine Distance

# The [haversine distance](https://en.wikipedia.org/wiki/Haversine_formula)
# measures the shortest distance between two points on a sphere.  In this
# example we will create a user-defined function to compute the haversine
# distance between the ride origin and destination.

# Define the haversine distance function (based on the code at
# [rosettacode.org](http://rosettacode.org/wiki/Haversine_formula#Python)):
from math import radians, sin, cos, sqrt, asin
def haversine(lat1, lon1, lat2, lon2):
  """
  Return the haversine distance in kilometers between two points.
  """
  R = 6372.8 # Earth radius in kilometers
 
  dLat = radians(lat2 - lat1)
  dLon = radians(lon2 - lon1)
  lat1 = radians(lat1)
  lat2 = radians(lat2)
 
  a = sin(dLat/2.0)**2 + cos(lat1)*cos(lat2)*sin(dLon/2.0)**2
  c = 2.0*asin(sqrt(a))
 
  return R * c

# **Note:** We have made some minor changes to the code to make it integer proof.

# Test that this result is about 2887.2599506071106:
haversine(36.12, -86.67, 33.94, -118.40)

# Register the UDF as a DataFrame function:
from pyspark.sql.types import DoubleType
haversine_udf = udf(haversine, returnType=DoubleType())

# Apply the haversine UDF:
from pyspark.sql.functions import round
distances = rides\
  .select("distance", (haversine_udf("origin_lat", "origin_lon", "dest_lat", "dest_lon") * 1000)\
  .alias("haversine_distance"))
distances.show(5)

# We would expect the haversine distance to be less than the ride distance:
distances\
  .select((distances.haversine_distance > distances.distance).alias("greater_than"))\
  .groupBy("greater_than")\
  .count()\
  .show()

# The null values probably correspond to canceled rides:
rides.filter(rides.cancelled == 1).count()

# What about the true values?
distances.filter(distances.haversine_distance > distances.distance).show(5)


# ## Exercises

# Create a user-defined function (UDF) that extracts day of the week from a
# timestamp column.  Hint: Use the Python `datetime` module.  

# Use your UDF to compute the explore the number of rides by day of the week.


# ## Cleanup

# Stop the SparkSession:
spark.stop()


# ## References

# [pyspark.sql.functions.udf](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.udf)

# [Working with UDFs in Apache Spark](https://blog.cloudera.com/blog/2017/02/working-with-udfs-in-apache-spark/)

# [Python datetime module](https://docs.python.org/2/library/datetime.html)
