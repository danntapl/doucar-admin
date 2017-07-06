# # User-Defined Functions

# The [haversine distance](https://en.wikipedia.org/wiki/Haversine_formula) measures the shortest distance between two points on a sphere.
# In this module we will create a user-defined function to compute the haversine distance between the ride origin and destination.



# ## Create a SparkSession
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('udf').master('local').getOrCreate()


# ## Load the rides data
rides = spark.read.csv('duocar/rides_fargo.txt', sep='\t', header=True, inferSchema=True)


# ## Define the haversine distance function

# Based on the code at [rosettacode.org](http://rosettacode.org/wiki/Haversine_formula#Python):

from math import radians, sin, cos, sqrt, asin

# **Question:** Does the import need to be inside the function definition?

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


# ## Register the UDF as a DataFrame function

from pyspark.sql.functions import udf
haversine_udf = udf(haversine)


# ## Apply the haversine UDF
from pyspark.sql.functions import round
distances = rides.select((haversine_udf('origin_lat', 'origin_lon', 'dest_lat', 'dest_lon') * 1000).alias('haversine_distance'), 'distance')
distances.show(10)

# We would expect the haversine distance to be less than the ride distance.
distances.select((distances.haversine_distance > distances.distance).alias('greater_than')).groupBy('greater_than').count().show()

# The null values probably correspond to canceled rides:
rides.filter(rides.cancelled == 1).count()

# What about the true values?
distances.filter(distances.haversine_distance > distances.distance).show()


# ## Stop the SparkSession
spark.stop()

# ## References

# [Working with UDFs in Apache Spark](https://blog.cloudera.com/blog/2017/02/working-with-udfs-in-apache-spark/)


# ## TODO

# * Register and apply the UDF as a Spark SQL function
# * Use a Scala UDF from Python
# * Note the performance hit of using Python UDFs versus Scala or Java UDFs