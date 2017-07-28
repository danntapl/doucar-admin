# # Explore Window Functions

# Create SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("explore_window").master("local").getOrCreate()

# Read data:
rides = spark.read.parquet("/duocar/joined/")

# Select and filter data:
tmp = rides.select("ride_id", "rider_id", "date_time").filter(rides.rider_id < "220200000003")

from pyspark.sql import Window
ws = Window.partitionBy("rider_id").orderBy("date_time")

from pyspark.sql.functions import *
tmp.select("*",
           count("ride_id").over(ws).alias("count"),
           rank().over(ws).alias("rank"),
           dense_rank().over(ws).alias("dense_rank"),
           percent_rank().over(ws).alias("percent_rank"),
           ntile(4).over(ws).alias("ntile")).show(60)



rides.groupBy("rider_id", window("date_time", "1 week")).count().show()

tmp.rollup("rider_id").count().show()

# It appears that you can not use an empty over() statement like in SQL:
#tmp.select("*", count("ride_id").over().alias("count")).show(100)

# Window specification for all rows:
ws = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
tmp.select("*", count("ride_id").over(ws).alias("count")).show(100)

# Window specification for all rows with rider_id partitions:
ws = Window.partitionBy("rider_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
tmp.select("*", count("ride_id").over(ws).alias("count")).show(100)

# Window specification for all rows with rider_id partitions:
ws = Window.partitionBy("rider_id").rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
tmp.select("*", count("ride_id").over(ws).alias("count")).show(100)

# **Question:** Is there any difference between `rangeBetween` and `rowsBetween`?

# Compute date and time for first ride for each rider:
ws = Window.partitionBy("rider_id").rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
tmp.select("*", min("date_time").over(ws).alias("timestamp_first_ride")).show(100, truncate=False)


# Compute time between rides:
ws = Window.partitionBy("rider_id").orderBy("date_time")
tmp.select("*", lag("date_time").over(ws).alias("last_ride")).show(100, truncate=False)

tmp.select("*", lag("date_time").over(ws).alias("last_ride")).show(100, truncate=False)

tmp.withColumn("previous_ride", lag("date_time").over(ws))\
  .withColumn("time_between_rides", datediff("date_time", "previous_ride"))\
  .groupBy("rider_id")\
  .agg(mean("time_between_rides"))\
  .show()
  
ttt = rides\
  .withColumn("previous_ride", lag("date_time").over(ws))\
  .withColumn("time_between_rides", datediff("date_time", "previous_ride"))\
  .groupBy("rider_id", "rider_student")\
  .agg(mean("time_between_rides").alias("mean_time_between_rides"))\
  .toPandas()

ttt.hist("mean_time_between_rides", by="rider_student")

# Stop the SparkSession:
spark.stop()