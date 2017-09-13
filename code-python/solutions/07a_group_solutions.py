# # Summarizing and grouping data

# Copyright © 2010–2017 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.

# Sample answers.  You can run this standalone, or execute selected statements in 
# a wider session.

# ## Setup

from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("group").getOrCreate()
rides = spark.read.parquet("/duocar/joined/")
rides.persist()

# ## Exercises


# Who are DuoCar's top 10 riders in terms of number of rides?

from pyspark.sql.functions import count
rides\
  .groupBy("rider_id", "rider_first_name", "rider_last_name")\
  .agg(count("*").alias("count"))\
  .orderBy("count", ascending=False)\
  .show(10)

# Or, without cancelled rides:

rides\
  .filter("cancelled == False")\
  .groupBy("rider_id", "rider_first_name", "rider_last_name")\
  .agg(count("*").alias("count"))\
  .orderBy("count", ascending=False)\
  .show(10)

  
# Who are DuoCar's top 10 drivers in terms of total distance driven?

from pyspark.sql.functions import sum
rides\
  .groupBy("driver_id", "driver_first_name", "driver_last_name")\
  .agg(sum("distance").alias("total_distance_driven"))\
  .orderBy("total_distance_driven", ascending=False)\
  .show(10)

  
# Does star rating depend on vehicle make?

from pyspark.sql.functions import avg, format_number
rides\
  .groupBy("vehicle_make")\
  .agg(avg("star_rating").alias("avg_star_rating"))\
  .withColumn("avg_star_rating", format_number("avg_star_rating", 2))\
  .orderBy("avg_star_rating")\
  .show(30)


# How do star ratings depend on rider sex and student status?

rides\
  .groupBy("rider_sex", "rider_student")\
  .agg(avg("star_rating"))\
  .orderBy("rider_sex", "rider_student")\
  .show()
  
# or:

rides\
  .groupBy("rider_sex")\
  .pivot("rider_student")\
  .agg(avg("star_rating").alias("avg_star_rating"))\
  .show()

# ## Cleanup

spark.stop()
