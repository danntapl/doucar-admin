# # User-Defined Functions

# Copyright © 2010–2017 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.

# Sample answers.  You can run this standalone, or execute selected statements 
# in a wider session.


# ## Setup

# Create Spark session and read rides dataset
from pyspark.sql import SparkSession
spark = SparkSession\
  .builder.master("local").appName("udf-solutions").getOrCreate()
rides = spark.read.parquet("/duocar/clean/rides/")


# ## Exercises

# Create a user-defined function (UDF) that extracts day of the week from a
# timestamp column.  Hint: Use the Python `datetime` module.  

# Define the function
import datetime
def day_of_week(timestamp):
  return timestamp.weekday()

# Register with Spark
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf
day_of_week_udf = udf(day_of_week, returnType=IntegerType())


# Use your UDF to explore the number of rides by day of the week.

# Apply function to get a new column
rides = rides\
  .withColumn("day_of_week", day_of_week_udf("date_time"))

# Verify 
rides.select("date_time", "day_of_week").show(3, truncate=False)

# Exploratory query
rides.groupBy("day_of_week").count().orderBy("day_of_week").show()


# ## Cleanup

# Stop the SparkSession
spark.stop()

