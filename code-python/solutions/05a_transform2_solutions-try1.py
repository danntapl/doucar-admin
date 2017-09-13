# # Transforming Data - Transforming DataFrame Columns

# Copyright © 2010–2017 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.

# Sample answers.  You can run this standalone, or execute selected statements in 
# a wider session.

# Initial setup and read rides dataset.

from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("transform2_answer").getOrCreate()

rides = spark.read.csv("/duocar/raw/rides/", header=True, inferSchema=True)

# ## Exercises

# Convert the **rides.driver_id** column to a string column.

from pyspark.sql.functions import format_string
rides = rides.withColumn("driver_id", format_string("%012d", "driver_id"))

# Extract the year from the **rides.date_time** column.

from pyspark.sql.functions import year
rides = rides.withColumn("year", year("date_time"))

# Convert **rides.duration** from seconds to  minutes.

from pyspark.sql.functions import col, round
rides = rides.withColumn("duration_minutes", round(col("duration")/60, 1))

# Convert the **rides.cancelled** column to a boolean column.

from pyspark.sql.functions import col
rides = rides.withColumn("cancelled", col("cancelled")==1)

# Convert the **rides.star_rating** column to a double column.

from pyspark.sql.functions import col
rides = rides.withColumn("star_rating", col("star_rating") * 1.0)

# Altogether:
rides = spark.read.csv("/duocar/raw/rides/", header=True, inferSchema=True)
from pyspark.sql.functions import format_string, year, col, round
rides = rides\
  .withColumn("driver_id", format_string("%012d", "driver_id"))\
  .withColumn("year", year("date_time"))\
  .withColumn("duration_minutes", round(col("duration")/60, 1))\
  .withColumn("cancelled", col("cancelled")==1)\
  .withColumn("star_rating", col("star_rating") * 1.0)

rides.show(5)
rides.printSchema()

# Clean up

spark.stop()
