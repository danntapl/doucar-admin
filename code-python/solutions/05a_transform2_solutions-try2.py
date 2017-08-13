# # Transforming Data - Transforming DataFrame Columns

# Sample answers.  You can run this standalone, or execute selected statements in 
# a wider session.

# Initial setup and read rides dataset.

from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("transform2_answer").getOrCreate()

rides = spark.read.csv("/duocar/raw/rides/", header=True, inferSchema=True)

# ## Exercises

# Convert the **rides.driver_id** column to a string column.

# Extract the year from the **rides.date_time** column.

# Convert **rides.duration** from seconds to  minutes.

# Convert the **rides.cancelled** column to a boolean column.

# Convert the **rides.star_rating** column to a double column.


# Altogether:

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
