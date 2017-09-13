# # Combining and splitting data

# Copyright © 2010–2017 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.

# Sample answers.  You can run this standalone, or execute selected statements 
# in a wider session.


# ## Setup

# Create session.
from pyspark.sql import SparkSession
spark = SparkSession\
  .builder.appName("combine-solutions").master("local").getOrCreate()


# ## Exercises

# **Exercise:** Create a DataFrame with all combinations of vehicle make and 
# vehicle year (regardless of whether the combination is observed in the data).

# Read drivers dataset.
drivers = spark.read.parquet("/duocar/clean/drivers/")

# Generate a DataFrame with all (observed) vehicle makes:
vehicle_make_df = drivers\
  .select("vehicle_make")\
  .distinct()
  
# Generate a DataFrame with all (observed) vehicle years:
vehicle_year_df = drivers\
  .select("vehicle_year")\
  .distinct()

# Use the `crossJoin` method to generate all combinations:
combinations = vehicle_make_df.crossJoin(vehicle_year_df)
combinations.orderBy("vehicle_make", "vehicle_year").show()

# **Note**: This is not bulletproof. It would not give the desired result if
# a year was missing in the middle of the list.


# **Exercise:** Join the demographic and weather data with the joined rides data.

# Read the cleaned, joined rides data
joined = spark.read.parquet("/duocar/joined/")

# Read demographic and weather data
demographics = \
  spark.read.csv("/duocar/raw/demographics/", \
                 sep="\t", header=True, inferSchema=True)
weather = spark.read.csv("/duocar/raw/weather/", header=True, inferSchema=True)

# Massage demographics column names for joined result: demographics for riders
demographics = demographics\
  .withColumnRenamed("block_group", "rider_block_group")\
  .withColumnRenamed("median_income", "rider_blk_grp_median_income")\
  .withColumnRenamed("median_age", "rider_blk_grp_median_age")

# Join to enhance joined dataset with demographics for riders' home blocks
from pyspark.sql.functions import substring
joined = joined\
  .join(demographics\
        , substring(joined.rider_home_block, 1, 12) == \
                    demographics.rider_block_group\
        , "left_outer")

# Massage weather column names for joined result.
# Also, we could consider changing all column names to lower snake case.
weather = weather\
  .withColumnRenamed("Station_ID", "weather_station_id")\
  .withColumnRenamed("Date", "weather_date")\
  .withColumnRenamed("Events", "weather_events")

# Join to enhance joined dataset with weather data  
# **Note:** this join is suitable with this data with only one weather 
# station-- a more detailed join for location will be needed as the business 
# expands to more areas.
from pyspark.sql.functions import to_date
joined = joined\
  .join(weather\
        , to_date(joined.date_time) == to_date(weather.weather_date)\
        , "left_outer")

  
# **Exercise:** Are there any drivers who have not provided a ride?

# Read rides dataset.
rides = spark.read.parquet("/duocar/clean/rides/")

# A solution using joins:
lazy_drivers1 = drivers\
  .join(rides, drivers.id == rides.driver_id, "left_anti")
lazy_drivers1.count()
lazy_drivers1.select("id").orderBy("id").show()

# A solution using set operations:
lazy_drivers2 = drivers\
  .select("id")\
  .subtract(rides.select("driver_id"))
lazy_drivers2.count()
lazy_drivers2.orderBy("id").show()


# ## Cleanup

spark.stop()
