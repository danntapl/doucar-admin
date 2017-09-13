# # Complex Types

# Copyright © 2010–2017 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.

# Sample answers.  You can run this standalone, or execute selected statements 
# in a wider session.


# ## Setup

# Create session and read driver dataset
from pyspark.sql import SparkSession
spark = SparkSession\
  .builder.master("local").appName("complex_types").getOrCreate()
drivers = spark.read.csv("/duocar/raw/drivers/", header=True, inferSchema=True)


# ## Exercises

# Create an array called **home_array** that includes driver's home 
# latitude and longitude.

from pyspark.sql.functions import array
drivers = drivers.withColumn("home_array", array("home_lat", "home_lon"))

# Verify

drivers.select("home_lat", "home_lon", "home_array").show(3, truncate=False)


# Create a map called **name_map** that includes the driver's first and 
# last name.

from pyspark.sql.functions import create_map, lit
drivers = drivers\
  .withColumn("name_map", \
              create_map(lit("first"), "first_name", lit("last"), "last_name"))
  
# Verify

drivers.select("first_name", "last_name", "name_map").show(3, truncate=False)


# Create a struct called **name_struct** that includes the driver's first 
# and last name.

from pyspark.sql.functions import struct
drivers = drivers\
  .withColumn("name_struct", \
              struct(drivers.first_name.alias("first"),\
                     drivers.last_name.alias("last")))

# Verify

from pyspark.sql.functions import to_json
drivers.select("first_name", "last_name", "name_struct", to_json("name_struct"))\
  .show(3, truncate=False)


# ## Cleanup

spark.stop()