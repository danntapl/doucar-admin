# # Transforming Data - Transforming DataFrames

# Copyright © 2010–2017 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.

# Sample answers.  You can run this standalone, or execute selected statements 
# in a wider session.


# ## Setup

from pyspark.sql import SparkSession
spark = SparkSession\
  .builder\
  .master("local")\
  .appName("transform1-exercise")\
  .getOrCreate()


# ## Exercises

# Read the raw driver data from HDFS into a Spark DataFrame.

drivers = spark.read.csv("/duocar/raw/drivers/", header=True, inferSchema=True)


# How young is the youngest driver?  How old is the oldest driver?

# Create a DataFrame from drivers with a new column, "age":
from pyspark.sql.functions \
  import current_date, floor, months_between, to_date
driver_ages = drivers\
  .withColumn("birth_date", to_date("birth_date"))\
  .withColumn("today", current_date())\
  .withColumn("age", \
              floor(months_between("today", "birth_date")/12))
   
# Youngest driver:
driver_ages\
  .orderBy("birth_date", ascending=False)\
  .select(to_date("birth_date"), "age")\
  .show(1)

  
# Oldest driver:
driver_ages\
  .orderBy("birth_date", ascending=True)\
  .select("birth_date", "age")\
  .show(1)


# How many female drivers does DuoCar have?  
# How many non-white, female drivers?

drivers\
  .filter(drivers.sex == "female")\
  .count()

drivers\
  .filter(drivers.sex == "female")\
  .filter(drivers.ethnicity != "White")\
  .count()
  

# Create a new DataFrame without any personally identifiable information (PII).

drivers_nopii = drivers.drop("first_name", "last_name")

# Note, it is possible that annonymized drivers can be reidentified with data
# like `birth_date`, `home_block`, and `home_lat/lon`.  So, a more rigorous
# annonymization might look like this:

from pyspark.sql.functions import year
drivers_nopii = drivers\
  .drop("first_name"\
        ,"last_name"\
        ,"home_block"\
        , "home_lat"\
        , "home_lon")\
  .withColumn("birth_year", year("birth_date"))\
  .drop("birth_date")


# Replace the missing values in the **rides.service** column with "Car" for
# standard DuoCar service.

rides = spark.read.csv("/duocar/raw/rides", header=True, inferSchema=True)
rides_fixed = rides.fillna("Car", "service")


# ## Cleanup

