# # Reading and Writing Data

# Copyright © 2010–2017 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.

# This script contains sample solutions to the exercises.  You can run this
# script in a new session or execute selected statements in an existing
# session.  Note that there is generally more than one way to solve a problem.


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession \
  .builder \
  .master("local") \
  .appName("read-solutions") \
  .getOrCreate()


# ## Exercises

# (1) Read the raw driver file from HDFS into a Spark DataFrame.

drivers = spark.read.csv("/duocar/raw/drivers/", header=True, inferSchema=True)

drivers.first()

# (2) Save the driver DataFrame as a JSON file in your CDSW practice directory.

!hdfs dfs -mkdir practice

drivers.write.format("json").save("practice/drivers_json")

# (3) Use Hue to inspect the JSON file.

# **Note:** The JSON file is noticeably larger than the corresponding CSV file.
# Each row is a JSON object.

# (4) Read the driver JSON file into a Spark DataFrame.

drivers_from_json = spark \
  .read \
  .format("json") \
  .load("practice/drivers_json")
  
drivers_from_json.first()

# **Note:** The columns are in alphabetical order.

# (5) Delete the JSON file.
  
!hdfs dfs -rm -r -skipTrash practice/drivers_json


# ## Cleanup

# Stop the SparkSession:

spark.stop()

