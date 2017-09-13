# # Inspecting Data

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
  .appName("inspect-exercise")\
  .getOrCreate()


# ## Exercises

# Read the raw driver data from HDFS into a Spark DataFrame.

drivers = spark.read.csv("/duocar/raw/drivers/", header=True, inferSchema=True)


# Inspect the driver DataFrame.  Are the data types for each column appropriate?

# Inspect various columns of the driver DataFrame.  Are there any issues with 
# the data?

# Here's a routine to inspect each column in turn:

def inspect_dataframe(df):
  print("====")
  print("====")
  print("INSPECT DATAFRAME")
  print("====")
  
  df.printSchema()
  
  from pyspark.sql.functions import count, countDistinct
  for c in df.columns:
    print("====")
    print("Inspecting column: " + c)
    print("====")
    df.select(c).printSchema()
    df.select(c).show(5)
    df.select(c).describe().show()
    df.select(count("*").alias("N"), count(c), countDistinct(c)).show()

  # Begin column notes.  Copy and paste output into a script to begin report.
  print("")
  print("# ## Observations:")
  for c in df.columns:
    print("# * " + c + ":")
    
    
# Invoke the routine:

inspect_dataframe(drivers)
    
# ## Observations:
# * id: Datatype could be string instead of int.
# * birth_date: Datatype could be date instead of timestamp.
# * start_date: Ditto.
# * first_name:
# * last_name:
# * sex:
# * ethnicity: Has null values which could be changed to a value like 
# "Not given"
# * student: Datatype could be boolean instead of integer.
# * home_block: Datatype could be string instead of long.
# * home_lat: Fine. Could examine distribution more with approxQuantile.
# * home_lon: Ditto.
# * vehicle_make:
# * vehicle_model: This categorical has 140 distinct values; some strings 
# are numbers, hence numerical summary report.
# * vehicle_year:
# * vehicle_color:
# * vehicle_grand: Datatype could be boolean instead of integer.
# * vehicle_noir: Ditto.
# * vehicle_elite: Ditto.
# * rides:
# * stars:


# Another routine, just to consider categorical variables:

def inspect_categorical_variables(df, column_list):
  print("====")
  print("====")
  print("INSPECT CATEGORICAL VARIABLES")
  print("====")
  
  for c in column_list:
    print("====")
    print("Inspecting column: " + c)
    print("====")
    n = df.select(c).distinct().count()
    print("Number of distinct values: " + str(n))
    df.groupBy(c).count().orderBy(c).show(20)
    
  # Begin column notes.  Copy and paste output into a script to begin report.
  print("")
  print("# ## Observations:")
  for c in column_list:
    print("# * " + c + ":")
    
    
# Invoke the routine with variables of interest:    

drivers_categorical_columns =\
  ["ethnicity", "vehicle_make", "vehicle_model", "vehicle_color"]
inspect_categorical_variables(drivers, drivers_categorical_columns)    

# ## Observations:
# * ethnicity: As noted before, null values.  May want to replace these.
# * vehicle_make:
# * vehicle_model: 140 distinct values.  
# Further inspection w/`show(200).orderBy("count")` shows many values with
# just one instance.
# * vehicle_color:


# ## Cleanup

spark.stop()
