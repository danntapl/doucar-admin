# # Complex Types

# In this module we demonstrate complex column types in Spark SQL.

# * Arrays
# * Maps
# * Structs


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("complex_types").getOrCreate()

# Load raw ride data from HDFS:
rides = spark.read.csv("/duocar/raw/rides/", header=True, inferSchema=True)

# Load raw driver data from HDFS:
drivers = spark.read.csv("/duocar/raw/drivers/", header=True, inferSchema=True)

# Load raw rider data from HDFS:
riders = spark.read.csv("/duocar/raw/riders/", header=True, inferSchema=True)


# ## Arrays

# Use the
# [array](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.array)
# function to create an array from multiple columns:
from pyspark.sql.functions import array
drivers_array = drivers.select("vehicle_make", "vehicle_model",\
    array("vehicle_make", "vehicle_model").alias("vehicle_array"))
drivers_array.printSchema()
drivers_array.show(5, truncate=False)

# **Note:** `vehicle_array` is a Python list.

# Use the
# [size](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.size)
# function to get the length of the array:
from pyspark.sql.functions import size
drivers_array.select("vehicle_array", size("vehicle_array")).show(5, False)

# Use index notation to access elements of the array:
drivers_array.select("vehicle_array", drivers_array.vehicle_array[0]).show(5, False)

# **Note:** Some equivalent alternatives to the previous expression:

drivers_array.select("vehicle_array", drivers_array["vehicle_array"][0]).show(1, False)

from pyspark.sql.functions import col
drivers_array.select("vehicle_array", col("vehicle_array")[0]).show(1, False)

from pyspark.sql.functions import column
drivers_array.select("vehicle_array", column("vehicle_array")[0]).show(1, False)

from pyspark.sql.functions import expr
drivers_array.select("vehicle_array", expr("vehicle_array[0]")).show(1, False)

drivers_array.selectExpr("vehicle_array", "vehicle_array[0]").show(1, False)

# Use the
# [sort_array](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.sort_array)
# function to sort the array:
from pyspark.sql.functions import sort_array
drivers_array.select("vehicle_array", sort_array("vehicle_array", asc=True)).show(5, False)

# Use the `array_contains` function to search the array:
from pyspark.sql.functions import array_contains
drivers_array.select("vehicle_array", array_contains("vehicle_array", "Subaru")).show(5, False)

# Use the `explode` and `posexplode` functions to explode the array:
from pyspark.sql.functions import explode, posexplode
drivers_array.select("vehicle_array", explode("vehicle_array")).show(5, False)
drivers_array.select("vehicle_array", posexplode("vehicle_array")).show(5, False)


# ## Maps

# Use the
# [create_map](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.create_map)
# function to create a map:
from pyspark.sql.functions import lit, create_map
drivers_map = drivers.select("vehicle_make", "vehicle_model",\
    create_map(lit("make"), "vehicle_make", lit("model"), "vehicle_model").alias("vehicle_map"))
drivers_map.printSchema()
drivers_map.show(5, False)

# Use the `size` function to get the length of the map:
drivers_map.select("vehicle_map", size("vehicle_map")).show(5, False)

# Use dot notation to access the value by key:
drivers_map.select("vehicle_map", drivers_map.vehicle_map.make).show(5, False)

# Use the `explode` and `posexplode` functions to explode the map:
drivers_map.select("vehicle_map", explode("vehicle_map")).show(5, False)
drivers_map.select("vehicle_map", posexplode("vehicle_map")).show(5, False)


# ## Structs

# Use the
# [struct](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.struct)
# function to create a struct:
from pyspark.sql.functions import struct
drivers_struct = drivers.select("vehicle_make", "vehicle_model",\
    struct(drivers.vehicle_make.alias("make"),\
    drivers.vehicle_model.alias("model")).alias("vehicle_struct"))
drivers_struct.printSchema()
drivers_struct.show(5, False)

# **Note:** The struct is a `Row` object (embedded in a `Row` object).
drivers_struct.head(5)

# Use dot notation to access struct items:
drivers_struct.select("vehicle_struct", col("vehicle_struct").make.alias("vehicle_make")).show(5, False)

# **Note:** Using `col` is a bit more concise in this case.

# Use the `to_json` function to convert the struct to a JSON string:
from pyspark.sql.functions import to_json
drivers_struct.select("vehicle_struct", to_json("vehicle_struct")).show(5, False)


# ## Exercises

# Create an array called **home_array** that includes driver's home latitude and longitude.

# Create a map called **name_map** that includes the driver's first and last name.

# Create a struct called **name_struct** that includes the driver's first and last name.


# ## Cleanup

# Stop the SparkSession:
spark.stop()


# ## References

# [pyspark.sql.functions.array](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.array)

# [pyspark.sql.functions.create_map](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.create_map)

# [pyspark.sql.functions.struct](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.struct)

