# # Complex Types

# * Arrays
# * Maps
# * Structs


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('complex_types').master('local').getOrCreate()

# Load ride data from HDFS:
rides = spark.read.csv('duocar/rides_fargo.txt', sep='\t', header=True, inferSchema=True)

# Load driver data from HDFS:
drivers = spark.read.csv('duocar/drivers_fargo.txt', sep='\t', header=True, inferSchema=True)

# Load rider data from HDFS:
riders = spark.read.csv('duocar/riders_fargo.txt', sep='\t', header=True, inferSchema=True)


# ## Arrays

# Use the [array](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.array) function to create an array from multiple columns:
from pyspark.sql.functions import array
origin_array = rides.select('origin_lat', 'origin_lon', array('origin_lat', 'origin_lon').alias('origin'))
origin_array.head(5)

# **Note:** `origin` is a Python list.

# Use the `size` function to get the length of the array:
from pyspark.sql.functions import size
origin_array.select('origin', size('origin')).head(5)

# Access the array elements using index syntax:

origin_array.select('origin', origin_array.origin[0]).head(5)

# **Note:** Some equivalent alternatives to the previous expression:

origin_array.select(origin_array['origin'][0]).head()

from pyspark.sql.functions import col
origin_array.select(col('origin')[0]).head()

from pyspark.sql.functions import column
origin_array.select(column('origin')[0]).head()

from pyspark.sql.functions import expr
origin_array.select(expr('origin[0]')).head()

origin_array.selectExpr('origin[0]').head()

# Sort an array

# TDB

# Search an array

# TBD


# ## Maps

# Use the [create_map](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.create_map) function to create a map:
from pyspark.sql.functions import create_map
silly_map = drivers.select(create_map('id', 'last_name').alias('name_map'))
silly_map.head(5)

# **Question:** Can we find a more interesting example?

# Access a value by key

# TBD


# ## Structs

# Use the `struct` function to create a struct:
from pyspark.sql.functions import struct
vehicle_struct = drivers.select(struct('vehicle_make', 'vehicle_model', 'vehicle_year').alias('vehicle'))
vehicle_struct.head(5)

# **Note:** The struct is a `Row` object (embedded in a `Row` object).

# **Question:** Is there a way to rename 'vehicle_make' to 'make' as part of the struct creation process?
drivers.select(struct(drivers.vehicle_make.alias('make'),\
                      drivers.vehicle_model.alias('model'),\
                      drivers.vehicle_year.alias('year')).alias('vehicle')).head(5)


# Use dot notation to access struct items:
vehicle_struct.select(vehicle_struct.vehicle.vehicle_make.alias('vehicle_make')).head(5)

# Use the `to_json` function to convert the struct to a JSON string:
from pyspark.sql.functions import to_json
vehicle_struct.select(to_json('vehicle')).head(5)


# ## Cleanup

# Stop the SparkSession:
spark.stop()


# ## TODO
# * Complete array examples
# * Complete map examples
# * Complete struct examples
# * Generate exercises