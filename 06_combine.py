# # Combining, splitting, and reshaping data

# * Joining
# * Set operations
# * Splitting
# * Pivoting


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('combine').master('local').getOrCreate()

# Load the rides data:
rides = spark.read.csv('duocar/rides_fargo.txt', sep='\t', header=True, inferSchema=True)

# Load the rider data:
riders = spark.read.csv('duocar/riders_fargo.txt', sep='\t', header=True, inferSchema=True)

# Load the driver data:
drivers = spark.read.csv('duocar/drivers_fargo.txt', sep='\t', header=True, inferSchema=True)

# Load demographic data.
demo = spark.read.csv('duocar/demographics.txt', sep='\t', header=True, inferSchema=True)


# ## Joining

# Use the `crossJoin` DataFrame method to create the Cartesian product of two DataFrames:

vehicle_make_df = drivers.select('vehicle_make').distinct().orderBy('vehicle_make')
vehicle_make_df.show()

vehicle_year_df = drivers.select('vehicle_year').distinct().orderBy('vehicle_year')
vehicle_year_df.show()

vehicle_make_df.crossJoin(vehicle_year_df).show()

# **Warning:** Be careful as this can result in very large DataFrames.


# ## Set operations

# Spark SQL provides the following DataFrame methods that implement various set operations:
# * `union`
# * `intersect`
# * `subtract`

# Use the `union` method to get the union of rows in two DataFrames with similar schema:
driver_names = drivers.select('first_name')
driver_names.count()

rider_names = riders.select('first_name')
rider_names.count()

driver_names.union(rider_names).count()

# Note that `union` does not remove duplicates.  Use the `distinct` method to remove duplicates:
driver_names.union(rider_names).distinct().count()


# ## Splitting

# ### Deterministic splitting

# **Question:** Is there a way to split on the value of a (categorical) variable?

# ### Random splitting

# Use the `randomSplit` DataFrame method to split a DataFrame into random subsets:

riders.count()

(train, validate, test) = riders.randomSplit([0.6, 0.2, 0.2])
train.count()
validate.count()
test.count()

# Use the `seed` argument to ensure replicability:
(train, validate, test) = riders.randomSplit([0.6, 0.2, 0.2], seed=12345)
train.count()
validate.count()
test.count()

# If the proportions do not add up to one, then Spark will normalize the values:
(train, validate, test) = riders.randomSplit([60.0, 20.0, 20.0], seed=12345)
train.count()
validate.count()
test.count()

# **Note:** We will use this functionality to create train, validation, and test datasets for machine learning pipelines.


# ## Pivoting


# ## Cleanup

# Stop the SparkSession
spark.stop()