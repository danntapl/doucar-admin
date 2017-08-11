# ## Tuning Spark Applications

# Spark applications and Spark environments present countless opportunities for
# ever deeper understanding and performance tuning.  Here we present a few of 
# the techniques readily available to you in your scripts--and often have
# the most dramatic effects on runtime performance.

# Note this script has no `spark.stop()` method.  We'll run it four times, 
# each time launching a new session, then running the entire script and timing 
# the run.

# For each run, we'll make a slight change. The four runs:

# 1. Run the script as is.  
# There are three actions at the bottom of the script--and you may not want 
# to wait for all three actions to complete.
# 2. Edit line 60 and change `local` to `yarn`, then launch a new session
# and run again.  
# This runs the application in the Hadoop cluster rather than locally.
# Notice the change in setup time.  
# If you like, monitor the application in the Spark Application WebUI.
# 3. For another session, uncomment line 69, to partition the rides 
# DataFrame. 
# This enables the Hadoop cluster to run your program with more parallelism.  
# Look at the stage progress bar during the run, to see evidence of growing 
# parallelism.
# Then visit the Spark application Web UI and view the timeline.  
# This shows commonly configured YARN behavior called *dynamic resource 
# allocation*, allowing your application to request executors as needed.  
# The number of executors you are allowed is subject to resource and 
# administered limits, including allowance for changing load conditions.  
# What happens when your application sits idle for more than a minute?
# 4. One more session: Uncomment lines 92-93, to request caching of your 
# result1 DataFrame.  
# DataFrames normally exist only as execution plans that run whenever you 
# call an action to get some result.  
# The `persist` method causes a DataFrame--the next time it is requested--to 
# be materialized and cached, so that its state is readily available 
# for reuse (to the extent that the cache is complete).

# The `DataFrame.persist()` method can be called with several different 
# `StorageLevel`s, including:
# * MEMORY_ONLY -- Cache the partitions of the DataFrame in available runtime
# memory (either the local container, or the executors on the cluster).
# * MEMORY_AND_DISK -- Cache in memory as far as possible; spill partitions 
# to temporary disk files if memory is no longer sufficient.
# (This is the default.  The `DataFrame.cache()` method is equivalent to 
# `persist` with this default.)
# * DISK_ONLY -- Store the partitions entirely on disk and read them from 
# there when needed for reuse.

# Note there is also a `DataFrame.unpersist()` method, to release a DataFrame
# from cache (presumably to make room for some other DataFrame in cache space).

# Setup
from pyspark.sql import SparkSession
spark = SparkSession\
  .builder\
  .config("spark.app.name", "tune")\
  .config("spark.master", "local")\
  .getOrCreate()
  
# Get the Application Web UI URL for browsing.
spark.sparkContext.uiWebUrl

# Read drivers and rides datasets
drivers = spark.read.csv("/duocar/raw/drivers/", inferSchema=True, header=True)
rides = spark.read.csv("/duocar/raw/rides/", inferSchema = True, header = True)
# rides = rides.repartition(27)
  
# Explode the size of the rides DataFrame
multiplier = spark.range(5000)
rides = rides.crossJoin(multiplier)

# Join to get rider_id, driver_id, and driver's full name
from pyspark.sql.functions import concat_ws  
driver_riders = rides\
  .join(drivers, rides.driver_id == drivers.id)\
  .withColumn("driver_name",\
              concat_ws(", ", drivers.last_name, drivers.first_name))\
  .select("driver_id", "driver_name", "rider_id")

# Aggregate to find for each driver the number of distinct riders carried
from pyspark.sql.functions import col
result1 = \
  driver_riders\
  .distinct()\
  .groupBy("driver_id", "driver_name")\
  .count().withColumn("distinct_riders", col("count"))\
  .select("driver_id", "driver_name", "distinct_riders")
  
# from pyspark import StorageLevel
# result1.persist(StorageLevel.MEMORY_ONLY)

# How many distinct riders (customers) for each driver.  Order by most riders first.
result1.orderBy("distinct_riders", ascending=False).show(10, truncate=False)

# Same data, but order by driver name
result1.orderBy("driver_name", ascending=True).show(10, truncate=False)

# What is the mean number of distinct riders per driver?  ...and other basics.
result1.select("distinct_riders").describe().show()


# ## Exercises

# Look in the documentation for the explanation of the `repartition` and 
# `coalesce` methods on DataFrames.

# If you configure a SparkSession with master set to `local[2]`, you get
# a local run, but using two runtime threads to get some degree of 
# parallelism.  Leaving lines 69, 92, and 93 uncommented, change the session
# to run with this setting and compare run time.


# ## References

# [Tuning Spark](http://spark.apache.org/docs/latest/tuning.html), but note 
# that the Catalyst optimizer embedded in Spark SQL and DataFrames works at 
# a more sophisticated level than some of the lower-level RDD tuning 
# techniques presented here.

# [DataFrame functions-Repartition,Coalesce,Persist,Cache,Unpersist](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame)
