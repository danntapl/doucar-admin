# # Monitoring Spark Applications

# Copyright © 2010–2017 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.

# Recall that, for Spark:
# * Underlying every DataFrame is an *RDD* (Resilient Distributed 
# Dataset)--the basic programming abstraction of Spark programs. An RDD
# can be thought of as a DataFrame stripped of its schema and schema-related 
# functions.
# * An RDD--and thus a DataFrame--is typically divided into distinct subsets of
# records called *partitions*.  Partitions can be handled by separate processes
# in the distributed cluster environment--thus they are the fundamental unit
# of parallelism in your Spark programs.
# * An *application* is a single SparkSession (and its underlying SparkContext).
# A typical script runs only one Spark application.
# * A *job* is actual data processing activity in the runtime system triggered
# by an *action* to produce a result.  Common actions include `show`,
# `write`, and `count` (applied to a DataFrame).  A single application may
# have many jobs.
# * A *stage* is a part of a job that is *embarrassingly parallel*: it can be fully
# accomplished in separate *tasks* that run independently in the 
# distributed environment.  There is a direct 1-to-1 relationship between the
# number of partitions in a DataFrame, and the number of tasks needed to process
# that DataFrame.
# * Some transformations that take one DataFrame to another are *narrow*: they are 
# combined together in a single stage.  Examples of narrow operations are `select`,
# `filter`, `withColumn`.  Any string of narrow transformations will remain
# in the same partition scheme with one another.
# * Some tranformations are *wide*: they will trigger a (somewhat costly) 
# reorganization of all records in a DataFrame into a new DataFrame.  Examples
# are `groupBy` and `orderBy`.  

# ## Setup

# Create a local SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession\
  .builder\
  .config("spark.app.name", "monitor")\
  .config("spark.master", "local")\
  .getOrCreate()
  
# If you create a SparkSession with local master, then it does not run on the
# Hadoop cluster, and will not appear in the Hue Job Browser.

# You can see the operation of your Spark application by browsing directly
# to the Spark Application WebURL

drivers = spark.read.csv("/duocar/raw/drivers/", header=True, inferSchema=True)
rides = spark.read.csv("/duocar/raw/rides/", header=True, inferSchema=True)

# Split rides into multiple partitions to see Spark runtime effects
rides = rides.repartition(35)

# Notice that each `read.csv` generates 3 tiny research jobs.

# Project driver_id, driver name, and rider_id
from pyspark.sql.functions import concat_ws  
driver_riders = rides\
  .join(drivers, rides.driver_id == drivers.id)\
  .withColumn("driver_name",\
              concat_ws(", ", drivers.last_name, drivers.first_name))\
  .select("driver_id", "driver_name", "rider_id")
  
# Check the number of partitions per DF:

drivers.rdd.getNumPartitions()
rides.rdd.getNumPartitions()
driver_riders.rdd.getNumPartitions()
# This last check actually produces a tiny job to consider the number of
# partitions in the yet-to-be-realized join.

# First action--get a count:

driver_riders.count()

# In the Spark Job UI:
# * View the jobs list, note number of tasks
# * Click on the count job link, note 3 stages
# * Click on DAG visualization and presentation of 3 stages
# * Click on the middle stage and see the 35 tasks

# Another job: this one with a groupBy

result1 = \
  driver_riders\
  .groupBy("driver_id")\
  .count()\
  .orderBy("count", ascending=False)
  
!hdfs dfs -rm -r practice/result1
result1.write.csv("practice/result1")

# driver_riders.persist(StorageLevel.MEMORY_ONLY)

# How many distinct riders (different customers), has each driver served?
# Report driver name, and distinct riders, ordered by the driver with the
# most distinct riders first.
from pyspark.sql.functions import col
driver_riders\
  .distinct()\
  .groupBy("driver_id", "driver_name")\
  .count().withColumn("distinct_riders", col("count"))\
  .select("driver_name", "distinct_riders")\
  .orderBy("distinct_riders", ascending=False)\
  .show(10)
  
# Same data, but report alphabetical by driver last_name, first_name.
driver_riders\
  .distinct()\
  .groupBy("driver_id", "driver_name")\
  .count().withColumn("distinct_riders", col("count"))\
  .select("driver_name", "distinct_riders")\
  .orderBy("driver_name", ascending=True)\
  .show(10)
  
spark.stop()

# Now create a distributed application in YARN:

spark = SparkSession\
  .builder\
  .config("spark.app.name", "monitor2")\
  .config("spark.master", "yarn")\
  .getOrCreate()


# The Spark Application UI is still presented, but now your application is 
# also visible in the Hue Job Browser.  Click on the job ID, and then the 
# "Jobs" URL to see the same Spark Application UI.
  
drivers = spark.read.csv("/duocar/raw/drivers/", header=True, inferSchema=True)
rides = spark.read.csv("/duocar/raw/rides/", header=True, inferSchema=True)
rides = rides.repartition(35)

# In the Application UI, click on the Event Timeline.  Notice that an executor
# was created in the cluster on your behalf in order to serve the tiny jobs 
# from your `read.csv` commands.  If you wait another minute, then the now-idle
# executor will be killed.  This destruction and creation of executors based
# on the ebb and flow of demand is YARN's dynamic resource allocation.

from pyspark.sql.functions import concat_ws  
driver_riders = rides\
  .join(drivers, rides.driver_id == drivers.id)\
  .withColumn("driver_name",\
              concat_ws(", ", drivers.last_name, drivers.first_name))\
  .select("driver_id", "driver_name", "rider_id")
  
# First action--get a count:

driver_riders.count()

# Another job:

result1 = \
  driver_riders\
  .groupBy("driver_id")\
  .count()\
  .orderBy("count", ascending=False)
  
!hdfs dfs -rm -r practice/result1
result1.write.csv("practice/result1")

# Note, if you pay attention to the session output, you will see animated text
# lines appearing similar to this:

# [Stage 22:==========>  (162 + 15) / 200]+----------------+---------------+

# A line like that means that, at the moment, Stage 22 of the application is
# currently running.  It consists of 200 tasks, of which 162 are completed, and
# 15 are currently running.  (Each task is running in a separate executor or
# --if local--a separate thread.)

# A third job:

driver_riders\
  .distinct()\
  .groupBy("driver_id", "driver_name")\
  .count().withColumn("distinct_riders", col("count"))\
  .select("driver_name", "distinct_riders")\
  .orderBy("distinct_riders", ascending=False)\
  .show(10)
  
# A fourth job:

driver_riders\
  .distinct()\
  .groupBy("driver_id", "driver_name")\
  .count().withColumn("distinct_riders", col("count"))\
  .select("driver_name", "distinct_riders")\
  .orderBy("driver_name", ascending=True)\
  .show(10)
  
spark.stop()

