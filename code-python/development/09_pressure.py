# # Exert runtime pressure on the cluster

# Copyright © 2010–2017 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.

# Exert pressure on the cluster through:
# * Artificially repartitioning the rides DF into 35 partitions
# * Expanding the data in rides by a factor of 5000

# Note, uncommenting line 35 (caching the reused DF) slows things
# down a bit!
     

from pyspark.sql import SparkSession
from pyspark import StorageLevel
from pyspark.sql.functions import col

spark = SparkSession\
  .builder\
  .config("spark.app.name", "config")\
  .config("spark.master", "yarn")\
  .getOrCreate()
  
drivers = spark.read.csv("/duocar/raw/drivers/", inferSchema=True, header=True)

rides = spark.read.csv("/duocar/raw/rides/", inferSchema = True, header = True)\
  .repartition(35)

multiplier = spark.range(5000).repartition(10)
rides = rides.crossJoin(multiplier)

from pyspark.sql.functions import concat_ws  
driver_riders = rides\
  .join(drivers, rides.driver_id == drivers.id)\
  .withColumn("driver_name",\
              concat_ws(", ", drivers.last_name, drivers.first_name))\
  .select("driver_id", "driver_name", "rider_id")

# driver_riders.persist(StorageLevel.MEMORY_ONLY)

# How many distinct riders (different customers), has each driver served?
# Report driver name, and distinct riders, ordered by the driver with the
# most distinct riders first.
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
  
