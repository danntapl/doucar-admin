# # Save Files to Hive using Spark

# **Question:** Should I set "spark.sql.hive.convertMetastoreParquet" to "false"?

# **Note:** According to this
# [known issue](https://www.cloudera.com/documentation/enterprise/release-notes/topics/cdh_rn_spark_ki.html#concept_qmr_hg5_vt),
# the `saveAsTable` method should not work, but it seems to behave as expected.

# **Note:** Hive and Impala do not support the Date type.

# **Note:** This is wasteful as it stores another copy of the data in Hive warehouse.

# Create SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession\
  .builder\
  .appName("explore_hive.py")\
  .enableHiveSupport()\
  .master("local")\
  .getOrCreate()

# Create duocar database:
spark.sql("drop database if exists duocar cascade")
spark.sql("create database duocar")
spark.sql("use duocar")

# Create (clean) ride table:
spark.sql("drop table if exists rides")
rides = spark.read.parquet("/duocar/clean/rides/")
rides.write.format("parquet").saveAsTable("rides")

# Create (clean) ride reviews table:
spark.sql("drop table if exists ride_reviews")
ride_reviews = spark.read.parquet("/duocar/clean/ride_reviews/")
ride_reviews.write.format("parquet").saveAsTable("ride_reviews")

# Create (clean) driver table:
spark.sql("drop table if exists drivers")
drivers = spark.read.parquet("/duocar/clean/drivers/")
drivers_fixed = drivers\
  .withColumn("birth_date", drivers.birth_date.cast("timestamp"))\
  .withColumn("start_date", drivers.start_date.cast("timestamp"))
drivers_fixed.write.format("parquet").saveAsTable("drivers")

# Create (clean) rider table:
spark.sql("drop table if exists riders")
riders = spark.read.parquet("/duocar/clean/riders/")
riders_fixed = riders\
  .withColumn("birth_date", riders.birth_date.cast("timestamp"))\
  .withColumn("start_date", riders.start_date.cast("timestamp"))
riders_fixed.write.format("parquet").saveAsTable("riders")

# Create (clean) joined table:
spark.sql("drop table if exists joined")
joined = spark.read.parquet("/duocar/joined/")
joined_fixed = joined\
	.withColumn("driver_birth_date", col("driver_birth_date").cast("timestamp"))\
	.withColumn("driver_start_date", col("driver_start_date").cast("timestamp"))\
	.withColumn("rider_birth_date", col("rider_birth_date").cast("timestamp"))\
	.withColumn("rider_start_date", col("rider_start_date").cast("timestamp"))
joined_fixed.write.format("parquet").saveAsTable("joined")

# Verify database and tables:
spark.sql("show databases").show()
spark.sql("show tables").show()

# Stop SparkSession:
spark.stop()