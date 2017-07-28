# # Explore the Catalog Interface

# Create a SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").enableHiveSupport().appName("catalog").getOrCreate()

# **Note:** I do not seem to have to enable Hive support for all this to work.

# Load the joined rides data:
joined = spark.read.parquet("/duocar/joined/")

# List databases:
spark.catalog.listDatabases()

# List current database:
spark.catalog.currentDatabase()

# List tables in current database and temporary tables:
spark.catalog.listTables()

# Create a temporary table:
joined.createOrReplaceTempView("joined_tmp")
spark.catalog.listTables()
spark.sql("describe joined_tmp").show()

# Create internal (managed) table from DataFrame:
joined.write.saveAsTable("joined_int", mode="overwrite")
spark.catalog.listTables()
spark.sql("describe joined_int").show()

# Create external table from Parquet file:
joined = spark.catalog.createExternalTable("joined_ext", "/duocar/joined/")
spark.catalog.listTables()
spark.sql("describe joined_ext").show()

# List columns of table:
spark.catalog.listColumns("joined_int")

# **Note:** This does not seem to work with temporary tables.

# Cache temporary table:
spark.catalog.isCached("joined_tmp")
spark.catalog.cacheTable("joined_tmp")
spark.catalog.isCached("joined_tmp")
spark.catalog.uncacheTable("joined_tmp")
spark.catalog.isCached("joined_tmp")

# Drop temporary table:
spark.catalog.dropTempView("joined_tmp")
spark.catalog.listTables()

# Drop internal table:
spark.sql("drop table if exists joined_int")
spark.catalog.listTables()

# Drop external table:
spark.sql("drop table if exists joined_ext")
spark.catalog.listTables()

# List functions:
spark.catalog.listFunctions()

# Stop the SparkSession:
spark.stop()

# -----

# Hive does not support Date types.
from pyspark.sql.functions import col
joined2 = joined\
  .withColumn('driver_birth_date', col("driver_birth_date").cast("timestamp"))\
  .withColumn('driver_start_date', col("driver_birth_date").cast("timestamp"))\
  .withColumn('rider_birth_date', col("driver_birth_date").cast("timestamp"))\
  .withColumn('rider_start_date', col("driver_birth_date").cast("timestamp"))