# # Reading and Writing Data


# Sample answers.  You can run this standalone, or execute selected statements 
# in a wider session.


# ## Setup

library(sparklyr)

# Connect local session
config <- spark_config()
config$spark.driver.host <- Sys.getenv("CDSW_IP_ADDRESS")
spark <- spark_connect(
  master = "local",
  app_name = "read-solutions",
  config = config
)

# Reset practice directory in HDFS
system("hdfs dfs -rm -r -skipTrash practice")
system("hdfs dfs -mkdir practice")


# ## Exercises

# Read the raw drivers file into a Spark DataFrame.

# View a few records to verify the file format (could also be done in Hue)
system("hdfs dfs -cat /duocar/raw/drivers/* | head -3")

# Comma-delimited, header present.  Read the csv file.
drivers <- spark_read_csv(
  sc = spark,
  name = "drivers",
  path = "/duocar/raw/drivers/",
  header = TRUE,
  infer_schema = TRUE,
  delimiter = ",")

# Verify
drivers


# Save the drivers DataFrame as a JSON file in your CDSW practice directory.

# (guessing a syntax similar to the one for parquet)
spark_write_json(
  drivers,
  path = "practice/drivers_json")

# Verify (could also be viewed in Hue)
system("hdfs dfs -cat practice/drivers_json/* | head -3")


# Read the drivers JSON file into a Spark DataFrame.

# (guessing syntax similar to the one for parquet)
drivers_json <- spark_read_json(
  sc = spark,
  name = "drivers_json",
  path = "practice/drivers_json")


# Delete the JSON file.

# Cleanup directory from HDFS
system("hdfs dfs -rm -r -skipTrash practice/drivers_json")


# ## Cleanup

# Stop the `SparkSession`
spark_disconnect(spark)
