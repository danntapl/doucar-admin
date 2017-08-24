# # Reading and Writing Data

# ## Setup

library(sparklyr)
config <- spark_config()
config$spark.driver.host <- Sys.getenv("CDSW_IP_ADDRESS")
spark <- spark_connect(
  master = "local",
  app_name = "read",
  config = config
)

#[//]: # (Instructor note:)

#[//]: # (This section describes some of the functions documented at)
#[//]: # (https://spark.rstudio.com/reference/index.html#section-spark-data)
#[//]: # (There are some other functions there that are not)
#[//]: # (described here but that students may ask about, like:)

#[//]: # (spark_read_table())
#[//]: # (which is not necessary when you are using dplyr)
#[//]: # (because you can instead use tbl())

#[//]: # (spark_write_table())
#[//]: # (which creates a permanent Hive table)
#[//]: # (which we don't want students to do yet)
#[//]: # (and is probably safer to do with PySpark than sparklyr)

#[//]: # (spark_read_source and spark_write_source)
#[//]: # (which have to do with Spark packages—a more advanced topic)

#[//]: # (spark_read_jdbc and spark_write_jdbc)
#[//]: # (which are beyond the scope of this course)

#[//]: # (In general, browsing the list of functions in sparklyr)
#[//]: # (will cause students to see a lot of content they do not)
#[//]: # (need to know about.)


# ## Working with delimited text files

# The rider data is a comma-delimited text file
# Use the `spark_read_csv()` function to read it into a Spark DataFrame

# ### Infer the schema

# Showing only the required arguments:

riders <- spark_read_csv(
  sc = spark,
  name = "riders",
  path = "/duocar/raw/riders/"
)

# Showing some more arguments, which are set to the default values:

riders <- spark_read_csv(
  sc = spark,
  name = "riders",
  path = "/duocar/raw/riders/",
  header = TRUE,
  infer_schema = TRUE,
  delimiter = ","
)

# Show the schema and first few rows of data:
riders

# The returned object (`riders` in this example) is a `tbl_spark`.

class(riders)

# This represents a remote Spark DataFrame. It is not loaded in R's memory.
# Notice that only the first few rows of data are shown.


# ### Manually specify the schema

riders2 <- spark_read_csv(
  sc = spark,
  name = "riders",
  path = "/duocar/raw/riders/",
  infer_schema = FALSE,
  columns = c(
    id = "character",
    birth_date = "character",
    start_date = "character",
    first_name = "character",
    last_name = "character",
    sex = "character",
    ethnicity = "character",
    student = "integer",
    home_block = "character",
    home_lat = "numeric",
    home_lon = "numeric",
    work_lat = "numeric",
    work_lon = "numeric"
  )
)

riders2

# Notice, in this explicit schema we override the default choices that
# Spark would have made for the datatypes of the `id` and `home_block` 
# columns.

# Write the file to a tab-delimited file:

system("hdfs dfs -rm -r -skipTrash practice")

system("hdfs dfs -mkdir practice")

spark_write_csv(
  riders,
  path = "practice/riders_tsv",
  delimiter = "\t"
)

system("hdfs dfs -ls practice/riders_tsv")

system("hdfs dfs -cat practice/riders_tsv/* | head -n 5")

# **Note:** Disregard the `cat: Unable to write to output steam.` message.


# ## Working with Parquet files

spark_write_parquet(
  riders,
  path = "practice/riders_parquet"
)

system("hdfs dfs -ls practice/riders_parquet")

# Note that the schema is stored with the data

riders_parquet <- spark_read_parquet(
  sc = spark,
  name = "riders_parquet",
  path = "practice/riders_parquet"
)

riders_parquet


# ## Working with Hive Tables

# Use the `DBI::dbGetQuery()` function to run SQL queries:

library(DBI)

dbGetQuery(spark, "SHOW DATABASES")

dbGetQuery(spark, "SHOW TABLES")

dbGetQuery(spark, "DESCRIBE airlines")

dbGetQuery(spark, "SELECT * from airlines limit 10")


# `dbGetQuery()` returns the query result to R as a data frame

airlines <- dbGetQuery(spark, "SELECT * FROM airlines")

class(airlines)

airlines

# Only use `dbGetQuery()` when the query result will be small enough to fit in memory in your R session.


# You can also manage the result of a SQL query as a `tbl_spark`.
# To do this, you need to load dplyr and use `tbl(spark, sql(...))`.

library(dplyr)

flights <- tbl(spark, sql("SELECT * FROM flights"))

class(flights)

flights


# But you need not use SQL to access Hive tables with sparklyr
# Instead you can just reference the Hive table name with `tbl()`:

flights <- tbl(spark, "flights")

# This gives exactly the same result:

class(flights)

flights

# There are more details in the next module about how sparklyr works together with dplyr


# ## Copying data frames from R to Spark

# Use the `sdf_copy_to()` function to copy a local R data frame to Spark

iris_tbl <- sdf_copy_to(spark, iris)

iris_tbl

# If you remove the variable `iris_tbl` (which represents this remote Spark data frame) you can re-create it without copying the data to Spark again.
# Just use `tbl()` and reference the name (`iris`):

rm(iris_tbl)

iris_tbl <- tbl(spark, "iris")

iris_tbl

# Note: `sdf_copy_to()` *does not persist* the copied data in HDFS.
# The data is stored in a temporary location in HDFS and may be cached in Spark memory.
# After you end your session by disconnecting from Spark, it will no longer be available.

# ## Exercises

# Read the raw drivers file into a Spark DataFrame.

# Save the drivers DataFrame as a JSON file in your CDSW practice directory.

# Read the drivers JSON file into a Spark DataFrame.

# Delete the JSON file.


# ## Cleanup

# Remove practice directory from HDFS:

system("hdfs dfs -rm -r -skipTrash practice/riders_tsv")
system("hdfs dfs -rm -r -skipTrash practive/riders_parquet")

# Stop the `SparkSession`:

spark_disconnect(spark)
