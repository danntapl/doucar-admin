# # Reading and Writing Data

# ## Setup

library(sparklyr)
spark <- spark_connect(master = "local", app_name = "read")


# ## Working with delimited text files

# The rider data is a comma-delimited text file
# Use the `spark_read_csv()` function to read it into a Spark DataFrame

# ### Infer the schema

# With only the required arguments:

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

# **Note:** Do not worry about the `cat: Unable to write to output steam.` message.


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


# You can also return the result of a SQL query as a `tbl_spark`.
# To do this, you need to load dplyr and use `tbl(spark, sql(...))`.
# This will *not* return the full query result; it will only display the first few rows.

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

# Use the `copy_to()` function to copy a local R data frame to Spark

iris_tbl <- copy_to(spark, iris)

# On our class cluster, this may fail when you are connected to a local Spark instance.
# Workaround: Connect to Spark on YARN instead.

spark_disconnect(spark)
spark <- spark_connect(master = "yarn", app_name = "write")
iris_tbl <- copy_to(spark, iris)

iris_tbl

# If you remove the variable `iris_tbl` (which represents this remote Spark data frame) you can re-create it without copying the data to Spark again.
# Just use `tbl()` and reference the name (`iris`):

rm(iris_tbl)

iris_tbl <- tbl(spark, "iris")

iris_tbl

# Note: `copy_to()` *does not persist* the copied data in HDFS.
# The data is stored in a temporary location in HDFS and may be cached in Spark memory.
# After you end your session by disconnecting from Spark, it will no longer be available.


# ## Cleanup

# Remove practice directory from HDFS:

system("hdfs dfs -rm -r -skipTrash practice/riders_tsv")
system("hdfs dfs -rm -r -skipTrash practive/riders_parquet")

# Stop the `SparkSession`:

spark_disconnect(spark)
