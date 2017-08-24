# # Inspecting Data

# ## Setup

library(sparklyr)
library(dplyr)

config <- spark_config()
config$spark.driver.host <- Sys.getenv("CDSW_IP_ADDRESS")
spark <- spark_connect(
  master = "local",
  app_name = "inspect",
  config = config
)

# ## Load the riders data from HDFS into a Spark DataFrame

riders <- spark_read_csv(
  sc = spark,
  name = "riders",
  path = "/duocar/raw/riders/"
)


# ## Viewing some data and examining the schema

# Print the `tbl_spark` to see the names and types 
# of the columns and the first 10 rows of data, 
# for as many columns as fit on the screen

riders

# To get the first *`x`* rows for some other value 
# of *`x`*, use `head()`.
# The default number of rows is 6.

riders %>% head()

riders %>% head(5)

riders %>% head(20)


# To get a vector of the column names, use `colnames()`:

colnames(riders)

# Or with the pipe operator `%>%`:

riders %>% colnames()

# There are other styles of using the pipe operator, 
# but the one above is preferable.

# ok:
riders %>% colnames
riders %>% colnames(.)

# better:
riders %>% colnames()


# ## Counting the number of rows and columns

# To get the number of rows and columns, use 
# `sdf_nrow()` and `sdf_ncol()`. "sdf" stands for 
# "Spark DataFrame".

riders %>% sdf_nrow()
riders %>% sdf_ncol()

# Or use `sdf_dim()` to get a vector of both:

riders %>% sdf_dim()


# ## Inspecting a column (variable)

# To select one or more columns, use `select()`:

riders %>% select(first_name)

riders %>% select(first_name, last_name)


# To select the distinct values of one or more columns, 
# use `distinct()`:

riders %>% distinct(first_name)

riders %>% distinct(first_name, last_name)


# You can also use `sdf_nrow()`, `sdf_ncol()`, or 
# `sdf_dim()` after operations like these:

riders %>% 
  distinct(first_name, last_name) %>% 
  sdf_dim()


# But to go beyond these simple operations, we need 
# to learn more about *dplyr verbs* and how they can 
# be used to manipulate data. This is the subject 
# of the next module.


# ## Exercises

# Read the drivers data and view the first 5 rows.

# What are the names of the columns in the drivers data?

# How many rows are in the drivers data?

# Select only the `vehicle_make` and `vehicle_model` columns
# from the drivers data.

# How many different vehicle makes are in the drivers data?


# ## Cleanup

spark_disconnect(spark)
