# # Inspecting Data

# ## Setup

library(sparklyr)
library(dplyr)

spark <- spark_connect(master = "local", app_name = "inspect")


# ## Load the riders data from HDFS into a Spark DataFrame

riders <- spark_read_csv(
  sc = spark,
  name = "riders",
  path = "/duocar/raw/riders/"
)


# ## Viewing some data and examining the schema

# Print the `tbl_spark` to see the names and types of the columns
# and the first 10 rows of data, for as many columns as fit on the screen

riders

# To get the first *`x`* rows for some other value of *`x`*, use `head()`.
# The default number of rows is 6.

riders %>% head()

riders %>% head(5)

riders %>% head(20)


# To get a vector of the column names, use `colnames()`:

colnames(riders)

# Or with the pipe operator `%>%`:

riders %>% colnames()

# There are other styles of using the pipe operator, but the one above is preferable.

# ok:
riders %>% colnames
riders %>% colnames(.)

# better:
riders %>% colnames()


# ## Counting the number of rows and columns

# To get the number of rows or columns, use `nrow()` or `ncol()`:

riders %>% nrow()
riders %>% ncol()

# Or use `dim()` to get a vector of both:

riders %>% dim()


# ## Inspecting a column (variable)

# To select one or more columns, use `select()`:

riders %>% select(first_name)

riders %>% select(first_name, last_name)


# To select the distinct values of one or more columns, use `distinct()`:

riders %>% distinct(first_name)

riders %>% distinct(first_name, last_name)


# You can also use `nrow()`, `ncol()`, or `dim()` after these:

riders %>% 
  distinct(first_name, last_name) %>% 
  dim()


# But to go beyond these simple operations, we need to learn more 
# about *dplyr verbs* and how they can be used to manipulate data.
# This is the subject of the next module.


# ## Cleanup

spark_disconnect(spark)
