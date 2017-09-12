# # sparklyr and dplyr

# In this module we describe
# * How sparklyr works together with dplyr
# * How sparklyr interacts with Spark by issuing SQL queries
# * How to return Spark tables to R as local data frames

# ## Setup

library(sparklyr)
config <- spark_config()
config$spark.driver.host <- Sys.getenv("CDSW_IP_ADDRESS")
spark <- spark_connect(
  master = "local",
  app_name = "dplyr",
  config = config
)

# ## Remote Spark DataFrames

# In the last module, we saw how sparklyr returns `tbl_spark` objects
# which represent remote Spark DataFrames.
# It is not loaded in R's memory.
# Only the first few rows of data are shown.

riders <- spark_read_csv(
  sc = spark,
  name = "riders",
  path = "/duocar/raw/riders/"
)

riders

class(riders)


# Let's rename riders to `riders_tbl_spark` so we remember what type of object it is

riders_tbl_spark <- riders
rm(riders)


# We will learn throughout this training about how sparklyr and dplyr allow you to do many things with this `tbl_spark` object
# all without loading the full data into R

# But some functions require that the data be loaded in R
# For example, the functions in graphics packages like ggplot2 and leaflet
# These do not work with `tbl_spark` objects.
# For example, this fails:

#``` r
#library(leaflet)
#
#leaflet(riders_tbl_spark) %>%
#  addTiles() %>%
#  addCircles(lng = ~home_lon, lat = ~home_lat) # fails
#```

# Before we see how to make this succeed, let's first look more at this `riders_tbl_spark` object.
# We know that it is a `tbl_spark` object,
# but it also inherits some other classes:

class(riders_tbl_spark)

# Notice this object also inherits `tbl_sql`, `tbl_lazy`, and `tbl`.
# these are classes defined by the packages `tibble` and `dplyr`.
# `tbl` is an underlying class defined by the `tibble` package
# `tbl_lazy` means that R uses lazy evaluation on this object; it does not immediately perform operations but waits until the result is requested
# `tbl_sql` means that R can perform operations on this object by issuing SQL queries which are processed by some SQL backend (Spark SQL in this case)

# This work of translating commands into SQL is done by dplyr, with help from sparklyr
# and also by the package dbplyr which is a general database backend for dplyr.
# sparklyr imports dplyr and dbplyr, but to do many things, you need to load dplyr yourself:

library(dplyr)


# With dplyr loaded, you can use many functions that are not available just in sparklyr
# One useful one is `show_query()`.
# This shows the SQL query that would be issued to Spark to create this object

riders_tbl_spark %>% show_query()

# This is a very simple SQL query just representing the whole table
# We'll see later that as you apply other operations on this table, it changes what SQL dplyr executes:

riders_tbl_spark %>% head(5) %>% show_query()


# `head()` is a very simple operation
# (We will learn about many other operations later)
# Actually, when you print a `tbl_spark`, dplyr automatically applies `head(10)` in order to display only the first 10 rows:

riders_tbl_spark # This is the same as: `print(riders_tbl_spark)`


# Remember that this object doesn't store any of that data in R's memory
# It just prints a few lines to the screen


# ## Returning Data to R

# So how *do* you make Spark execute the SQL command 
# that we saw in `show_query()` and return 
# the *full* result to R?
# By using the `collect()` function:

riders_tbl_df <- riders_tbl_spark %>% collect()

# Let's look at the resulting object:

riders_tbl_df

# Important: All the data in this is stored in memory in R!
# So you should only use `collect()` if the table is small enough to fit in R's memory on the computer you're using!

# This object is no longer a `tbl_spark`.
# It's now a `tbl_df`, also called a "data frame tbl" or simply a "tibble"

class(riders_tbl_df)

# Notice how this object also inherits `data.frame`.
# It *is* actually a data frame, but it has the `tbl_df` and `tbl` classes also
# which help make the object do things like print more nicely.

# You can make the object just a raw `data.frame` by calling `as.data.frame()`:

riders_df <- as.data.frame(riders_tbl_df)

# Then the output when you print that is different
# and takes more time to render in CDSW:

riders_df

# So generally we keep these as `tbl_df` objects.

rm(riders_df)


# Now with this `tbl_df`, you can use leaflet, and it succeeds:

library(leaflet)

leaflet(riders_tbl_df) %>%
  addTiles() %>%
  addCircles(lng = ~home_lon, lat = ~home_lat)


# ## Working with Local Data Frames and Remote Spark DataFrames

# It's not just graphics functions that require you to `collect()` the data before using them;
# Many of the usual functions that work on data frames do not work on `tbl_spark` objects.
# (they may fail or return unexpected output)

# But all the usual functions that work on data.frame objects do work on `tbl_df` objects.
# Because remember that `tbl_df` objects *are* data frames.

# For example, you cannot use `$` or `[]` to pull a column from a `tbl_spark`: 

#```r
#riders_tbl_spark$student # returns NULL
#
#riders_tbl_spark[, "student"] # fails
#```

# But you can do this on a `tbl_df`:

riders_tbl_df$student %>% table()

riders_tbl_df[, "student"]

# And you cannot use `str()` to examine the structure of a `tbl_df`
# (it returns a mess of technical info, not the column structure)

#```r
#riders_tbl_spark %>% str() # returns non-useful output
#```

# But you can do this on a `tbl_df`: 
riders_tbl_df %>% str()


# However, for many operations, dplyr provides its own ways to work with `tbl_spark` objects
# So you don't have to call `collect()` to do simple things.

# For example, instead of using `$` to get a single column,
# dplyr provides the function `pull()` which collects just the one column:

riders_tbl_spark %>% pull(student)

# And instead of using `str()` to see the structure, you can use `glimpse()`:

riders_tbl_spark %>% glimpse()

# We will learn about many more such functions in the remainder of the course.


# ## dplyr Functions for Spark Connection Objects

# dplyr also provides some special functions for working with the Spark connection object.

# One example is `src_tbls()` which lists the tables 

src_tbls(spark)

# Another example is the function `tbl()` which we saw previously.
# You can use this to issue a SQL query directly:

flights <- tbl(spark, sql("SELECT * FROM flights"))

# Or you can use it to reference a table by name:

flights <- tbl(spark, "flights")


# Sometimes you need to use a table that's in a non-default database.
# To see what databases are available, call the `src_databases()` function:

src_databases(spark)

# Then use the function `in_schema()` in the dbplyr package to qualify which database a table is in.
# To avoid loading the dbplyr package, you can use `dbplyr::` before the function call:

drivers <- tbl(spark, dbplyr::in_schema("duocar", "drivers"))

# Another way to use a table that's in a non-default database is to change the current database.
# You can do this using the `tbl_change_db()` function:

tbl_change_db(spark, "duocar")

# Then you can refer to tables in the duocar database without using `in_schema()`:

drivers <- tbl(spark, "drivers")

# Then switch back to the default database:

tbl_change_db(spark, "default")


# For the rest of the course, we will keep dplyr loaded at all times.
# You should generally do this whenever you're using sparklyr.


# ## Exercises

# Examine the structure of the drivers table.

# Collect the first 100 rows of the drivers table into 
# a data frame tbl.

# Show the SQL query that sparklyr executed to do this.

# Create a character vector containing the names of the 
# tables in the duocar database.

# Show two different ways to create a character vector
# containing the makes of all the drivers' vehicles,
# one using `collect()`, the other using `pull()`.

# Name this character vector `makes` and run the following code:

#```r
#makes_counts <- table(makes)
#makes_df <- data.frame(
#  make = names(makes_counts),
#  count = as.numeric(makes_counts)
#)
#makes_df <- makes_df[makes_df$count > 20, ]
#
#library(ggplot2)
#ggplot(makes_df, aes(x = make, y = count)) + geom_col()
#```

# What are the top three vehicle makes among DuoCar drivers?


# ## Cleanup

spark_disconnect(spark)
