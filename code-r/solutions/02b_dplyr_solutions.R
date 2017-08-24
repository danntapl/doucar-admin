# # sparklyr and dplyr

# Sample answers.  You can run this standalone, or execute selected statements 
# in a wider session.


# ## Setup

library(sparklyr)
library(dplyr)

config <- spark_config()
config$spark.driver.host <- Sys.getenv("CDSW_IP_ADDRESS")
spark <- spark_connect(
  master = "local",
  app_name = "dplyr",
  config = config
)


# ## Exercises

# Examine the structure of the drivers table.

# Load the table (if not already loaded)
tbl_change_db(spark, "duocar")
drivers <- tbl(spark, "drivers")

# Examine
drivers %>% glimpse()


# Collect the first 100 rows of the drivers table into 
# a data frame tbl.

drivers_100 <- drivers %>% head(100)
drivers_100_tbl <- drivers_100 %>% collect()


# Show the SQL query that sparklyr executed to do this.

drivers_100 %>% show_query()


# Create a character vector containing the names of the 
# tables in the duocar database.

tbl_change_db(spark, "duocar")
tnames <- src_tbls(spark)

# Verify
tnames


# Show two different ways to create a character vector
# containing the makes of all the drivers' vehicles,
# one using `collect()`, the other using `pull()`.

# (1) using collect() -- More heavy weight
makes0 <- drivers %>% collect() %>% as.data.frame()
makes <- makes0[,"vehicle_make"]

# (2) using pull() -- More light weight: fetches just the one column
makes <- drivers %>% pull(vehicle_make)


# Name this character vector `makes` and run the following code:

makes_counts <- table(makes)
makes_df <- data.frame(
  make = names(makes_counts),
  count = as.numeric(makes_counts)
)
makes_df <- makes_df[makes_df$count > 20, ]

library(ggplot2)
ggplot(makes_df, aes(x = make, y = count)) + geom_col()


# What are the top three vehicle makes among DuoCar drivers?

# Reading the bar graph: Ford, Toyota, Chevrolet, in that order.


# ## Cleanup

spark_disconnect(spark)

