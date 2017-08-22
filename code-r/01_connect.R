# # Connecting to Spark

# TBD: document rationale for sparklyr over SparkR

# ## Installing sparklyr

# Warning: This takes several minutes the first time you run it.

install.packages("sparklyr")

library(sparklyr)


# ## Connecting to a local Spark instance

# Use the `spark_connect()` function to create a Spark connection

#```r
#spark <- spark_connect(master = "local", app_name = "connect-local")
#```

# The argument `master` specifies which Spark instance to connect to.
# Use `master = "local"` to connect to a local Spark instance.

# The optional `app_name` argument changes the default name of 
# the Spark session.

# In some environments, those are the only two arguments you need
# to pass to `spark_connect()`.
# But when using the local Spark instance from CDSW,
# it's also necessary to set a Spark configuration parameter.

# To set configuration parameters, first use the `spark_config()`
# function to retrieve the default configuration:

config <- spark_config()

# You can view the default configuration. It is a named list:

config

# To use local Spark from CDSW, it is necessary to modify this
# configuration list to set the Spark configuration property 
# `spark.driver.host` to the IP address of the CDSW engine 
# container. This is stored in an environment variable 
# named `CDSW_IP_ADDRESS`.

config$spark.driver.host <- Sys.getenv("CDSW_IP_ADDRESS")

# Then pass this modified `config` object as the `config` argument
# to the `spark_connect()` function:

spark <- spark_connect(
  master = "local",
  app_name = "connect-local",
  config = config
)

# Now your Spark connection is ready to be used.


# ## Using the Spark connection

# To use the connection to Spark, you pass the Spark connection 
# object to various sparklyr functions. For example:

# Use the `spark_version()` function to get the version of Spark.

spark_version(spark)

# Create a Spark DataFrame by creating an R data frame and
# copying it to Spark, then view it:

df <- sdf_copy_to(spark, data.frame(team = c("brian", "glynn", "ian")))
df

# See if the connection to Spark is still open

spark_connection_is_open(spark)

# Disconnect from Spark

spark_disconnect(spark)

# See if the connection to Spark is still open

spark_connection_is_open(spark)


# ## Connecting to a Spark cluster via YARN

# To connect to Spark on YARN, use `master = "yarn"`:

spark <- spark_connect(master = "yarn", app_name = "connect-yarn")

# If this command fails, you may need to restart your R session,
# load sparklyr, and run it again.


# ## Viewing the Spark Job UI

# Create a simple Spark job to exercise the Spark Job UI:

df <- sdf_len(spark, 10000000)
df

# Access the Spark UI through the Spark UI button in the upper right
# menu in CDSW.

# Disconnect from Spark

spark_disconnect(spark)
