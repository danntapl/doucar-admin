# # Connecting to Spark

# TBD: document rationale for sparklyr over SparkR

# ## Installing sparklyr

install.packages("sparklyr")

library(sparklyr)


# ## Connecting to a local Spark instance

# Use the `spark_connect()` function to create a Spark connection

spark <- spark_connect(master = "local", app_name = "connect-local")

# * The optional `app_name` argument changes the default name of the Spark session.


# Use the `spark_version()` function to get the version of Spark.

spark_version(spark)


# Create a Spark DataFrame
# (by creating an R data frame and copying it to Spark)

df <- sdf_copy_to(spark, data.frame(team = c("brian", "glynn", "ian")))

# View the Spark DataFrame

df

# See if the connection to Spark is still open

spark_connection_is_open(spark)

# Disconnect from Spark

spark_disconnect(spark)

# See if the connection to Spark is still open

spark_connection_is_open(spark)


# ## Connecting to a Spark cluster via YARN

spark <- spark_connect(master = "yarn", app_name = "connect-yarn")


# ## Viewing the Spark Job UI

# Create a simple Spark job to exercise the Spark Job UI:

df <- sdf_len(spark, 1000000)
nrow(df)

# Print the URL for the Spark Job UI
cat(spark_web(spark))


# Disconnect from Spark

spark_disconnect(spark)
