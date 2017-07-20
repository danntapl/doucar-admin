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


# TBD: do something trivial here to show Spark is working


# Disconnect from Spark

spark_disconnect(spark)


# ## Connecting to a Spark cluster via YARN

spark <- spark_connect(master = "yarn", app_name = "connect-yarn")


# TBD: do something trivial here to show Spark is working


# Disconnect from Spark

spark_disconnect(spark)