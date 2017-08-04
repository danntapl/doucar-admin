# # Configuring, monitoring, and tuning Spark Applications


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('config').master('local').getOrCreate()


# ## Configuring Spark Applications


# ## Monitoring Spark Applications


# ## Tuning Spark Applications

# ### Persisting DataFrames

# `cache`, `persist`, and `unpersist`

# ### Partitioning DataFrames

# `partition`  and `coalesce` methods


# ## Exercises


# ## Cleanup

# Stop the SparkSession:
spark.stop()


# ## References

# [Spark Configuration](http://spark.apache.org/docs/latest/configuration.html)

# [Monitoring and Instrumentation](http://spark.apache.org/docs/latest/monitoring.html)

# [Tuning Spark](http://spark.apache.org/docs/latest/tuning.html)
