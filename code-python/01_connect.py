# # Connecting to Spark

# In this module we demonstrate how to run Spark locally within our CDSW
# container and how to run Spark on our Hadoop cluster via YARN.  The default
# behavior in CDSW is to run Spark on a Hadoop cluster via YARN.


# ## Running Spark locally within the CDSW container

# We can run Spark locally within our CDSW container.  This allows us to
# develop and test our code on a small sample of data before we run the code on
# the Hadoop cluster.

# We begin by creating a
# [SparkSession](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.SparkSession),
# which is the entry point to Spark SQL:
from pyspark.sql import SparkSession

# Use the `getOrCreate` method of the `builder` class to create a SparkSession:
spark = SparkSession.builder \
  .master("local") \
  .appName("connect-local") \
  .getOrCreate()

# Notes:

# * The call to `master("local")` directs Spark to run the application locally.
# A local Spark instance is a single process running within our CDSW container.

# * The optional `appName` method changes the default name of the SparkSession.

# * An underlying `SparkContext` is created with the SparkSession--it is our
# Spark "runtime".

# * The note in the console about setting logging level is incorrect for our
# session that creates a SparkSession explicitly.  Use
# `spark.SparkContext.setLogLevel(newLevel)` to adjust the logging level.  We
# will discuss logging in more detail later.

# Use the `version` method to get the version of Spark:
spark.version

# Use the `createDataFrame` method to create a Spark DataFrame:
df = spark.createDataFrame([("brian", ), ("glynn", ), ("ian", )], schema=["team"])

# **Note:** We must pass in a Python list of tuples.  Each tuple represents a record.

# Use the `printSchema` method to print the schema:
df.printSchema()

# Use the `show` method to view the DataFrame:
df.show()

# Use the `stop` method to end the `SparkSession`:
spark.stop()

# **Note:** This actually stops the underlying `SparkContext`.


# ## Running Spark on a Hadoop cluster via YARN

# To connect to a Hadoop cluster via YARN, use `master("yarn")`:
spark = SparkSession.builder \
  .master("yarn") \
  .appName("connect-yarn") \
  .getOrCreate()

# **Note**: The default value for master is `"yarn"`.  In CDSW, Spark runs in `client` mode.


# ## Viewing the Spark UI

# Create a simple Spark job to exercise the Spark UI:
df = spark.range(1000000)
df.count()

# There are various ways to view the internal details of your Spark 
# applications.  Here are a few.

# **Method 1:**
# To view the Spark UI, choose `Spark UI` from the grid pulldown menu in the 
# upper-right of your workbench.  **Note**, the Spark UI is presented only
# while you have a SparkSession running.

# **Method 2:** 
# If your application runs in the Hadoop cluster with `master("yarn")`, 
# then you can view details of your application using
# the Hue Job Browser for Spark Applications on the cluster.
# (Hue shows information from the Hadoop cluster, and so will not
# present details of Spark appications that run locally in a CDSW container.)

# Stop the SparkSession:
spark.stop()


# ## Viewing the Spark History Server UI

# Once your SparkSession has completed (with `spark.stop()`) you can open the
# Spark History Server UI to view details of the session run.  Invoke the Spark
# History Server by choosing `Spark History` from the grid pulldown menu in the
# upper-right of the workbench.


# ## Exercises

# (1) Create a SparkSession that connects to Spark in local mode.  Configure
# the SparkSession to use two cores.  

# (2) Create a small DataFrame.  Print the schema.  View the DataFrame.  Count
# the number of records.

# (3) Explore the Spark UI.

# (4) Stop the SparkSession.

# (5) Explore the Spark History Server UI.


# ## References

# [Using Cloudera's Distribution of Apache Spark
# 2](https://www.cloudera.com/documentation/data-science-workbench/latest/topics/cdsw_dist_comp_with_Spark.html)

# [Spark Overview](http://spark.apache.org/docs/latest/index.html)

# [Spark Python API](http://spark.apache.org/docs/latest/api/python/index.html)
