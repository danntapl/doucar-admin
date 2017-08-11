# # Connecting to Spark

# In this module we demonstrate how to run Spark in local mode (on the CDSW
# cluster) and in cluster model (on the Hadoop cluster via YARN).


# ## Creating a SparkSession

# A
# [SparkSession](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.SparkSession)
# is the entry point to Spark SQL.

from pyspark.sql import SparkSession

# Use the `getOrCreate` method of the `builder` class to create a SparkSession:

spark = SparkSession.builder.appName("connect").getOrCreate()

# Notes:

# * The optional `appName` method changes the default name of the SparkSession.

# * An underlying `SparkContext` is created with the SparkSession--it is your Spark "runtime".

# * The note from the console about setting logging level is incorrect for your
# session that creates a SparkSession explicitly.  To adjust the logging level
# use `spark.SparkContext.setLogLevel('newLevel')`.  Legal values are:
#    * TRACE
#    * DEBUG
#    * INFO
#    * WARN
#    * ERROR
#    * FATAL
#    * OFF

# Use the `version` method to get the version of Spark.

spark.version

# Use the `stop` method to end the `SparkSession`.

spark.stop()

# This actually stops the underlying `SparkContext`.

# The `SparkSession` can connect to Spark in local mode or cluster mode.  The
# default behavior in CDSW is cluster mode.


# ## Connecting to a local Spark instance

# A local Spark instance is a single local process, and is useful for
# developing and debugging code on small datasets.  To connect to a local Spark
# instance running on the CDSW node, use `master("local")`:

# Create a SparkSession:
spark = SparkSession.builder.master("local").appName("connect-local").getOrCreate()

# Create a Spark DataFrame:
df = spark.createDataFrame([("brian", ), ("glynn", ), ("ian", )], schema=["team"])

# Print the schema:
df.printSchema()

# View the DataFrame:
df.show()

# Stop the SparkSession:
spark.stop()


# ## Connecting to a Spark cluster via YARN

# To connect to a Spark cluster via YARN, use `master("yarn")`:

spark = SparkSession.builder.master("yarn").appName("connect-yarn").getOrCreate()

# **Note**: The default value for master is `"yarn"`.


# ## Viewing the Spark Job UI

# Create a simple Spark job to exercise the Spark Job UI:
df = spark.range(1000000)
df.count()

# There are at least four methods for viewing your Spark Job UI.  Here are two.

# **Method 1:** Use the `uiWebUrl` method:

spark.sparkContext.uiWebUrl

# Paste this URL into your browser to view the Spark Job UI.

# **Method 2:** Use the Hue Job Browser for Spark Applications on the cluster.

# See also [Accessing Spark 2 Web UIs from Cloudera Data Science
# Workbench](https://www.cloudera.com/documentation/data-science-workbench/latest/topics/cdsw_spark_ui.html#cdsw_spark_ui).


# ## Viewing the Spark History Server UI

# You can open the Spark History Server UI directly from CDSW.

# Stop the SparkSession:
spark.stop()


## Exercises

# Create a SparkSession that connects to Spark in local mode.  Configure the SparkSession to use two cores.  

# Create a small DataFrame.  Print the schema.  View the DataFrame.  Count the number of records.

# Explore the Spark Job UI.

# Stop the SparkSession.

# Explore the Spark History Server UI.


## References

# [Using Cloudera's Distribution of Apache Spark
# 2](https://www.cloudera.com/documentation/data-science-workbench/latest/topics/cdsw_dist_comp_with_Spark.html)
