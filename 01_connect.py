# # Connecting to Spark


# ## TODO
# * Killing a Spark application?
# ## GAD Note: Maybe later in the course.  Note also: do you want to kill an *app* or a *job*?


# ## Creating a SparkSession

# A `SparkSession` is the entry point to Spark SQL.

from pyspark.sql import SparkSession

# Use the `getOrCreate` method of the `builder` class to create a SparkSession:

spark = SparkSession.builder.appName('connect').getOrCreate()

# Notes:


# * The optional `appName` method changes the default name of the SparkSession.

# * An underlying `SparkContext` is created with the SparkSession--it is your Spark "runtime".

# * The note from the console about setting logging level is incorrect for your session that creates a SparkSession explicitly.  To adjust the logging level use `spark.SparkContext.setLogLevel('newLevel')`.  Legal values are:
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

# The `SparkSession` can connect to a local instance of Spark or to a Spark cluster.


# ## Connecting to a local Spark instance

# A local Spark instance is a single local process, and is useful for developing and debugging code 
# on small datasets.  To connect to a local Spark instance running on the CDSW node, 
# use `master('local')`:

spark = SparkSession.builder.master('local').appName('connect').getOrCreate()

spark.stop()


# ## Connecting to a Spark cluster via YARN

# To connect to a Spark cluster via YARN, use `master('yarn')`:

spark = SparkSession.builder.master('yarn').appName('connect').getOrCreate()

# Note, the default value for master is `'yarn'`.


# ## Viewing the Spark Job UI

# Create a simple Spark job to exercise the Spark Job UI:
df = spark.range(1000000)
df.count()

# ## GAD Note: I suggest removing Methods 1 and 2, and reversing the other two--see part 2 below.

# **Method 1:** Enter `echo spark-${CDSW_ENGINE_ID}.${CDSW_DOMAIN}` into the 
# CDSW Terminal and paste the result into your browser.

# **Method 2:** Run the following Python code and select the resulting link:

import os, IPython
url = "spark-%s.%s" % (os.environ["CDSW_ENGINE_ID"], os.environ["CDSW_DOMAIN"])
IPython.display.HTML("<a href=http://%s>Spark UI</a>" % url)

# **Method 3:** Use the Hue Job Browser for Spark Applications on the cluster.

# **Method 4:** Use the `uiWebUrl` method.

spark.sparkContext.uiWebUrl

# ## GAD Note part 2 (from above), I suggest replacing the script between these two GAD notes with:

# There are at least four methods for viewing your Spark Job UI.  Here are two.

# **Method 1:** Use the `uiWebUrl` method:

spark.sparkContext.uiWebUrl

# , then paste this url into your browser.

# **Method 2:** Use the Hue Job Browser for Spark Applications on the cluster.

# See also [Accessing Spark 2 Web UIs from Cloudera Data Science Workbench](https://www.cloudera.com/documentation/data-science-workbench/latest/topics/cdsw_spark_ui.html#cdsw_spark_ui).

# ## Viewing the Spark History Server UI


# You can open the Spark History Server UI directly from CDSW.

spark.stop()
