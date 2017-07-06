# # Connecting to Spark


# ## TODO
# * Killing a Spark application?


# ## Creating a SparkSession

# A `SparkSession` is the entry point to Spark SQL.

from pyspark.sql import SparkSession

# Use the `getOrCreate` method of the `builder` class to create a SparkSession.

spark = SparkSession.builder.appName('connect').getOrCreate()

# Use the optional `appName` method to change the default name of the SparkSession.

# **Note:** An underlying `SparkContext` is created with the SparkSession.

# **Note:** To adjust the logging level us `spark.SparkContext.setLogLevel(newLevel)`.

# Use the `version` method to get the version of Spark.

spark.version

# Use the `stop` method to end the `SparkSession`.

spark.stop()

#**Note:**  According to the documentation, this actually stops the underlying `SparkContext`.

# The `SparkSession` can connect to a local instance of Spark or a Spark cluster.


# ## Connecting to a local Spark instance

# A local Spark instance is useful for developing and debugging code on small datasets.
# To connect to a local Spark instance running on the CDSW node:

spark = SparkSession.builder.master('local').appName('connect').getOrCreate()

spark.stop()


# ## Connecting to a Spark cluster via YARN

# To connect to a Spark cluster via YARN:

spark = SparkSession.builder.master('yarn').appName('connect').getOrCreate()

# **Note:**  Spark runs in `client` mode for CDSW.

# **Note:**  YARN client mode is the default setting in CDSW.


# ## Viewing the Spark Job UI

# Create a simple Spark job to exercise the Spark Job UI
df = spark.range(1000000)
df.count()

# **Method 1:** Enter `echo spark-${CDSW_ENGINE_ID}.${CDSW_DOMAIN}` into the CDSW Terminal and paste the result into your browser.

# **Method 2:** Run the following Python code and select the resulting link:

import os, IPython
url = "spark-%s.%s" % (os.environ["CDSW_ENGINE_ID"], os.environ["CDSW_DOMAIN"])
IPython.display.HTML("<a href=http://%s>Spark UI</a>" % url)

# **Method 3:** Use the Hue Job Browser for Spark Applications on the cluster.

# **Method 4:** Use the `uiWebUrl` method.

spark.sparkContext.uiWebUrl


# ## Viewing the Spark History Server UI

# Open the Spark History Server UI directly from CDSW.

spark.stop()
