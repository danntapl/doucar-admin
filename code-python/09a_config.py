# # Configuring, monitoring, and tuning Spark Applications

# Copyright © 2010–2017 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.

# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('config').master('local').getOrCreate()

# ## General concepts for configuration

# Each SparkSession you create is a Spark application running with a variety of 
# application settings, for example:

# * the address of your HDFS file system
# * the memory requested for executors (processes) when the application runs 
# distributed on a cluster

# Most of these settings are handled for you by the administrator, who uses 
# Cloudera Manager to make settings that work as a general policy.  However, 
# you can configure some settings specifically for your own scripts.  

# Spark provides three separate techniques that configure separate parts
# of an application:

# 1. Logging
# 2. Spark properties
# 3. Environment variables

# Of these, the first two are the most common and useful, and we'll cover those 
# two techniques here.

# **One last note:** Your SparkSession, which provides methods for using 
# DataFrames and Spark SQL, actually has an underlying component called the 
# *SparkContext*, which is your actual "runtime", with support for RDDs--the 
# lower-level data structure in Spark programs. You configure your application 
# with methods that act on this underlying SparkContext.

# ## Configuring logging

# Logging in Spark (and nearly all Hadoop projects) is handled using the
# [Log4j](https://logging.apache.org/log4j/) framework.  Your administrator sets 
# default logging policies, which Spark picks up from a file in a well-known path:

!cat $SPARK_HOME/conf/log4j.properties

# Notice the line `shell.log.level=ERROR`, which sets default logging for sessions.

# You can change the requested log level any time throughout your 
# SparkSession with the command `spark.SparkContext.setLogLevel('<newLevel>')`.  
# The value for `<newLevel>` can be one of:
#    * ALL
#    * TRACE
#    * DEBUG
#    * INFO
#    * WARN
#    * ERROR
#    * FATAL
#    * OFF

# Whatever log level you set will cause your session to emit log messages 
# generated at that level and all levels below it on this list.

myrdd = spark.sparkContext.parallelize([1,2,3])

spark.sparkContext.setLogLevel("INFO")
myrdd.count()

spark.sparkContext.setLogLevel("WARN")
myrdd.count()


# ## Configuring Spark Properties

# You control most application settings with Spark properties.  You must provide
# these settings *before* you start your SparkSession with the `getOrCreate()` 
# method--unlike settings for logging, which can be changed repeatedly throughout 
# a SparkSession.

spark.stop()
spark = SparkSession\
  .builder\
  .appName('config')\
  .master('local')\
  .getOrCreate()

# In this example, the `appName` and `master` functions are convenience functions
# for the more general `config` function, which you invoke with a Spark property
# name and set value:

spark.stop()
spark = SparkSession\
  .builder\
  .config("spark.app.name", "config2")\
  .config("spark.master", "yarn")\
  .config("spark.ui.killEnabled", "true")\
  .getOrCreate()
  
# The most common Spark properties to set are documented 
# [here](http://spark.apache.org/docs/latest/configuration.html#available-properties).

# You can see all set properties in the configuration component of your 
# SparkContext:

spark.sparkContext.getConf().getAll()

# (Note that properties set to compiled defaults do not appear.)

# You can see a single set property (if it is set to a non-default value):

spark.sparkContext.getConf().get("spark.ui.port")

# ## Exercises

# Control logging:
# 1. Run a local session.  
# 2. Set logging to DEBUG level and watch for a minute
# 3. Set logging back to ERROR level.
# 4. Repeat these steps with a YARN-based session

# For a Spark application running on a cluster, what is the default memory
# requested for each executor process?  Start an application on the cluster
# with this memory request doubled.  Confirm your setting.

# ## Cleanup

# Stop the SparkSession (if any):
spark.stop()


# ## References

# [Spark Configuration](http://spark.apache.org/docs/latest/configuration.html)

# [Log4j](https://logging.apache.org/log4j/)