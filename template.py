# # TITLE


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('NAME').master('local').getOrCreate()


# ## Body

# Insert the magic here.


# ## Exercises


# ## Cleanup

# Stop the SparkSession:
spark.stop()


# ## TODO
# * Task