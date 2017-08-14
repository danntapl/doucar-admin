# # Deploying Models and Pipelines in Production

# In this module we show how save, load, and apply transformers, estimators, and pipelines.


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("deploy").master("local").getOrCreate()

# Read the enhanced ride data from HDFS:
rides = spark.read.parquet("/duocar/joined/")


# ## Preprocess the data

# Convert `distance` from integer to double.
pre1 = rides.withColumn('distance_double', rides.distance.cast('double'))

# Convert `star_rating` from integer to double.
pre2 = pre1.withColumn('star_rating_double', rides.star_rating.cast('double'))

# Rename preprocessed DataFrame:
preprocessed = pre2


# ## Transform the data

# ### Binarize `star_rating`

# Specify
# [Binarizer](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.Binarizer):
from pyspark.ml.feature import Binarizer
binarizer = Binarizer(inputCol='star_rating_double', outputCol='star_rating_binarized', threshold=3.5)

# **Note:**  `Binarizer` requires the input column to be of type double.

# Apply binarizer:
binarized = binarizer.transform(preprocessed)

# Verify transformation:
binarized.select('star_rating', 'star_rating_binarized').show()

# Null values get mapped to null values.
binarized.filter(binarized.star_rating.isNull()).select('star_rating', 'star_rating_binarized').show()

# Save the Binarizer to HDFS:
!hdfs dfs -rm -r myduocar/binarizer
binarizer.save("myduocar/binarizer")

# **Note**: We are saving to our user directory in HDFS.
# The Binarizer instance is stored in JSON format.
# You can view it in Hue.


# ## Integrate with Hive?
# * Add column to existing Hive table?
# * Add new Hive table?


# ## Integrate with Kudu or HBase?


# ## PMML?


# ## Exercises


# ## Cleanup

# Stop the SparkSession:
spark.stop()


# ## References