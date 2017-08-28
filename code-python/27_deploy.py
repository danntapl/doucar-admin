# # Deploying Models and Pipelines in Production

# In this module we show how save, load, and apply transformers, estimators,
# and pipelines.


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("deploy").master("local").getOrCreate()

# Read the enhanced ride data from HDFS:
rides = spark.read.parquet("/duocar/joined/")


# ## Preprocess the data

# Convert `distance` from integer to double:
pre1 = rides.withColumn("distance_double", rides.distance.cast("double"))

# Convert `star_rating` from integer to double:
pre2 = pre1.withColumn("star_rating_double", rides.star_rating.cast("double"))

# Rename preprocessed DataFrame:
preprocessed = pre2


# ## Transform the data

# Let us start out will a simple example of deploying a single transformer.

# ### Binarize `star_rating`

# Specify
# [Binarizer](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.Binarizer):
from pyspark.ml.feature import Binarizer
binarizer = Binarizer(inputCol="star_rating_double", outputCol="star_rating_binarized", threshold=3.5)

# **Note:**  `Binarizer` requires the input column to be of type double.

# Apply binarizer:
binarized = binarizer.transform(preprocessed)

# Verify transformation:
binarized.select("star_rating", "star_rating_binarized").show()
binarized.groupBy("star_rating", "star_rating_binarized").count().orderBy("star_rating").show()

# Note that null values get mapped to null values.
binarized \
  .filter(binarized.star_rating.isNull()) \
  .select("star_rating", "star_rating_binarized") \
  .show()

# Save the Binarizer to HDFS:
!hdfs dfs -rm -r myduocar/binarizer
binarizer.save("myduocar/binarizer")

# **Note**: We are saving to our user directory in HDFS.  The Binarizer
# instance is stored in JSON format.  You can view it in Hue.


# ## Other Deployment Options

# [Predictive Model Markup Language
# (PMML)](http://dmg.org/pmml/v4-3/GeneralStructure.html): The older
# `spark.mllib` Scala API allows for some model types to be exported to PMML.
# However, this functionality does not seem be actively maintained. 

# [MLeap](https://github.com/combust/mleap): It is still early, but MLeap looks
# like an interesting option.


# ## Exercises

# None


# ## Cleanup

# Stop the SparkSession:
spark.stop()


# ## References

# None
