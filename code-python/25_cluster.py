# # Building and evaluating clustering models

# Copyright © 2010–2017 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.

# In this module we use Gaussian mixture models in Spark MLlib to look for
# structure in the rider data.


# ## Setup

# Install the [folium](https://github.com/python-visualization/folium) package
# for map plotting:
!pip install folium

# Import useful packages, modules, classes, and functions:
from __future__ import print_function
import folium

# Create a SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("cluster").getOrCreate()

# Load clean rider data from HDFS:
riders = spark.read.parquet("/duocar/clean/riders/")


# ## Preprocess the data

# Filter on student riders:
from pyspark.sql.functions import col
students = riders.filter(col("student") == True)


# ## Extract, transform, and select the features

# Select features to cluster on home latitude and longitude:
selected = ["home_lat", "home_lon"]

# Assemble the feature vector:
from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(inputCols=selected, outputCol="features")
assembled = assembler.transform(students)


# ## Build and evaluate a Gaussian mixture model

# Specify a Gaussian mixture model with two clusters:
from pyspark.ml.clustering import GaussianMixture
gm = GaussianMixture(featuresCol="features", k=2)
type(gm)

# Examine all arguments:
print(gm.explainParams())

# Fit the Gaussian mixture model: 
gm_model = gm.fit(assembled)
type(gm_model)

# Examine (multivariate) Gaussian distribution function:
gm_model.gaussiansDF.head(5)

# Plot Gaussian means (cluster centers):
center_map = folium.Map(location=[46.8772222, -96.7894444], zoom_start=13)
for cluster in gm_model.gaussiansDF.collect():
  folium.Marker(cluster["mean"]).add_to(center_map)
center_map

# Examine mixing weights:
gm_model.weights

# Examine model summary:
gm_model.hasSummary

# Examine cluster sizes:
gm_model.summary.clusterSizes

# Examine predictions DataFrame:
gm_model.summary.predictions.printSchema()
gm_model.summary.predictions.select("prediction", "probability").head(5)


# ## Save and apply clustering model

# Remove model if it exists:
!hdfs dfs -rm -r myduocar/gm_model

# Save the Gaussian mixture model:
gm_model.save("myduocar/gm_model")

# Load the Gaussian mixture model:
from pyspark.ml.clustering import GaussianMixtureModel
gm_model_loaded = GaussianMixtureModel.load("myduocar/gm_model")

# Apply the Gaussian mixture model:
clustered = gm_model_loaded.transform(assembled)

# Examine schema and view data:
clustered.printSchema()
clustered.head(5)

# Compute cluster sizes:
clustered.groupBy("prediction").count().orderBy("prediction").show()


# ## Explore cluster profiles

# Explore clusters:
clustered \
  .groupBy("prediction", "sex") \
  .count() \
  .orderBy("prediction", "sex") \
  .show()

# Plot profiles:
pandas_df = clustered.toPandas()
import seaborn as sns
sns.countplot("prediction", hue="sex", data=pandas_df)


# ## Exercises

# (1) Experiment with different values of k (number of clusters).

# (2) Experiment with other hyperparameters.

# (3) Look for clusters for all riders (not just student riders).


# ## Cleanup

# Stop the SparkSession:
spark.stop()


# ## References

# [Wikipedia - Cluster analysis](https://en.wikipedia.org/wiki/Cluster_analysis)

# [Spark Documentation - Clustering](http://spark.apache.org/docs/latest/ml-clustering.html)

# [Spark Python API - GaussianMixture class](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.clustering.GaussianMixture)

# [Spark Python API - GaussianMixtureModel class](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.clustering.GaussianMixtureModel)

# [Spark Python API - GaussianMixtureSummary class](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.clustering.GaussianMixtureSummary)

