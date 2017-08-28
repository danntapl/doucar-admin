# # Building and evaluating clustering models

# In this module we use the clustering functionality is Spark MLlib to look for
# structure in the ride data.


# ## Setup

# Install the folium package for map plotting:
!pip install folium

# Import useful packages, modules, classes, and functions:
from __future__ import print_function
import folium

# Create a SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("cluster").getOrCreate()

# Load the enhanced ride data from HDFS:
rides = spark.read.parquet('/duocar/joined/')


# ## Preprocess the data

# Remove the canceled rides:
rides_filtered = rides.filter(rides.cancelled == 0)


# ## Extract, transform, and select the features

# Select origin and destination coordinates:
rides_selected = rides_filtered.select('origin_lat', 'origin_lon', 'dest_lat', 'dest_lon')

# Assemble the feature vector:
from pyspark.ml.feature import VectorAssembler
va = VectorAssembler(inputCols=['origin_lat', 'origin_lon', 'dest_lat', 'dest_lon'], outputCol='features')
rides_assembled = va.transform(rides_selected)

# Standardize the feature vector:
from pyspark.ml.feature import StandardScaler
ss = StandardScaler(inputCol='features', outputCol='features_scaled', withMean=False, withStd=True)
rides_standardized = ss.fit(rides_assembled).transform(rides_assembled)

# **Note:** Given that latitude and longitude are in similar scales,
# standardization may be superfluous in this case.
rides_standardized.describe().show()

# Spark MLlib does not provide a transformer to unscale the features.  In order
# to create meaningful plots below, we will proceed with unscaled features.


# ## Specify and fit a k-means model

# Use the `KMeans` class constructor to specify a k-means model:
from pyspark.ml.clustering import KMeans
kmeans = KMeans(featuresCol='features', predictionCol='cluster', k=3)
type(kmeans)

# Use the `explainParams` method to get a full list of the arguments:
print(kmeans.explainParams())

# Use the `fit` method to fit the k-means model:
kmeans_model = kmeans.fit(rides_standardized)
type(kmeans_model)

# **Note:** Euclidean distance may not be appropriate in this case.


# ## Evaluate the k-means model

# Compute cluster costs:
kmeans_model.computeCost(rides_standardized)

# **Question:** How useful is this number?  I would prefer silhouette.

# Print out the cluster centroids:
kmeans_model.clusterCenters()

if kmeans_model.hasSummary:
  
  # Print cluster sizes
  print(kmeans_model.summary.clusterSizes)
  
  # Show predicitions
  kmeans_model.summary.predictions.show(5)
  
# Plot cluster centers
center_map = folium.Map(location=[46.8772222, -96.7894444])
for cluster in kmeans_model.clusterCenters():
  # Plot marker at origin.
  folium.Marker([cluster[0], cluster[1]], icon=folium.Icon(color="green")).add_to(center_map)
  # Plot marker at destination.
  folium.Marker([cluster[2], cluster[3]], icon=folium.Icon(color="red")).add_to(center_map)
  # Plot line between origin and destination.
  folium.PolyLine([[cluster[0], cluster[1]], [cluster[2], cluster[3]]]).add_to(center_map)
center_map


# ## Explore cluster profiles

# Get original DataFrame with cluster ids:
predictions = kmeans_model.summary.predictions

from pyspark.sql.functions import count, mean, stddev
#predictions.groupBy("cluster").agg(count("distance"), mean("distance"), stddev("distance")).show()


# ## Exercises

# None


# ## Cleanup

# Stop the SparkSession:
spark.stop()


# ## References

# [Clustering](http://spark.apache.org/docs/latest/ml-clustering.html)
