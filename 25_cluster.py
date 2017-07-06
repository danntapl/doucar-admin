# # Clustering

# In this module we will use the clustering functionality is Spark MLlib to look for structure in the ride data.


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('cluster').master('local').getOrCreate()

# Load ride data from HDFS:
rides = spark.read.csv('duocar/rides_fargo.txt', sep='\t', header=True, inferSchema=True)


# ## Preprocess the data

# Remove the canceled rides:
rides_filtered = rides.filter(rides.cancelled == 0)

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

# **Note:** Given that latitude and longitude are already in the same scale, standardization may be superfluous in this case.


# ## Specify and fit a k-means model

# Specify and run the k-means algorithm
from pyspark.ml.clustering import KMeans
kmeans = KMeans(featuresCol='features', predictionCol='cluster', k=3)

# Use the `explainParams` method to get a full list of the arguments:
print(kmeans.explainParams())

# Use the `fit` method to fit the k-means model:
kmeans_model = kmeans.fit(rides_standardized)

# **Note:** Euclidean distance may not be appropriate in this case.


# ## Evaluate the k-mean model

# Compute cluster costs:
kmeans_model.computeCost(rides_standardized)

# **Question:** How useful is this number?  I would prefer silhouette.

# Print out the cluster centroids:
kmeans_model.clusterCenters()

if kmeans_model.hasSummary:
  
  # Print cluster sizes
  kmeans_model.summary.clusterSizes
  
  # Show predicitions
  kmeans_model.summary.predictions.show()
  
# Plot cluster centers
!pip install gmplot
import gmplot
from IPython.display import HTML
lat, lon = gmplot.GoogleMapPlotter.geocode('Fargo, ND')
mymap = gmplot.GoogleMapPlotter(lat, lon, 10)
for cluster in kmeans_model.clusterCenters():
  # Plot marker at origin.
  mymap.marker(cluster[0], cluster[1], color='green')
  # Plot marker at destination.
  mymap.marker(cluster[2], cluster[3], color='red')
  # Plot line between origin and destination.
  mymap.plot([cluster[0], cluster[2]], [cluster[1], cluster[3]])
# Save the file in /cdn, which is required for CDSW.
mymap.draw('/cdn/mymap.html')  # The file must be stored in /cdn.
# Display the source file located in /cdn.
HTML("<iframe src=mymap.html height='500px' width='800px'></iframe>")

# **Note:** The markers are not displaying, most likely due to the fact that the 
# the marker image is stored in /home/cdsw rather than /cdn.


# ## Explore cluster profiles


# ## Exercises


# ## Cleanup

# Stop the SparkSession:
spark.stop()


# ## TODO
# * Task

