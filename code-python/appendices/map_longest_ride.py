# Get the route coordinates of the longest ride in the DuoCar
# data and use Folium to create a map of the ride route

# Setup
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import folium

spark = SparkSession.builder.master("local").getOrCreate()

# Load the rides and ride_routes data from HDFS
rides = spark.read.parquet("/duocar/clean/rides")
ride_routes = spark.read.parquet("/duocar/clean/ride_routes")

# Filter the rides data to the longest ride (by distance)
# and join it with the ride_routes data
longest_ride_route = rides\
  .orderBy(col("distance").desc())\
  .limit(1)\
  .join(ride_routes, col("id") == col("ride_id"))

# Make a list of the ride route coordinates
coordinates = [[float(i.lat), float(i.lon)] \
               for i in longest_ride_route.collect()]

# Make a Folium map
m = folium.Map()
m.fit_bounds(coordinates, padding=(25, 25))
folium.PolyLine(locations=coordinates, weight=5).add_to(m)
folium.Marker(coordinates[1], popup="Origin").add_to(m)
folium.Marker(coordinates[-1], popup="Destination").add_to(m)
m

# Cleanup
spark.stop()
