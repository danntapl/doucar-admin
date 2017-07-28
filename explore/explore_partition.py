# # Explore Partitioning

# **Note:** The stages finish at different times and the session log gets mixed up.

# Create the SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("explore_partition").master("local").getOrCreate()

# Define function to print out partitions:
def f(partition):
  print("**** Begin Partition ****")
  for row in partition:
    print row
  print '**** End Partition ****'

# Observe default behavior:
df = spark.read.csv("file:///home/cdsw/explore_partition.csv", header=True, inferSchema=True)
df.explain()
df.rdd.getNumPartitions()
df.foreachPartition(f)

# (Re)partition:
df2 = df.repartition(3)
df2.explain()
df2.rdd.getNumPartitions()
df2.foreachPartition(f)

# **Note:** Uses round robin partitioning.

# (Re)partition by first column:
df3 = df.repartition(3, "c1")
df3.explain()
df3.rdd.getNumPartitions()
df3.foreachPartition(f)

# **Note:** Uses hash partitioning.

# Coalesce df2:
df4 = df2.coalesce(2)
df4.explain()
df4.rdd.getNumPartitions()
df4.foreachPartition(f)

# **Question:** Why does it coalesce to one partition?
# To reduce shuffling?
# To get evenly sized partitions?

# Coalesce df3:
df5 = df3.coalesce(2)
df5.explain()
df5.rdd.getNumPartitions()
df5.foreachPartition(f)

# Stop the SparkSession:
spark.stop()