# # Explore Feature Transformations

# Create SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('explore_transforms').master('local[2]').getOrCreate()

# Read data:
joined_all = spark.read.parquet('/duocar/joined_all')

# Preprocess data:
tmp = joined_all.withColumn("distance_double", joined_all.distance.cast("double"))

# ## Bucketizer

from pyspark.ml.feature import Bucketizer
splits = [0, 1000, 10000, 100000]
bucketizer = Bucketizer(inputCol='distance_double', outputCol='distance_bucketized', splits=splits, handleInvalid='keep')
bucketized = bucketizer.transform(tmp)
bucketized.select('distance', 'distance_bucketized').show()

# **Note:** `Bucketizer` requires the input column to be of type double.  

def print_buckets(bucketizer):
  if bucketizer.getHandleInvalid() == 'keep':
    print 'null'
  splits = bucketizer.getSplits()
  for i in range(len(splits) - 1):
    print '[%i, %i)' % (splits[i], splits[i+1])
    
print_buckets(bucketizer)

# Stop SparkSession:
spark.stop()