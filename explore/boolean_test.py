# # Test Inference of Boolean Values

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('boolean_test').master('local').getOrCreate()

!head boolean_test.csv
df = spark.read.csv('file:///home/cdsw/boolean_test.csv', header=True, inferSchema=True)

df.printSchema()
df.show()

spark.stop()