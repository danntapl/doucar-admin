# # Working with Machine Learning Pipelines

# In this module we demonstrate how to assemble, configure, and run machine learning pipelines.


# ## TODO
# * Create outline
# * Demonstrate how to override parameters


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('pipeline').master('local[2]').getOrCreate()


# ## Read the data

# Read ride data from HDFS:
tmp0 = spark.read.parquet('/duocar/joined_all/')


# ## Preprocess the data

# Filter out cancelled rides:
tmp1 = tmp0.filter(tmp0.cancelled == 0)

# Convert `distance` from integer to double for Bucketizer:
tmp2 = tmp1.withColumn('distance', tmp1.distance.cast('double'))

# Create distance vector for MinMaxScaler:
from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(inputCols=['distance'], outputCol='distance_vectorized')
rides = assembler.transform(tmp2)

# **Note:** Spark MLlib is pretty strict about column types.


# ## Transformers

# A
# [Transformer](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.Transformer)
# takes a DataFrame as input and returns a DataFrame as output.
# [Bucketizer](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.Bucketizer)
# is an example of a transformer:
from pyspark.ml.feature import Bucketizer
transformer = Bucketizer(inputCol='distance', outputCol='distance_bucketized', splits=[0, 1000, 5000, 10000, 50000, 100000])

# All Transformers have a `transform` method that takes a DataFrame as input and
# returns a DataFrame as output:
rides_transformed = transformer.transform(rides)
rides_transformed.printSchema()
rides_transformed.select('distance', 'distance_bucketized').show(5)

# **Note:** Bucket 0.0 is [0.0, 1000.0), bucket 1.0 is [1000.0, 5000.0), etc.


# ## Estimators

# An
# [Estimator]()
# takes a DataFrame as input and returns a transformer (model) as output.
# ### Example: Feature Transformer

# [MinMaxScaler](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.MinMaxScaler)
# is an example of an estimator:
from pyspark.ml.feature import MinMaxScaler
scaler = MinMaxScaler(inputCol='distance_vectorized', outputCol='distance_scaled')

# **Note:** The input column for MinMaxScaler must be a Vector and it can not
# include any missing values.

# All estimators have a `fit` method that takes a DataFrame as input and returns
# a model transfomer as output:
scaler_model = scaler.fit(rides)

# In this particualar case, the result is an instance of the
# [MinMaxScalerModel](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.MinMaxScalerModel)
# class:
type(scaler_model)

# The fit parameters are typically accessible via attributes or methods:
scaler_model.originalMin
scaler_model.originalMax

# Since the model is a transformer, it has a transform method that applies the model:
rides_scaled = scaler_model.transform(rides)
rides_scaled.printSchema()
rides_scaled.select('distance_vectorized', 'distance_scaled').show(5)

# ### Example: Machine Learning Algorithm

# A
# [DecisionTreeClassifier](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.classification.DecisionTreeClassifier)
# is an estimator.
from pyspark.ml.classification import DecisionTreeClassifier
classifier = DecisionTreeClassifier(featuresCol='features', labelCol='cancelled')

# ## Pipelines

# A **pipeline** is a sequence of transformers and estimators that implement a data
# engineering or machine learning workflow.

# A [Pipeline]() is an estimator that takes a DataFrame as input and returns a
# transformer, a [PipeLineModel](), as output.


# ## Exercises


# ## Cleanup

# Stop the SparkSession:
spark.stop()


# ## References

# [ML Pipelines](http://spark.apache.org/docs/latest/ml-pipeline.html)

# [ML Pipelines API](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html)
