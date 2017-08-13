# # Working with Machine Learning Pipelines

# In the previous modules we have established a workflow in which we load some
# data; preprocess this data; extract, transform, and select features; and
# develop and evaluate a machine learning model.  In this module we show how
# we can encapsulate this workflow into a [Spark MLlib
# Pipeline](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml)
# that we can reuse in our development or production environment.


# ## Setup

# Import useful packages, modules, classes, and functions:
from __future__ import print_function
from pyspark.sql import SparkSession

# Create a SparkSession:
spark = SparkSession.builder.master("local").appName("pipeline").getOrCreate()


# ## Load data

# Read enhanced ride data from HDFS:
rides = spark.read.parquet("/duocar/joined_all/")

# Create train and test DataFrames:
(train, test) = rides.randomSplit([0.7, 0.3], 12345)


# ## Specify pipeline stages

# Filter out cancelled rides:
from pyspark.ml.feature import SQLTransformer
filterer = SQLTransformer(statement="SELECT * FROM __THIS__ WHERE cancelled == 0")

# **Note:** Recall that `SQLTransformer` requires the `statement` keyword.

# Index `star_rating` label:
from pyspark.ml.feature import StringIndexer
labeler = StringIndexer(inputCol="star_rating", outputCol="star_rating_indexed")

# **Note:** We are treating `star_rating` as a multiclass label.

# Index `vehicle_color`:
indexer = StringIndexer(inputCol="vehicle_color", outputCol="vehicle_color_indexed")

# Create dummy variables for `vehicle_color_indexed`:
from pyspark.ml.feature import OneHotEncoder
encoder = OneHotEncoder(inputCol="vehicle_color_indexed", outputCol="vehicle_color_encoded")

# Generate reviewed feature:
extractor = SQLTransformer(statement="SELECT *, review IS NOT NULL AS reviewed FROM __THIS__")

# Select and assemble features:
from pyspark.ml.feature import VectorAssembler
features = ["vehicle_year", "vehicle_color_encoded", "reviewed", "CloudCover"]
assembler = VectorAssembler(inputCols=features, outputCol="features")

# Specify estimator (classification algorithm):
from pyspark.ml.classification import RandomForestClassifier
classifier = RandomForestClassifier(featuresCol="features", labelCol="star_rating_indexed")
print(classifier.explainParams())

# Specify hyperparameter grid:
from pyspark.ml.tuning import ParamGridBuilder
maxDepthList = [5, 10, 20]
numTreesList = [20, 50, 100]
subsamplingRateList = [0.5, 1.0]
paramGrid = ParamGridBuilder() \
  .addGrid(classifier.maxDepth, maxDepthList) \
  .addGrid(classifier.numTrees, numTreesList) \
  .addGrid(classifier.subsamplingRate, subsamplingRateList) \
  .build()

# Specify evaluator:
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
evaluator = MulticlassClassificationEvaluator(labelCol="star_rating_indexed", metricName="accuracy")

# Specify validator:
from pyspark.ml.tuning import TrainValidationSplit
validator = TrainValidationSplit(estimator=classifier, estimatorParamMaps=paramGrid, evaluator=evaluator)


# ## Specify pipeline

# Specify Pipeline:
from pyspark.ml import Pipeline
stages = [filterer, labeler, indexer, encoder, extractor, assembler, validator]
pipeline = Pipeline(stages=stages)


# ## Fit pipeline model

# Fit PipelineModel
%time pipeline_model = pipeline.fit(train)


# ## Query PipelineModel

# Probe stages
pipeline_model.stages
[type(x) for x in pipeline_model.stages]

# Access stage
type(pipeline_model.stages[0])
pipeline_model.stages[0].labels

# Access classifier model
classifier_model = pipeline_model.stages[3]
classifier_model.bestModel.featureImportances


# ## Apply PipelineModel

# Apply Pipeline Model:
classified = pipeline_model.transform(preprocessed)
#classified.select("service", "service_indexed", "distance", "driver_student", "features").show()

# **Note:** In this case, VectorAssembler converts distance from an integer to a double
# and converts student from boolean to double.

# Confusion matrix
classified.crosstab("prediction", "star_rating").show()

# Create baseline prediction (always predict five-star rating)
from pyspark.sql.functions import lit
classified2 = classified.withColumn("prediction_baseline", lit(5.0))
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
evaluator = MulticlassClassificationEvaluator(labelCol="star_rating", metricName="accuracy")
evaluator.setPredictionCol("prediction_baseline").evaluate(classified2)
evaluator.setPredictionCol("prediction").evaluate(classified2)

# ----

# ## Read the data

# Read ride data from HDFS:
tmp0 = spark.read.parquet("/duocar/joined_all/")


# ## Preprocess the data

# Filter out cancelled rides:
tmp1 = tmp0.filter(tmp0.cancelled == 0)

# Convert `distance` from integer to double for Bucketizer:
tmp2 = tmp1.withColumn("distance", tmp1.distance.cast("double"))

# Create distance vector for MinMaxScaler:
from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(inputCols=["distance"], outputCol="distance_vectorized")
rides = assembler.transform(tmp2)

# **Note:** Spark MLlib is pretty strict about column types.


# ## Transformers

# A
# [Transformer](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.Transformer)
# takes a DataFrame as input and returns a DataFrame as output.
# [Bucketizer](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.Bucketizer)
# is an example of a transformer:
from pyspark.ml.feature import Bucketizer
transformer = Bucketizer(inputCol="distance", outputCol="distance_bucketized", splits=[0, 1000, 5000, 10000, 50000, 100000])

# All Transformers have a `transform` method that takes a DataFrame as input and
# returns a DataFrame as output:
rides_transformed = transformer.transform(rides)
rides_transformed.printSchema()
rides_transformed.select("distance", "distance_bucketized").show(5)

# **Note:** Bucket 0.0 is [0.0, 1000.0), bucket 1.0 is [1000.0, 5000.0), etc.


# ## Estimators

# An
# [Estimator]()
# takes a DataFrame as input and returns a transformer (model) as output.
# ### Example: Feature Transformer

# [MinMaxScaler](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.MinMaxScaler)
# is an example of an estimator:
from pyspark.ml.feature import MinMaxScaler
scaler = MinMaxScaler(inputCol="distance_vectorized", outputCol="distance_scaled")

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
rides_scaled.select("distance_vectorized", "distance_scaled").show(5)

# ### Example: Machine Learning Algorithm

# A
# [DecisionTreeClassifier](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.classification.DecisionTreeClassifier)
# is an estimator.
from pyspark.ml.classification import DecisionTreeClassifier
classifier = DecisionTreeClassifier(featuresCol="features", labelCol="cancelled")

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
