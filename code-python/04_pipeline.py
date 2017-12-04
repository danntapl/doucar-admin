# # Working with Machine Learning Pipelines

# Copyright © 2010–2017 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.

# In this exmple, we load some
# data; preprocess this data; extract, transform, and select features; and
# develop and evaluate a machine learning model.  In this module we show how we
# can encapsulate this workflow into a [Spark MLlib
# Pipeline](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml)


# ## Setup

# Import useful packages, modules, classes, and functions:
from __future__ import print_function
from pyspark.sql import SparkSession

# Create a SparkSession:
spark = SparkSession.builder.master("local").appName("pipeline").getOrCreate()


# ## Load data

# Read the enhanced ride data from HDFS:
rides = spark.read.parquet("/duocar/joined_all/")

# Create train and test DataFrames:
(train, test) = rides.randomSplit([0.7, 0.3], 12345)


# ## Specify pipeline stages

# A *Pipeline* is a sequence of stages that implement a data engineering or
# machine learning workflow.  Each stage in the pipeline is either a
# *Transformer* or an *Estimator*.  Recall that a
# [Transformer](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.Transformer)
# takes a DataFrame as input and returns a DataFrame as output.  Recall that an
# [Estimator](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.Estimator)
# takes a DataFrame as input and returns a Transformer (e.g., model) as output.
# We begin by specifying the stages in our machine learning workflow.

# Filter out cancelled rides:
from pyspark.ml.feature import SQLTransformer
filterer = SQLTransformer(statement="SELECT * FROM __THIS__ WHERE cancelled == 0")

# **Note:** The `SQLTransformer' has no default parameter and requires the `statement` keyword.

# Index `vehicle_color`:
from pyspark.ml.feature import StringIndexer
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

# Specify estimator (i.e., classification algorithm):
from pyspark.ml.classification import RandomForestClassifier
classifier = RandomForestClassifier(featuresCol="features", labelCol="star_rating")
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
evaluator = MulticlassClassificationEvaluator(labelCol="star_rating", metricName="accuracy")

# **Note:** We are treating `star_rating` as a multiclass label.

# Specify validator:
from pyspark.ml.tuning import TrainValidationSplit
validator = TrainValidationSplit(estimator=classifier, estimatorParamMaps=paramGrid, evaluator=evaluator)


# ## Specify a pipeline model

# A
# [Pipeline](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.Pipeline)
# itself is an `Estimator`:
from pyspark.ml import Pipeline
stages = [filterer, indexer, encoder, extractor, assembler, validator]
pipeline = Pipeline(stages=stages)


# ## Fit the pipeline model

# The `fit` method produces a
# [PipelineModel](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.PipelineModel),
# which is a `Transformer`:
%time pipeline_model = pipeline.fit(train)


# ## Query the PipelineModel

# Access the stages of a `PipelineModel` using its `stages` attribute:
pipeline_model.stages

# We can access each stage as necessary:
indexer_model = pipeline_model.stages[1]
indexer_model.labels

# The best model is an instance of the
# [RandomForestClassificationModel](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.classification.RandomForestClassificationModel)
# class:
validator_model = pipeline_model.stages[5]
type(validator_model.bestModel)
validator_model.bestModel.getNumTrees
validator_model.bestModel.featureImportances


# ## Apply the PipelineModel

# We can use the `transform` method of our `PipelineModel` to apply it to our
# test DataFrame:
classified = pipeline_model.transform(test)
classified.printSchema()

# Let us generate a confusion matrix to see how well we did:
classified \
  .crosstab("prediction", "star_rating") \
  .orderBy("prediction_star_rating") \
  .show()

# Compare model performance to baseline prediction (always predict five-star
# rating):
from pyspark.sql.functions import lit
classified_with_baseline = classified.withColumn("prediction_baseline", lit(5.0))
evaluator = MulticlassClassificationEvaluator(labelCol="star_rating", metricName="accuracy")
evaluator.setPredictionCol("prediction_baseline").evaluate(classified_with_baseline)
evaluator.setPredictionCol("prediction").evaluate(classified_with_baseline)

# Again, we have some work to do!


# ## Cleanup

# Stop the SparkSession:
spark.stop()


