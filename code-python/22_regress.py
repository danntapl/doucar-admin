# # Building and evaluating regression models

# In this module we build and evaluate a linear regression model to predict
# ride rating from various ride, driver, and rider information.  The general
# workflow will be the same for other regression algorithms; however, the
# particular details will differ.


# ## Setup

# Import useful packages, modules, classes, and functions:
from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# We do not seem to need these:
#``` python
#import numpy as np
#import pandas as pd
#import matplotlib.pyplot as plt
#import seaborn as sns
#```

# Create a SparkSession:
spark = SparkSession.builder.master("local").appName("regress").getOrCreate()

# Read the enhanced ride data from HDFS:
rides = spark.read.parquet("/duocar/joined_all")


# ## Preprocess the data for modeling

# Cancelled rides do not have a rating.  We can use the `filter` or `where`
# method to remove the cancelled rides:
processed1 = rides.filter(rides.cancelled == 0)
processed1.count()

# However, we will use the versatile
# [SQLTransformer](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.SQLTransformer)
# instead:
from pyspark.ml.feature import SQLTransformer
processed2 = SQLTransformer(statement="SELECT * FROM __THIS__ WHERE cancelled == 0").transform(rides)
processed2.count()

# **Note:** The `SQLTransformer` requires the `statement` keyword.

# **Note:** `__THIS__` represents the DataFrame passed to the `transform`
# method.

# **Note:** Using the `SQLTransformer` rather than the `filter` or `where`
# method allows us to use this transformation in a Spark MLlib pipeline.


# ## Extract, transform, and select features

# Next we identify a few potential features to include in our model.  Let us
# create function to explore potential (categorical) features:
def explore(df, feature, label, plot=True):
  from pyspark.sql.functions import count, mean, stddev
  aggregated = df \
    .groupBy(feature) \
    .agg(count(label), mean(label), stddev(label)).orderBy(feature)
  aggregated.show()
  if plot == True:
    pdf = aggregated.toPandas()
    pdf.plot.bar(x=pdf.columns[0], y=pdf.columns[2], yerr=pdf.columns[3], capsize=5)

# Did the rider review the ride?
engineered1 = processed2.withColumn("reviewed", col("review").isNotNull().cast("int"))
explore(engineered1, "reviewed", "star_rating")

# Does the year of the vehicle matter?
explore(processed2, "vehicle_year", "star_rating")

# What about the color of the vehicle?
explore(processed2, "vehicle_color", "star_rating")

# Do riders give better reviews on sunny days?
explore(engineered1, "CloudCover", "star_rating")

# Spark MLlib algorithms require the features to be a vector of doubles.  As a
# result, we need to further transform these features before we can build our
# regression model.

# Use `StringIndexer` to convert `vehicle_color` from string codes to numeric
# codes:
from pyspark.ml.feature import StringIndexer
indexer = StringIndexer(inputCol="vehicle_color", outputCol="vehicle_color_ix")
indexer_model = indexer.fit(engineered1)
list(enumerate(indexer_model.labels))
indexed = indexer_model.transform(engineered1)
indexed.select("vehicle_color", "vehicle_color_ix").show(5)

# **Note:** `StringIndexer` is an estimator.

# Use `OneHotEncoder` to generate a set of dummy variables:
from pyspark.ml.feature import OneHotEncoder
encoder = OneHotEncoder(inputCol="vehicle_color_ix", outputCol="vehicle_color_cd")
encoded = encoder.transform(indexed)
encoded.select("vehicle_color", "vehicle_color_ix", "vehicle_color_cd").show(5)

# **Note:** `OneHotEncoder` is a transformer.

# Now we are ready to select the features (and label):
selected = encoded.select("reviewed", "vehicle_year", "vehicle_color_cd", "star_rating")
features = ["reviewed", "vehicle_year", "vehicle_color_cd"]

# Finally, we must assemble the features into a single column of vectors:
from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(inputCols=features, outputCol="features")
assembled = assembler.transform(selected)
assembled.head(5)

# Let us save the data for subsequent modules:
assembled.write.parquet("myduocar/regression_data", mode="overwrite")

# **Note:** We are saving the data in our personal HDFS directory.


# ## Create train and test sets

(train, test) = assembled.randomSplit([0.7, 0.3], 12345)

# **Gotcha:**  Weights must be doubles.


# ## Specify a linear regression model

# Use the `LinearRegression` class to specify a linear regression model:
from pyspark.ml.regression import LinearRegression
lr = LinearRegression(featuresCol="features", labelCol="star_rating")

# Use the `explainParams` method to get a full list of parameters:
print(lr.explainParams())


# ## Fit the linear regression model

# Use the `fit` method to fit the linear regression model:
lr_model = lr.fit(train)

# Query model performance:
lr_model.summary.r2

# Query the model:
lr_model.intercept
lr_model.coefficients
lr_model.summary.coefficientStandardErrors
lr_model.summary.tValues
lr_model.summary.pValues


# ## Evaluate model performance on the test set.

# ### Method 1: Use the `evaluate` method of the `LinearRegressionModel` class

summary_test = lr_model.evaluate(test)

summary_test.r2

summary_test.rootMeanSquaredError

# Plot observed versus predicted values:
tmp = summary_test.predictions.select("prediction", "star_rating").toPandas()
tmp.plot.scatter(x="prediction", y="star_rating")

# **Developer Note:**  This does not seem to work if I import pandas above or if I run it twice in the same session.

# ### Method 2: Use the `evaluate` method of the `RegressionEvaluator` class

# Generate predictions on the test set:
test_with_predictions = lr_model.transform(test)
test_with_predictions.show(5)

# Create instance of `RegressionEvaluator` class:
from pyspark.ml.evaluation import RegressionEvaluator
evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="star_rating", metricName="r2")
print(evaluator.explainParams())
evaluator.evaluate(test_with_predictions)

# Evaluate using another metric:
evaluator.setMetricName("rmse").evaluate(test_with_predictions)


# ## Exercises

# (1)  Experiment with different sets of features.

# (2)  Experiment with a different regression method.


# ## Cleanup

# Stop the SparkSession:
spark.stop()


# ## References

# [Classification and regression](http://spark.apache.org/docs/latest/ml-classification-regression.html)
