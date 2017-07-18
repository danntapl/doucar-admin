# # Building and Evaluating Regression Models


# ## TODO
# * Test on cluster
# * Add diagnostic plots
# * Add rider data
# * Add weather data
# * Add references
# * Add links


# ## Setup

# Import useful packages:
#import numpy as np
#import pandas as pd
#import matplotlib.pyplot as plt

# Create a SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('regress').master('local').getOrCreate()


# ## Generate modeling data

# Read ride data from HDFS:
rides = spark.read.csv('/duocar/rides', sep='\t', header=True, inferSchema=True)

# Remove cancelled rides:
filtered = rides.filter(rides.cancelled == 0)

# Read ride review data from HDFS:
reviews = spark.read.csv('/duocar/ride_reviews', sep='\t', inferSchema=True).toDF('ride_id', 'review')

# Join ride review data:
joined1 = filtered.join(reviews, filtered.id == reviews.ride_id, 'left_outer')

# Read driver data from HDFS:
drivers = spark.read.csv('/duocar/drivers', sep='\t', header=True, inferSchema=True)

# Join driver data:
joined2 = joined1.join(drivers, joined1.driver_id == drivers.id, 'left_outer')

# **Developer Note:**  Consider precomputing this data.


# ## Extract, transform, and select features

# Create function to explore features:
def explore(df, feature, label):
  from pyspark.sql.functions import count, mean
  aggregated = df.rollup(feature).agg(count(feature), mean(label)).orderBy(feature)
  aggregated.show()
#  aggregated.toPandas().plot(x=0, y=2, kind='scatter')

# Did rider review the ride?
engineered1 = joined2.withColumn('reviewed', joined2.review.isNotNull().cast('int'))
explore(engineered1, 'reviewed', 'star_rating')

# Does the year of the vehicle matter?
explore(joined2, 'vehicle_year', 'star_rating')

# What about the color of the vehicle?
explore(joined2, 'vehicle_color', 'star_rating')

# Use `StringIndexer` to convert the string codes to numeric codes:
from pyspark.ml.feature import StringIndexer
indexer = StringIndexer(inputCol='vehicle_color', outputCol='vehicle_color_ix')
indexer_model = indexer.fit(engineered1)
indexer_model.labels
indexed = indexer_model.transform(engineered1)

# **Note:** `StringIndexer` requires a fit and transform.

# Use `OneHotEncoder` to generate a set of dummy variables:
from pyspark.ml.feature import OneHotEncoder
encoder = OneHotEncoder(inputCol='vehicle_color_ix', outputCol='vehicle_color_cd')
encoded = encoder.transform(indexed)

# **Note:** `OneHotEncoder` only requires a transform.

# Select features (and label):
selected = encoded.select('reviewed', 'vehicle_year', 'vehicle_color_cd', 'star_rating')
features = ['reviewed', 'vehicle_year', 'vehicle_color_cd']

# Assemble feature vector:
from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(inputCols=features, outputCol='features')
assembled = assembler.transform(selected)
assembled.head(5)

# Save data for subsequent modules:
assembled.write.parquet('duocar/regression_data', mode='overwrite')


# ## Create train and test sets

(train, test) = assembled.randomSplit([0.7, 0.3], 12345)

# **Gotcha:**  Weights must be doubles.


# ## Specify a linear regression model

# Use the `LinearRegression` class to specify a linear regression model:
from pyspark.ml.regression import LinearRegression
lr = LinearRegression(featuresCol='features', labelCol='star_rating')

# Use the `explainParams` method to get a full list of parameters:
print(lr.explainParams())


# ## Fit the linear regression model

# Use the `fit` method to fit the linear regression model:
lr_model = lr.fit(train)

# Query model performance:
lr_model.summary.r2

# Query model:
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
tmp = summary_test.predictions.select('prediction', 'star_rating').toPandas()
tmp.plot(x='prediction', y='star_rating', kind='scatter')

# **Developer Note:**  This does not seem to work if I import pandas above or if I run it twice in the same session.

# ### Method 2: Use the `evaluate` method of the `RegressionEvaluator` class

# Generate predictions on the test set:
test_with_predictions = lr_model.transform(test)
test_with_predictions.show(5)

# Create instance of `RegressionEvaluator` class:
from pyspark.ml.evaluation import RegressionEvaluator
evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='star_rating', metricName='r2')
print(evaluator.explainParams())
evaluator.evaluate(test_with_predictions)

# Evaluate using another metric:
evaluator.setMetricName('rmse').evaluate(test_with_predictions)


# ## Exercises

# Try different sets of features.

# Try a different regression method.


# ## Cleanup

# Stop the SparkSession:
spark.stop()
