# # Building and Evaluating Classification Models

# In this module we demonstrate how to build and evaluate classification
# models using logistic regression in Apache Spark MLlib.


# ## TODO
# * Use pre-joined data when available
# * Add documentation links
# * Add additional commentary as needed
# * Test on cluster


# ## Setup

# Import useful packages:
#import numpy as np
#import pandas as pd
#import matplotlib.pyplot as plt

# Create a SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('classify').master('local[2]').getOrCreate()


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


# ## Generate label

# In the regression module, we treated `star_rating` as a continuous variable.
# This is not necessarily the best approach:
joined2.groupBy('star_rating').count().orderBy('star_rating').show()

# In this module, let us see if we can predict four- or five-star ratings:
joined3 = joined2.withColumn('high_rating', joined2.star_rating > 3.5)
joined3.crosstab('star_rating', 'high_rating').show()

# Alternatively, we could have used `Binarizer` to create the new variable:
tmp = joined2.withColumn('star_rating', joined2.star_rating.cast('double'))
from pyspark.ml.feature import Binarizer
binarizer = Binarizer(inputCol='star_rating', outputCol='high_rating', threshold = 3.5)
joined4 = binarizer.transform(tmp)
joined4.crosstab('star_rating', 'high_rating').show()

# **Note:** `Binarizer` does not like integer values, thus we had to convert to doubles.


# ## Extract, transform, and select features

# Create function to explore features:
def explore(df, feature, label):
  from pyspark.sql.functions import count, mean
  aggregated = df.rollup(feature).agg(count(feature), mean(label)).orderBy(feature)
  aggregated.show()

# Did the rider review the ride?
engineered1 = joined4.withColumn('reviewed', joined4.review.isNotNull().cast('int'))
explore(engineered1, 'reviewed', 'high_rating')

# **Note:** The null value represents the rolled-up (total) results and 
# `ave(high_rating)` gives the observed probability of a high rating.

# Does the year of the vehicle matter?
explore(joined4, 'vehicle_year', 'high_rating')

# **Note:** The rider is more likely to give a high rating when the car is newer.
# We will leave this a continuous feature.

# What about the color of the vehicle?
explore(joined4, 'vehicle_color', 'high_rating')

# **Note:** The rider is more likely to give a high rating to black cars and
# less likely to give a high rating if the car is yellow.

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
selected = encoded.select('reviewed', 'vehicle_year', 'vehicle_color_cd', 'star_rating', 'high_rating')
features = ['reviewed', 'vehicle_year', 'vehicle_color_cd']

# Assemble feature vector:
from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(inputCols=features, outputCol='features')
assembled = assembler.transform(selected)
assembled.head(5)

# Save data for subsequent modules:
assembled.write.parquet('duocar/classification_data', mode='overwrite')

# **Note:** We are saving to our user directory in HDFS.

# ## Create train and test sets

(train, test) = assembled.randomSplit([0.7, 0.3], 12345)

# **Gotcha:**  Weights must be doubles.


# ## Specify a logistic regression model

# Use the `LogisticRegression` class to specify a logistic regression model:
from pyspark.ml.classification import LogisticRegression
log_reg = LogisticRegression(featuresCol='features', labelCol='high_rating')

# Use the `explainParams` method to get a full list of parameters:
print(log_reg.explainParams())


# ## Fit the linear regression model

# Use the `fit` method to fit the linear regression model:
log_reg_model = log_reg.fit(train)

# The result is an instance of the [LogisticRegressionModel]() class:
type(log_reg_model)

# The model parameters are stored in the `intercept` and `coefficients` attributes:
log_reg_model.intercept
log_reg_model.coefficients

# The `summary` attribute is an instance of the [BinaryLogisticRegressionTrainingSummary]() class:
type(log_reg_model.summary)

# Query the iteration history:
log_reg_model.summary.totalIterations
log_reg_model.summary.objectiveHistory

def plot_iterations(summary):
  plt.plot(summary.objectiveHistory)
  plt.title('Training Summary')
  plt.xlabel('Iteration')
  plt.ylabel('Objective Function')
  plt.show()

plot_iterations(log_reg_model.summary)

# Query model performance:
log_reg_model.summary.areaUnderROC
log_reg_model.summary.roc.show(5)

def plot_roc_curve(summary):
  roc_curve = summary.roc.toPandas()
  plt.plot(roc_curve['FPR'], roc_curve['FPR'], 'k')
  plt.plot(roc_curve['FPR'], roc_curve['TPR'])
  plt.title('ROC Area: %s' % summary.areaUnderROC)
  plt.xlabel('False Positive Rate')
  plt.ylabel('True Positive Rate')
  plt.show()

plot_roc_curve(log_reg_model.summary)


# ## Evaluate model performance on the test set.

# ### Method 1: Use the `evaluate` method of the `LogisticRegressionModel` class

test_summary = log_reg_model.evaluate(test)

# The result is an instance of the
# [BinaryLogisticRegressionSummary]()
# class:
type(test_summary)

# It has attributes similar to those of the `BinaryLogisticRegressionTrainingSummary` class:
plot_roc_curve(test_summary)

# ### Method 2: Use the `evaluate` method of the [BinaryClassificationEvaluator]() class:

# Generate predictions on the test dataset:
test_with_predictions = log_reg_model.transform(test)
test_with_predictions.show(5)

# **Note:** The resulting DataFrame includes three types of predictions.
# The `rawPrediction` is a vector of log-odds, `prediction` is a vector or probabilities
# `prediction` is the predicted class based on the probability vector.

# Create instance of `BinaryClassificationEvaluator` class:
from pyspark.ml.evaluation import BinaryClassificationEvaluator
evaluator = BinaryClassificationEvaluator(rawPredictionCol='rawPrediction', labelCol='high_rating', metricName='areaUnderROC')
print(evaluator.explainParams())
evaluator.evaluate(test_with_predictions)

# Evaluate using another metric:
evaluator.setMetricName('areaUnderPR').evaluate(test_with_predictions)


# ## Scoring out a new dataset

# There are two ways to score out a new dataset.

# ### Method1: The `evaluate` method

# The more expensive way is to use the `evaluate` method of the `LogisticRegressionModel`
# class.  The `predictions` attribute of the resulting `BinaryLogisticRegressionSummary`
# instance contains the scored DataFrame:
test_with_evaluation = log_reg_model.evaluate(test)
test_with_evaluation.predictions.printSchema()
test_with_evaluation.predictions.head(5)

# ### Method 2: The `transform` method

# The more direct and efficient way is to use the `transform` method of the
# `LogisticRegressionModel` class:
test_with_predictions = log_reg_model.transform(test)
test_with_predictions.printSchema()
test_with_predictions.head(5)


# ## Exercises

# Experiment with different sets of features.

# Plot the precision-recall curve.

# How would you plot two ROC curves?

# Use multinomial logistic regression to predict the original star rating.

# Try a different classification method.


# ## Cleanup

# Stop the SparkSession:
spark.stop()


# ## References

# [Classification and regression](http://spark.apache.org/docs/latest/ml-classification-regression.html)

# [pyspark.ml.classification module](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml.classification)