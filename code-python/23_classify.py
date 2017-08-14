# # Building and Evaluating Classification Models

# Spark MLlib provides a number of
# [classification](https://en.wikipedia.org/wiki/Statistical_classification)
# algorithms for predicting the value of a categorical variable.  In this
# module we demonstrate how to build and evaluate a classification model using
# [logistic regression](https://en.wikipedia.org/wiki/Logistic_regression).
# The general process will be similar for other classification algorithms,
# although the particular details will vary.


# ## Setup

# Import useful packages, modules, classes, and functions:
from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
#import numpy as np
#import pandas as pd
import matplotlib.pyplot as plt
#import seaborn as sns

# Create a SparkSession:
spark = SparkSession.builder.master("local").appName("classify").getOrCreate()


# ## Preprocess the modeling data

# Read the enhanced ride data from HDFS:
rides = spark.read.parquet("/duocar/joined/")

# A cancelled ride does not have a rating, so we remove all cancelled rides:
filtered = rides.filter(rides.cancelled == 0)

# Alternatively, we can use
# [SQLTransformer](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.SQLTransformer),
# which will allow us to incorporate this preprocessing step into our machine
# learning pipeline:
from pyspark.ml.feature import SQLTransformer
filtered = SQLTransformer(statement="SELECT * FROM __THIS__ WHERE cancelled == 0").transform(rides)

# **Note:** `__THIS__` is a placeholder for the DataFrame passed into the `transform` method.


# ## Generate label

# In the regression module we treated `star_rating` as a continuous variable.
# However, `star_rating` is actually an ordered categorical variable:
filtered.groupBy("star_rating").count().orderBy("star_rating").show()

# Rather than try to predict each value, let us see if we can distinguish between five-star and
# non-five-star ratings:
labeled = filtered.withColumn("high_rating", col("star_rating") > 4.5)
labeled.crosstab("star_rating", "high_rating").show()

# Alternatively, we can use
# [Binarizer](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.Binarizer)
# to create our label:
from pyspark.ml.feature import Binarizer
converted = filtered.withColumn("star_rating", col("star_rating").cast("double"))
binarizer = Binarizer(inputCol="star_rating", outputCol="high_rating", threshold = 4.5)
labeled = binarizer.transform(converted)
labeled.crosstab("star_rating", "high_rating").show()

# **Note:** `Binarizer` does not like integer values, thus we had to convert to doubles.


# ## Extract, transform, and select features

# Create function to explore features:
def explore(df, feature, label):
  from pyspark.sql.functions import count, mean
  aggregated = df.rollup(feature).agg(count(label), mean(label)).orderBy(feature)
  aggregated.show()

# **Feature 1:** Did the rider review the ride?
engineered1 = labeled.withColumn("reviewed", col("review").isNotNull().cast("int"))
explore(engineered1, "reviewed", "high_rating")

# **Note:** The null value represents the rolled-up (total) results and 
# `ave(high_rating)` gives the observed probability of a high rating.

# **Feature 2:** Does the year of the vehicle matter?
explore(labeled, "vehicle_year", "high_rating")

# **Note:** The rider is more likely to give a high rating when the car is
# newer.  We will treat this variable as a continuous feature.

# **Feature 3:** What about the color of the vehicle?
explore(labeled, "vehicle_color", "high_rating")

# **Note:** The rider is more likely to give a high rating if the car is is
# black and less likely to give a high rating if the car is yellow.

# The classification algorithms in Spark MLlib do not accept categorical
# features in this form, so let us convert `vehicle_color` to a set of dummy
# variables. First, we use
# [StringIndexer](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.StringIndexer)
# to convert the string codes to numeric codes:
from pyspark.ml.feature import StringIndexer
indexer = StringIndexer(inputCol="vehicle_color", outputCol="vehicle_color_ix")
indexer_model = indexer.fit(engineered1)
indexed = indexer_model.transform(engineered1)
indexed.select("vehicle_color", "vehicle_color_ix").show(5)
indexer_model.labels

# Then we use
# [OneHotEncoder](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.OneHotEncoder)
# to generate a set of dummy variables:
from pyspark.ml.feature import OneHotEncoder
encoder = OneHotEncoder(inputCol="vehicle_color_ix", outputCol="vehicle_color_cd")
encoded = encoder.transform(indexed)
encoded.select("vehicle_color", "vehicle_color_ix", "vehicle_color_cd").show(5)

# **Note:** `vehicle_color_cd` is stored in sparse format.

# Now we can (manually) select our features and label:
selected = encoded.select("reviewed", "vehicle_year", "vehicle_color_cd", "star_rating", "high_rating")
features = ["reviewed", "vehicle_year", "vehicle_color_cd"]

# The machine learning algorithms in Spark MLlib expect the features to be
# collected into a single column, so we use
# [VectorAssembler](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.VectorAssembler)
# to assemble our feature vector:
from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(inputCols=features, outputCol="features")
assembled = assembler.transform(selected)
assembled.head(5)

# **Note:** `features` is stored in sparse format.

# Save data for subsequent modules:
assembled.write.parquet("myduocar/classification_data", mode="overwrite")

# **Note:** We are saving the data to our user directory in HDFS.

# ## Create train and test sets

# We will fit our model on the train DataFrame and evaluate our model on the
# test DataFrame:
(train, test) = assembled.randomSplit([0.7, 0.3], 12345)

# **Gotcha:**  Weights must be doubles.


# ## Specify a logistic regression model

# Use the
# [LogisticRegression](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.classification.LogisticRegression)
# class to specify a logistic regression model:
from pyspark.ml.classification import LogisticRegression
log_reg = LogisticRegression(featuresCol="features", labelCol="high_rating")

# Use the `explainParams` method to get a full list of parameters:
print(log_reg.explainParams())


# ## Fit the linear regression model

# Use the `fit` method to fit the linear regression model on the train DataFrame:
log_reg_model = log_reg.fit(train)

# The result is an instance of the
# [LogisticRegressionModel](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.classification.LogisticRegressionModel)
# class:
type(log_reg_model)

# The model parameters are stored in the `intercept` and `coefficients` attributes:
log_reg_model.intercept
log_reg_model.coefficients

# The `summary` attribute is an instance of the
# [BinaryLogisticRegressionTrainingSummary](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.classification.BinaryLogisticRegressionTrainingSummary)
# class:
type(log_reg_model.summary)

# We can query the iteration history:
log_reg_model.summary.totalIterations
log_reg_model.summary.objectiveHistory

# and plot it too:
def plot_iterations(summary):
  plt.plot(summary.objectiveHistory)
  plt.title("Training Summary")
  plt.xlabel("Iteration")
  plt.ylabel("Objective Function")
  plt.show()

plot_iterations(log_reg_model.summary)

# We can also query the model performance, in this case, the area under the ROC curve:
log_reg_model.summary.areaUnderROC

# and plot the ROC curve:
log_reg_model.summary.roc.show(5)

def plot_roc_curve(summary):
  roc_curve = summary.roc.toPandas()
  plt.plot(roc_curve["FPR"], roc_curve["FPR"], "k")
  plt.plot(roc_curve["FPR"], roc_curve["TPR"])
  plt.title("ROC Area: %s" % summary.areaUnderROC)
  plt.xlabel("False Positive Rate")
  plt.ylabel("True Positive Rate")
  plt.show()

plot_roc_curve(log_reg_model.summary)

# **Note:** Redefining the `plot_roc_curve` function in the same session
# results in an error when the function is called.


# ## Evaluate model performance on the test set.

# We have been assessing the model performance on the train DataFrame.  We
# really want to assess it on the test DataFrame.

# **Method 1:** Use the `evaluate` method of the `LogisticRegressionModel` class

test_summary = log_reg_model.evaluate(test)

# The result is an instance of the
# [BinaryLogisticRegressionSummary](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.classification.BinaryLogisticRegressionSummary)
# class:
type(test_summary)

# It has attributes similar to those of the
# `BinaryLogisticRegressionTrainingSummary` class:
test_summary.areaUnderROC
plot_roc_curve(test_summary)

# **Method 2:** Use the `evaluate` method of the
# [BinaryClassificationEvaluator](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.evaluation.BinaryClassificationEvaluator)
# class

# Generate predictions on the test DataFrame:
test_with_prediction = log_reg_model.transform(test)
test_with_prediction.show(5)

# **Note:** The resulting DataFrame includes three types of predictions.  The
# `rawPrediction` is a vector of log-odds, `prediction` is a vector or
# probabilities `prediction` is the predicted class based on the probability
# vector.

# Create an instance of `BinaryClassificationEvaluator` class:
from pyspark.ml.evaluation import BinaryClassificationEvaluator
evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction", labelCol="high_rating", metricName="areaUnderROC")
print(evaluator.explainParams())
evaluator.evaluate(test_with_prediction)

# Evaluate using another metric:
evaluator.setMetricName("areaUnderPR").evaluate(test_with_prediction)


# ## Score out a new dataset

# There are two ways to score out a new dataset.

# **Method1:*** The `evaluate` method

# The more expensive way is to use the `evaluate` method of the
# `LogisticRegressionModel` class.  The `predictions` attribute of the
# resulting `BinaryLogisticRegressionSummary` instance contains the scored
# DataFrame:
test_with_evaluation = log_reg_model.evaluate(test)
test_with_evaluation.predictions.printSchema()
test_with_evaluation.predictions.head(5)

# **Note:** This is more expensive because the `evaluate` method computes all
# the evaluation metrics in addition to scoring out the DataFrame.

# ### Method 2: The `transform` method

# The more direct and efficient way is to use the `transform` method of the
# `LogisticRegressionModel` class:
test_with_prediction = log_reg_model.transform(test)
test_with_prediction.printSchema()
test_with_prediction.head(5)


# ## Exercises

# (1) Experiment with different sets of features.

# (2) Plot the precision-recall curve.

# (3) Plot the train and test ROC curves in the same figure?

# (4) Use multinomial logistic regression to predict the original star rating.

# (5) Experiment with other classification algorithms.


# ## Cleanup

# Stop the SparkSession:
spark.stop()


# ## References

# [Classification and regression](http://spark.apache.org/docs/latest/ml-classification-regression.html)

# [pyspark.ml.classification module](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml.classification)
