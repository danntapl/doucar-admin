# # Tuning hyperparameters using grid search

# Most machine learning algorithms have a set of parameters that govern the
# algorithm's behavior.  These parameters are called hyperparameters to
# distinguish them from the model parameters such as the coefficients in linear
# and logistic regression.  In this module we show how to use grid search and
# cross validation in Spark MLlib to determine a reasonable regularization
# parameter for [$l1$ lasso linear
# regression](https://en.wikipedia.org/wiki/Lasso_%28statistics%29).
  

# ## Setup

# Import useful packages, modules, classes, and functions:
from __future__ import print_function
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

# Create a SparkSession:
spark = SparkSession.builder.master("local").appName("hypertune").getOrCreate()


# ## Generate the modeling data

# Load the regression modeling data from HDFS:
rides = spark.read.parquet("myduocar/regression_data")
rides.show(5)

# Create train and test DataFrames:
(train, test) = rides.randomSplit([0.7, 0.3], 12345)


# ## Requirements for hyperparameter tuning

# We need to specify four components to perform hyperparameter tuning using
# grid search:
# * Estimator
# * Hyperparameter grid
# * Evaluator
# * Validation method


# ## Specify the estimator

# In this example we will use $l1$ (lasso) linear regression as our estimator:
from pyspark.ml.regression import LinearRegression
lr = LinearRegression(featuresCol="features", labelCol="star_rating", elasticNetParam=1.0)

# Use the `explainParams` method to get the full list of hyperparameters:
print(lr.explainParams())

# Setting `elasticNetParam=1.0` corresponds to $l1$ (lasso) linear regression.
# We are interested in finding a good value of `regParam`.


# ## Specify hyperparameter grid

# Use the
# [ParamGridBuilder](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.tuning.ParamGridBuilder)
# class to specify a hyperparameter grid:
from pyspark.ml.tuning import ParamGridBuilder
regParamList = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5]
grid = ParamGridBuilder().addGrid(lr.regParam, regParamList).build()

# The resulting object is simply a list of parameter maps:
grid

# Rather than specify `elasticNetParam` in the `LinearRegression` constructor, we can specify it in our grid:
grid = ParamGridBuilder().baseOn({lr.elasticNetParam: 1.0}).addGrid(lr.regParam, regParamList).build()
grid


# ## Specify the evaluator

# In this case we will use
# [RegressionEvaluator](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.evaluation.RegressionEvaluator)
# as our evaluator and specify root-mean-squared error as the metric:
from pyspark.ml.evaluation import RegressionEvaluator
evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="star_rating", metricName="rmse")


# ## Tuning the hyperparameters using holdout cross-validation

# For large DataFrames, holdout cross-validation will be more efficient.  Use
# the
# [TrainValidationSplit](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.tuning.TrainValidationSplit)
# class to specify holdout cross-validation:
from pyspark.ml.tuning import TrainValidationSplit
validator = TrainValidationSplit(estimator=lr, estimatorParamMaps=grid, evaluator=evaluator, trainRatio=0.75, seed=54321)

# Use the `fit` method to find the best set of hyperparameters:
%time cv_model = validator.fit(train)

# **Note:** Our train DataFrame is split again according to `trainRatio`.

# The resulting model is an instance of the
# [TrainValidationSplitModel](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.tuning.TrainValidationSplitModel)
# class:
type(cv_model)

# The cross-validation results are stored in the `validationMatrics` attribute:
cv_model.validationMetrics

# These are the RMSE for each set of hyperparameters.  Smaller is better.

def plot_holdout_results(model):
  plt.plot(regParamList, model.validationMetrics)
  plt.title("Hyperparameter Tuning Results")
  plt.xlabel("Regularization Parameter")
  plt.ylabel("Validation Metric")
  plt.show()
plot_holdout_results(cv_model)

# In this case the `bestModel` attribute is an instance of the
# [LinearRegressionModel](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.regression.LinearRegressionModel)
# class:
type(cv_model.bestModel)

# **Note:** The model is rerun on the entire dataset using the best set of hyperparameters.

# The usual attributes and methods are available:
cv_model.bestModel.intercept
cv_model.bestModel.coefficients
cv_model.bestModel.summary.rootMeanSquaredError
cv_model.bestModel.evaluate(test).r2


# ## Tune hyperparameters using k-fold cross-validation

# For small datasets k-fold cross-validation will be more accurate.  Use the
# [CrossValidator](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.tuning.CrossValidator)
# class to specify the k-fold cross-validation:
from pyspark.ml.tuning import CrossValidator
kfold_validator = CrossValidator(estimator=lr, estimatorParamMaps=grid, evaluator=evaluator, numFolds=3, seed=54321)
%time kfold_model = kfold_validator.fit(train)

# The result is an instance of the
# [CrossValidatorModel](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.tuning.CrossValidatorModel)
# class:
type(kfold_model)

# The cross-validation results are stored in the `avgMetrics` attribute:
kfold_model.avgMetrics
def plot_kfold_results(model):
  plt.plot(regParamList, model.avgMetrics)
  plt.title("Hyperparameter Tuning Results")
  plt.xlabel("Regularization Parameter")
  plt.ylabel("Validation Metric")
  plt.show()
plot_kfold_results(kfold_model)

# The `bestModel` attribute contains the model based on the best set of hyperparameters.
# In this case, it is an instance of the `LinearRegressionModel` class:
type(kfold_model.bestModel)

# Compute the performance of the performance of the best model on the test dataset:
kfold_model.bestModel.evaluate(test).r2


# ## Exercises

# (1) Maybe our regularization parameters are too large.  Rerun the
# hyperparameter tuning with regularization parameters [0.0, 0.02, 0.04, 0.06,
# 0.08, 1.0].

# (2) Create a parameter grid that searches over regularization type (lasso or
# ridge) as well as the regularization parameter.

# (3) Apply hyperparameter tuning to your favorite machine learning algorithm (estimator).


# ## Cleanup

# Stop the SparkSession:
spark.stop()


# ## References

# [Model Selection and hyperparameter tuning](http://spark.apache.org/docs/latest/ml-tuning.html)

# [pyspark.ml.tuning module](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml.tuning)

