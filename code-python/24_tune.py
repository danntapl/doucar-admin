# # Tuning hyperparameters using grid search

# In this module we demonstrate the use of grid search in Apache Spark MLlib to
# determine a reasonable regularization parameter for $l1$ (lasso) linear regression.


# ## TODO
# * Find more interesting model (with nonzero regularization parameter)
# * Add additional commentary
# * Add links to documentation
# * Test on cluster
# * Determine why LaTeX rendering is not working
# * Generate additional exercises


# ## Setup

# Import useful packages:
import matplotlib.pyplot as plt

# Create a SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('tune').master('local').getOrCreate()


# ## Generate modeling data

# Load enhanced ride data:
rides = spark.read.parquet('duocar/regression_data')

# Create train and test datasets:
(train, test) = rides.randomSplit([0.7, 0.3], 12345)


# ## Requirements for hyperparameter tuning

# We need to specify three *things* to perform hyperparameter tuning using grid search:
# * Estimator
# * Evaluator
# * Hyperparameter grid
# * Validation method


# ## Specifying the estimator

# In this example we will use $l1$ (lasso) linear regression as our estimator:
from pyspark.ml.regression import LinearRegression
lr = LinearRegression(featuresCol='features', labelCol='star_rating', elasticNetParam=1.0)

# Use the `explainParams` method to get the full list of hyperparameters:
print(lr.explainParams())


# ## Specifying the evaluator

# In this case we will use root-mean-squared error as the evaluator:
from pyspark.ml.evaluation import RegressionEvaluator
evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='star_rating', metricName='rmse')


# ## Specifying hyperparameter grid

# Use the `ParamGridBuilder` class to specify a hyperparameter grid:
from pyspark.ml.tuning import ParamGridBuilder
regParamList = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5]
grid = ParamGridBuilder().addGrid(lr.regParam, regParamList).build()

# The resulting object is simply a list of parameter maps:
grid


# ## Tuning the hyperparameters using holdout cross-validation

# For large datasets, holdout cross-validation will be more efficient.  Use the
# [TrainValidationSplit](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.tuning.TrainValidationSplit)
# class to specify holdout cross-validation:
from pyspark.ml.tuning import TrainValidationSplit
validator = TrainValidationSplit(estimator=lr, estimatorParamMaps=grid, evaluator=evaluator, trainRatio=0.75, seed=54321)

# Use the `fit` method to find the best set of hyperparameters:
%time cv_model = validator.fit(train)

# The resulting model is an instance of the [TrainValidationSplitModel]() class:
type(cv_model)

# The cross-validation results are stored in the `validationMatrics` attribute:
cv_model.validationMetrics
def plot_holdout_results(model):
  plt.plot(regParamList, model.validationMetrics)
  plt.title('Hyperparameter Tuning Results')
  plt.xlabel('Regularization Parameter')
  plt.ylabel('Validation Metric')
  plt.show()
plot_holdout_results(cv_model)

# The `bestModel` attribute is an instance of the `LinearRegressionModel` class:
type(cv_model.bestModel)

# **Note:** The model is rerun on the entire dataset using the best set of hyperparameters.

# The usual attributes and methods are available:
cv_model.bestModel.intercept
cv_model.bestModel.coefficients
cv_model.bestModel.summary.rootMeanSquaredError
cv_model.bestModel.evaluate(test).r2


# ## Tune hyperparameters using k-fold cross-validation

# For small datasets k-fold cross-validation will be more accurate.  Use the
# [CrossValidator]()
# class to specify the k-fold cross-validation:
from pyspark.ml.tuning import CrossValidator
kfold_validator = CrossValidator(estimator=lr, estimatorParamMaps=grid, evaluator=evaluator, numFolds=3, seed=54321)
%time kfold_model = kfold_validator.fit(train)

# The result is an instance of the [CrossValidatorModel]() class:
type(kfold_model)

# The cross-validation results are stored in the `avgMetrics` attribute:
kfold_model.avgMetrics
def plot_kfold_results(model):
  import matplotlib.pyplot as plt
  plt.plot(regParamList, model.avgMetrics)
  plt.title('Hyperparameter Tuning Results')
  plt.xlabel('Regularization Parameter')
  plt.ylabel('Validation Metric')
  plt.show()
plot_kfold_results(kfold_model)

# The `bestModel` attribute contains the model based on the best set of hyperparameters.
# In this case, it is an instance of the `LinearRegressionModel` class:
type(kfold_model.bestModel)

# Compute the performance of the performance of the best model on the test dataset:
kfold_model.bestModel.evaluate(test).r2

# ## Exercises

# Create a parameter grid that searches over regularization type (lasso or ridge)
# as well as the regularization parameter.


# ## Cleanup

# Stop the SparkSession:
spark.stop()


# ## References

# [Model Selection and hyperparameter tuning](http://spark.apache.org/docs/latest/ml-tuning.html)

# [pyspark.ml.tuning module](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml.tuning)

