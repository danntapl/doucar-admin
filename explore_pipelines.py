from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('explore_pipelines').master('local[2]').getOrCreate()

joined = spark.read.parquet('/duocar/joined/')

joined_all = spark.read.parquet('/duocar/joined_all/')

def explore(df, feature, label):
  from pyspark.sql.functions import count, mean
  aggregated = df.rollup(feature).agg(count('*'), mean(label)).orderBy(feature)
  aggregated.show()
  
features = ['service',
            'driver_sex',
            'driver_ethnicity',
            'driver_student',
            'vehicle_make',
            'vehicle_model',
            'vehicle_year',
            'vehicle_color',
            'vehicle_grand',
            'vehicle_noir',
            'vehicle_elite',
            'rider_sex',
            'rider_ethnicity',
            'rider_student']

for feature in features:
  print 'Feature: %s' % feature
  explore(joined, feature, 'star_rating')

# Explore cloud cover
from pyspark.sql.functions import mean
tmp = joined_all.groupBy('CloudCover').agg(mean('star_rating')).orderBy('CloudCover').toPandas()

# ## General preprocessing

# Filter out cancelled rides
preprocessed0 = joined_all.filter(joined_all.cancelled == 0)

# Generate reviewed feature
preprocessed1 = preprocessed0.withColumn('reviewed', preprocessed0.review.isNotNull())

# Rename DataFrame
preprocessed = preprocessed1


# ## Feature transformations

# Index `vehicle_color`:
from pyspark.ml.feature import StringIndexer
indexer = StringIndexer(inputCol='vehicle_color', outputCol='vehicle_color_indexed')

# Create dummy variables for `vehicle_color_indexed`:
from pyspark.ml.feature import OneHotEncoder
encoder = OneHotEncoder(inputCol='vehicle_color_indexed', outputCol='vehicle_color_encoded')

# Assemble Features
from pyspark.ml.feature import VectorAssembler
features = ['vehicle_year', 'vehicle_color_encoded', 'reviewed', 'CloudCover']
assembler = VectorAssembler(inputCols=features, outputCol='features')

# Specify classification algorithm (estimator)
from pyspark.ml.classification import RandomForestClassifier
classifier = RandomForestClassifier(featuresCol='features', labelCol='star_rating')

# Specify evaluator
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
evaluator = MulticlassClassificationEvaluator(labelCol='star_rating', metricName='accuracy')

# Specify hyperparameter grid
from pyspark.ml.tuning import ParamGridBuilder
maxDepthList = [5, 10, 20]
numTreesList = [20, 50, 100]
subsamplingRateList = [0.5, 1.0]
paramGrid = ParamGridBuilder()\
  .addGrid(classifier.maxDepth, maxDepthList)\
  .addGrid(classifier.numTrees, numTreesList)\
  .addGrid(classifier.subsamplingRate, subsamplingRateList)\
  .build()

# Specify validator
from pyspark.ml.tuning import TrainValidationSplit
validator = TrainValidationSplit(estimator=classifier, estimatorParamMaps=paramGrid, evaluator=evaluator)

# Specify Pipeline
from pyspark.ml import Pipeline
stages = [indexer, encoder, assembler, validator]
pipeline = Pipeline(stages=stages)

# Fit PipelineModel
pipeline_model = pipeline.fit(preprocessed)

# Apply Pipeline Model
classified = pipeline_model.transform(preprocessed)
#classified.select('service', 'service_indexed', 'distance', 'driver_student', 'features').show()

# **Note:** In this case, VectorAssembler converts distance from an integer to a double
# and converts student from boolean to double.

# Confusion matrix
classified.crosstab('prediction', 'star_rating').show()

# Create baseline prediction (always predict five-star rating)
from pyspark.sql.functions import lit
classified2 = classified.withColumn('prediction_baseline', lit(5.0))
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
evaluator = MulticlassClassificationEvaluator(labelCol='star_rating', metricName='accuracy')
evaluator.setPredictionCol('prediction_baseline').evaluate(classified2)
evaluator.setPredictionCol('prediction').evaluate(classified2)

# Probe stages
pipeline_model.stages
[type(x) for x in pipeline_model.stages]

# Access stage
type(pipeline_model.stages[0])
pipeline_model.stages[0].labels

# Access classifier model
classifier_model = pipeline_model.stages[3]
classifier_model.bestModel.featureImportances

spark.stop()