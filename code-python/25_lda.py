# # Topic Modeling with Latent Dirichlet Allocation (LDA)

# Copyright © 2010–2017 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.

# In this module we use latent Dirichlet allocation (LDA) to look for topics
# in the ride reviews.


# ## Setup

# Import useful packages, modules, classes, and functions:
from __future__ import print_function
from pyspark.sql import SparkSession

# Create a SparkSession:
spark = SparkSession.builder.master("local").appName("lda").getOrCreate()

# Read the processed reviews data:
reviews = spark.read.parquet("myduocar/clustering_data")

# **Note:** This data is produced by the `20_extract.py` script.


# ## Specify and fit an LDA model

# Specify an LDA model:
from pyspark.ml.clustering import LDA
lda = LDA(featuresCol="words_vectorized", k=2)
type(lda)

# Print the full set of parameters:
print(lda.explainParams())

# Fit the LDA model:
lda_model = lda.fit(reviews)
type(lda_model)


# ## Query the model

lda_model.describeTopics().show(truncate=False)

vocab = [u'driver',
 u'ride',
 u'air',
 u'ride.',
 u'car',
 u'really',
 u'freshener',
 u'vehicle',
 u'due',
 u'freshener.']

def f(x):
  print([vocab[i] for i in x[1]])
  
lda_model.describeTopics().foreach(f)

lda_model.estimatedDocConcentration()

lda_model.logLikelihood(reviews)

lda_model.logPerplexity(reviews)

lda_model.topicsMatrix()

reviews_with_topics = lda_model.transform(reviews)

reviews_with_topics.head(5)

lda_model.vocabSize()


# ## Apply the LDS model

reviews_with_topics = lda_model.transform(reviews)
reviews_with_topics.head(5)


# ## Exercises

# None


# ## Cleanup

# Stop the SparkSession:
spark.stop()


# ## References

# [Wikipedia - Latent Dirichlet allocation](https://en.wikipedia.org/wiki/Latent_Dirichlet_allocation)

# [Spark Documentation - Latent Dirichlet allocation](http://spark.apache.org/docs/latest/ml-clustering.html#latent-dirichlet-allocation-lda)

# [Spark Python API - LDA class](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.clustering.LDA)

# [Spark Python API - LDAModel class](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.clustering.LDAModel)

# [Spark Python API - LocalLDAModel class](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.clustering.LocalLDAModel)

# [Spark Python API - DistributedLDAModel class](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.clustering.DistributedLDAModel)

