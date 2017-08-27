# # Extracting, transforming, and selecting features

# In this module we demonstrate some of the functionality available in Spark
# MLlib to extract, transform, and select features for machine learning.  In
# particular, we generate features from the ride review text that can be used
# to predict ride rating.  We cover only a small subset of the available
# transformations; we will cover additional transformations in subsequent
# modules.


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("extract").getOrCreate()

# Read the ride review data from HDFS:
reviews = spark.read.parquet("/duocar/clean/ride_reviews/")

# Read the ride data from HDFS:
rides = spark.read.parquet("/duocar/clean/rides/")

# Join the ride review data with the ride data:
joined = reviews.join(rides, reviews.ride_id == rides.id, "left_outer")

# **Note:** We want only those rides with reviews.

# Select the subset of columns in which we are interested:
reviews_with_ratings = joined.select("ride_id", "review", "star_rating")
reviews_with_ratings.head(5)


# ## Extracting and transforming features

# The ride reviews are not in a form amenable to machine learning algorithms.
# Spark MLlib provides a number of feature extractors and feature transformers
# to preprocess the ride reviews into a form appropriate for modeling.

# Use the
# [Tokenizer](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.Tokenizer)
# class to tokenize the reviews:
from pyspark.ml.feature import Tokenizer
tokenizer = Tokenizer(inputCol="review", outputCol="words")
tokenized = tokenizer.transform(reviews_with_ratings)
tokenized.head(5)

# **Note:** `Tokenizer` is a
# [Transformer](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.Transformer)
# since it takes a DataFrame as input and returns a DataFrame as output via its
# `transform` method.

# **Note:** Punctuation is not being handled properly.  Use the
# [RegexTokenizer](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.RegexTokenizer)
# class to split on characters other than whitespace.

# Use the
# [CountVectorizer](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.CountVectorizer)
# class to compute the term frequency:
from pyspark.ml.feature import CountVectorizer
vectorizer = CountVectorizer(inputCol="words", outputCol="words_vectorized", vocabSize=10)
vectorizer_model = vectorizer.fit(tokenized)
vectorized = vectorizer_model.transform(tokenized)
vectorized.head(5)

# **Note:** `CountVectorizer` is an
# [Estimator](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.Estimator)
# since it takes a DataFrame as input and returns a `Transformer` as output via
# its `fit` method.  In this case, the resulting transformer is an instance of
# the
# [CountVectorizerModel](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.CountVectorizerModel)
# class.

# **Note:** The resulting word vector is stored in sparse format.

# **Note:** Alternatively, use the
# [HashingTF](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.HashingTF)
# class to compute the term frequency.

# **Note:** Our limited vocabulary includes a number of common words such as
# "the" that we do not expect to be predictive:
vectorizer_model.vocabulary
# Spark MLlib provides a transformer to remove these so-called "stop words".

# Use the
# [StopWordsRemover](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.StopWordsRemover)
# class to remove common words:
from pyspark.ml.feature import StopWordsRemover
remover = StopWordsRemover(inputCol="words", outputCol="words_removed")
remover.getStopWords()[:5]
removed = remover.transform(tokenized)
removed.head(5)

# Recount the words:
vectorizer = CountVectorizer(inputCol="words_removed", outputCol="words_vectorized", vocabSize=10)
vectorizer_model = vectorizer.fit(removed)
vectorized = vectorizer_model.transform(removed)
vectorized.head(5)

# **Note:** Our vocabulary seems more reasonable now:
vectorizer_model.vocabulary

# ## Selecting features

# We have generated a potentially large number of features.  How do we
# distinguish the relevant features from the irrelevant ones?  Spark MLlib
# provides the
# [ChiSqSelector](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.ChiSqSelector)
# estimator to address this challenge when the label is categorical.

# Use `ChiSqSelector` to select the top 5 features:
from pyspark.ml.feature import ChiSqSelector
selector = ChiSqSelector(featuresCol="words_vectorized", labelCol="star_rating", outputCol="selected_words", numTopFeatures=5)
selector_model = selector.fit(vectorized)
selected = selector_model.transform(vectorized)
selected.head(5)

# List selected words:
[vectorizer_model.vocabulary[i] for i in selector_model.selectedFeatures]

# `ChiSqSelector` does not provide any information on relative strength or
# direction of the predictive relationships.  We can apply some Spark SQL magic
# to compute the average ride rating based on the presence or absence of each
# vocabulary word in a review:
from pyspark.sql.functions import array_contains, count, mean
for word in vectorizer_model.vocabulary:
  print "**** word = %s ****\n" % word
  vectorized \
    .select(array_contains("words_removed", word).alias("contains_word"), "star_rating") \
    .groupBy("contains_word") \
    .agg(count("star_rating"), mean("star_rating")) \
    .show()

# These results appear to be consistent with the results of `ChiSqSelector`.


# ## Predict ride ratings

# As a preview of topics to come, let us build a Naive Bayes classifier to
# predict the ride rating from the word vector.  Before doing so, we need to
# preprocess the ride rating.

# Classification algorithms in Spark MLlib generally assume that the label is a
# zero-based integer.  Let us subtract one from the original star_rating to
# comply:
from pyspark.sql.functions import col
indexed = selected.withColumn("star_rating_indexed", col("star_rating") - 1.0)

# We can examine the mapping by applying the `crosstab` method:
indexed \
  .crosstab("star_rating", "star_rating_indexed") \
  .orderBy("star_rating_star_rating_indexed") \
  .show()

# Write data to HDFS:
indexed.write.parquet("myduocar/clustering_data", mode="overwrite")

# Now we are ready to build a simple Naive Bayes classifier:
from pyspark.ml.classification import NaiveBayes
naive_bayes = NaiveBayes(featuresCol="selected_words", labelCol="star_rating_indexed")
reviews_with_prediction = naive_bayes.fit(indexed).transform(indexed)

# Compute the *confusion matrix* via the `crosstab` method:
reviews_with_prediction \
  .crosstab("prediction", "star_rating_indexed") \
  .orderBy("prediction_star_rating_indexed") \
  .show()

# Compute the accuracy of the model:
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", \
                                              labelCol="star_rating_indexed", \
                                              metricName="accuracy")
evaluator.evaluate(reviews_with_prediction)

# It looks like we have some more work to do to improve our model.
# Five words will not suffice!


# ## Exercises

# (1) Determine if increasing the vocabulary size improves the solution.

# (2) Use the `RegexTokenizer` transformer to more cleanly tokenize the reviews.

# (3) Use the `HashingTF` estimator rather than the `CountVectorizer` estimator to generate the term-frequency vectors.


# ## Cleanup

# Stop the SparkSession:
spark.stop()


# ## References

# [Feature engineering](https://en.wikipedia.org/wiki/Feature_engineering)

# [Feature selection](https://en.wikipedia.org/wiki/Feature_selection)

# [Extracting, transforming, and selecting features](http://spark.apache.org/docs/latest/ml-features.html)
