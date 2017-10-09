# # Extracting, transforming, and selecting features

# Copyright © 2010–2017 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.

# In this module we demonstrate some of the functionality available in Spark
# MLlib to extract, transform, and select features for machine learning.  In
# particular, we generate features from the ride review text that can be used
# to predict the ride rating.  We cover only a small subset of the available
# transformations; we will cover additional transformations in subsequent
# modules.


# ## Setup

# Load and attach packages:
library(sparklyr)
library(dplyr)

# Connect to Spark:
config <- spark_config()
config$spark.driver.host <- Sys.getenv("CDSW_IP_ADDRESS")
spark <- spark_connect(master = "local", app_name = "extract", config = config)

# Read the ride review data from HDFS:
reviews = spark_read_parquet(sc = spark, name = "reviews", path = "/duocar/clean/ride_reviews/")

# Read the ride data from HDFS:
rides = spark_read_parquet(sc = spark, name = "rides", path = "/duocar/clean/rides/")

# We only want the rides with reviews, so left outer join `reviews` and
# `rides`:
joined <- reviews %>% left_join(rides, by = c("ride_id" = "id"))

# Select the subset of columns in which we are interested:
reviews_with_ratings <- joined %>% select(ride_id, review, star_rating)
reviews_with_ratings %>% print(width = 100)

# Verify that `star_rating` is not missing:
reviews_with_ratings %>% group_by(star_rating) %>% tally() %>% arrange(star_rating)


# ## Extracting and transforming features

# The ride reviews are not in a form amenable to machine learning algorithms.
# sparklyr provides acceess to a number of Spark MLlib feature extractors and feature transformers
# to preprocess the ride reviews into a form appropriate for modeling.

# Use the
# [ft_tokenizer](https://spark.rstudio.com/reference/ft_tokenizer.html)
# function to tokenize the reviews:
tokenized <- reviews_with_ratings %>% ft_tokenizer(input.col = "review", output.col = "words")
tokenized %>% pull(words) %>% head(5)

# **Note:** This produces a list of lists.

vectorized <- tokenized %>% ft_count_vectorizer(input.col = "words", output.col = "words_vectorized")
vectorized %>% pull(words_vectorized) %>% head(5)

# **Note:** sparklyr does not seem to store the result in a sparse format.

vocabulary <- tokenized %>% ft_count_vectorizer(input.col = "words", output.col = "words_vectorized", vocabulary.only=TRUE)

### BEGIN INSTRUCTOR NOTE
# **Question:** Is the word vector still stored in sparse format 
### END INSTRUCTOR NOTE

removed <- tokenized %>% ft_stop_words_remover(input.col = "words", output.col = "words_removed")
removed %>% pull(words_removed) %>% head(5)

# **Note:** There does not appear to be a way to modify the list of stop words.

removed %>% ft_count_vectorizer(input.col = "words_removed", output.col = "words_vectorized", vocabulary.only=TRUE) %>% head(10)
vectorized <- removed %>% ft_count_vectorizer(input.col = "words_removed", output.col = "words_vectorized")
vectorized %>% pull(words_vectorized) %>% head(5)

# **Note:** sparlyr do not provide the chi-squared selector.

# **Question:** Why doesn't this work?
# ```r
# nb_model <- vectorized %>% ml_naive_bayes(star_rating ~ words_vectorized)
# ```

nb_model <- vectorized %>% ml_naive_bayes(response = "star_rating", features = "words_vectorized")

spark_disconnect(spark)

### END OF R CODE.  PYTHON CODE BELOW.

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
vectorized.select("words", "words_vectorized").head(5)


# **Note:** The resulting word vector is stored in sparse format.

# **Note:** Alternatively, use the
# [HashingTF](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.HashingTF)
# class to compute the term frequency.

# **Note:** Our limited vocabulary includes a number of common words such as
# "the" that we do not expect to be predictive:
list(enumerate(vectorizer_model.vocabulary))
# Spark MLlib provides a transformer to remove these so-called "stop words".

# Use the
# [StopWordsRemover](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.StopWordsRemover)
# class to remove common words:
from pyspark.ml.feature import StopWordsRemover
remover = StopWordsRemover(inputCol="words", outputCol="words_removed")
remover.getStopWords()[:5]
removed = remover.transform(tokenized)
removed.select("words", "words_removed").head(5)


# Recount the words:
vectorizer = CountVectorizer(inputCol="words_removed", outputCol="words_vectorized")
vectorizer_model = vectorizer.fit(removed)
vectorized = vectorizer_model.transform(removed)
vectorized.select("words_removed", "words_vectorized").head(5)

# **Note:** Our vocabulary seems more reasonable now:
list(enumerate(vectorizer_model.vocabulary))


# ## Selecting features

# We have generated a potentially large number of features.  How do we
# distinguish the relevant features from the irrelevant ones?  Spark MLlib
# provides the
# [ChiSqSelector](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.ChiSqSelector)
# estimator to address this challenge when the label is categorical.

# Use `ChiSqSelector` to select the top 5 features:
from pyspark.ml.feature import ChiSqSelector
selector = ChiSqSelector(featuresCol="words_vectorized", labelCol="star_rating", outputCol="words_selected", numTopFeatures=5)
selector_model = selector.fit(vectorized)
selected = selector_model.transform(vectorized)
selected.select("words_removed", "words_selected").head(5)

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
naive_bayes = NaiveBayes(featuresCol="words_selected", labelCol="star_rating_indexed")
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
