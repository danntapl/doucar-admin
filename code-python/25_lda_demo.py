# # Topic Modeling with Latent Dirichlet Allocation (LDA)

# In this module we use latent Dirichlet allocation (LDA) to look for topics in
# the ride reviews.


# ## Setup

# Import useful packages, modules, classes, and functions:
from __future__ import print_function

# Create a SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("topic modeling").getOrCreate()

# Read the ride review data from HDFS:
reviews = spark.read.parquet("/duocar/clean/ride_reviews/")
reviews.head(5)


# ## Extracting and transforming features

# The ride reviews are not in a form amenable to machine learning algorithms.
# Spark MLlib provides a number of feature extractors and feature transformers
# to preprocess the ride reviews into a form appropriate for modeling.

# ### Parse the ride reviews

# Use the
# [Tokenizer](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.Tokenizer)
# class to tokenize the reviews:
from pyspark.ml.feature import Tokenizer
tokenizer = Tokenizer(inputCol="review", outputCol="words")
tokenized = tokenizer.transform(reviews)
tokenized.head(5)

# Note that punctuation is not being handled properly.  Use the
# [RegexTokenizer](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.RegexTokenizer)
# class to improve the tokenization:
from pyspark.ml.feature import RegexTokenizer
tokenizer = RegexTokenizer(inputCol="review", outputCol="words", gaps=False, pattern="[a-zA-z-']+")
tokenized = tokenizer.transform(reviews)
tokenized.head(5)

# Plot word cloud:
def plot_word_cloud(df, col):
  from pyspark.sql.functions import explode
  tmp1 = df.select(explode(col).alias('word')).groupBy('word').count()
  from pyspark import Row
  tmp2 = tmp1.rdd.map(lambda x: (x[0], x[1])).collect()
  from wordcloud  import WordCloud
  wordcloud = WordCloud().fit_words(dict(tmp2))
  import matplotlib.pyplot as plt
  plt.imshow(wordcloud, interpolation="bilinear")
  plt.axis("off")
plot_word_cloud(tokenized, "words")

# ### Count the frequency of words in each review

# Use the
# [CountVectorizer](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.CountVectorizer)
# class to compute the term frequency:
from pyspark.ml.feature import CountVectorizer
vectorizer = CountVectorizer(inputCol="words", outputCol="words_vectorized", vocabSize=100)
vectorizer_model = vectorizer.fit(tokenized)
vectorized = vectorizer_model.transform(tokenized)
vectorized.head(5)

# **Note:** The resulting word vector is stored in sparse format.

# Note that our limited vocabulary includes a number of common words such as
# "the" that we do not expect to be predictive:
vectorizer_model.vocabulary[:10]

# Spark MLlib provides a transformer to remove these so-called "stop words".
# Use the
# [StopWordsRemover](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.StopWordsRemover)
# class to remove common words:
from pyspark.ml.feature import StopWordsRemover
remover = StopWordsRemover(inputCol="words", outputCol="words_removed")
remover.getStopWords()[:10]
removed = remover.transform(tokenized)
removed.head(5)

# Plot word cloud:
plot_word_cloud(removed, "words_removed")

# Recount the words:
vectorizer = CountVectorizer(inputCol="words_removed", outputCol="words_vectorized", vocabSize=100)
vectorizer_model = vectorizer.fit(removed)
vectorized = vectorizer_model.transform(removed)
vectorized.head(5)

# Note that our vocabulary seems more reasonable now:
vectorizer_model.vocabulary[:10]


# ## Specify and fit a topic model using LDA

# Use the `LDA` class to specify an LDA model:
from pyspark.ml.clustering import LDA
lda = LDA(featuresCol="words_vectorized", k=9)

# Use the `fit` method to fit the LDA model:
lda_model = lda.fit(vectorized)


# ## Examine the topic model

# Print the top 10 words in each topic:
vocab = vectorizer_model.vocabulary
def print_top_words(x):
  print([vocab[i] for i in x[1]])
lda_model.describeTopics().foreach(print_top_words)


# ## Apply the topic model
reviews_with_topics = lda_model.transform(vectorized)
reviews_with_topics.head(5)


# ## Exercises

# (1) Determine if increasing the vocabulary size improves the solution.

# (2) Experiment with different hyperparameters for the LDA algorithm.

# (3) Use the `HashingTF` estimator rather than the `CountVectorizer` estimator
# to generate the term-frequency vectors.

# (4) Use the `NGram` transformer to consider pairs of words rather than single
# words.


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
