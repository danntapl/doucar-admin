# # Extracting, transforming, and selecting features

# In this module we demonstrate some of the functionality available in Spark MLlib
# to extract, transform, and select features for machine learning.  In particular,
# we focus on generating features from the ride review text data.


# ## TODO
# * Test on cluster
# * Add links to methods
# * Add additional analysis to explore predictive relationships


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('extract').master('local').getOrCreate()

# Read ride review data from HDFS:
reviews = spark.read.csv('/duocar/ride_reviews', sep='\t', inferSchema=True).toDF('ride_id', 'review')

# Read ride data from HDFS:
rides = spark.read.csv('/duocar/rides', sep='\t', header=True, inferSchema=True)

# Join ride data:
reviews_with_ratings = reviews.join(rides, reviews.ride_id == rides.id, 'left_outer')

# Select a subset of columns:
selected = reviews_with_ratings.select('ride_id', 'review', 'star_rating')


# ## Extract and transform features

# Use `Tokenizer` to tokenize the reviews:
from pyspark.ml.feature import Tokenizer
tokenizer = Tokenizer(inputCol='review', outputCol='words')
tokenized = tokenizer.transform(selected)
tokenized.head(5)

# **Note:** Punctuation is not being handled properly.  Use `RegexTokenizer`
# for splitting on characters other than whitespace.

# **Note:** `Tokenizer` only requires a transform method.

# Use `CountVectorizer` to compute the term frequency:
from pyspark.ml.feature import CountVectorizer
cv = CountVectorizer(inputCol='words', outputCol='words_vectorized', vocabSize=10)
cv_model = cv.fit(tokenized)
cv_model.vocabulary
vectorized = cv_model.transform(tokenized)
vectorized.head(5)

# **Note:** Alternatively, use `HashingTF` to compute the term frequency.

# **Note:** `CountVectorizer` requires a fit and transform method.

# Use `StopWordsRemover` to remove common words:
from pyspark.ml.feature import StopWordsRemover
remover = StopWordsRemover(inputCol='words', outputCol='words_removed')
removed = remover.transform(tokenized)

# Recount the words:
cv = CountVectorizer(inputCol='words_removed', outputCol='words_vectorized', vocabSize=10)
cv_model = cv.fit(removed)
cv_model.vocabulary
vectorized = cv_model.transform(removed)
vectorized.head(5)


# ## Select features

# Use `ChiSqSelector` to select top 5 features:
from pyspark.ml.feature import ChiSqSelector
selector = ChiSqSelector(featuresCol='words_vectorized', labelCol='star_rating', outputCol='selected_words', numTopFeatures=5)
selector_model = selector.fit(vectorized)
selected2 = selector_model.transform(vectorized)
selected2.head(5)

# Listed selected words:
[cv_model.vocabulary[i] for i in selector_model.selectedFeatures]

# **Note:** We don't know the strength or direction of the predictive relationships.


# ## Exercises

# Determine if increasing the vocabulary size improves the solution.

# Use `RegexTokenizer` to more cleanly tokenize the reviews.

# Use `HashingTF` rather than `CountVectorizer` to generate the term-frequency vectors.


# ## Cleanup

# Stop the SparkSession:
spark.stop()


# ## References

# [Extracting, transforming, and selecting features](http://spark.apache.org/docs/latest/ml-features.html)
