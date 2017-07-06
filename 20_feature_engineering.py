# # Feature Engineering and Preprocessing

# * Feature engineering (extraction)
# * Feature preprocessing (transformation)


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('engineering').master('local').getOrCreate()

# Read ride reviews from HDFS:
reviews_raw = spark.read.csv('/duocar/ride_reviews', sep='\t', inferSchema=True)

# Rename the columns:
reviews = reviews_raw.toDF('ride_id', 'review')

# Tokenize the reviews:
from pyspark.ml.feature import Tokenizer
tokenizer = Tokenizer(inputCol='review', outputCol='words')
reviews_tokenized = tokenizer.transform(reviews)
reviews_tokenized.head(10)

# Count words
from pyspark.ml.feature import CountVectorizer
cv = CountVectorizer(inputCol='words', outputCol='words_vectorized', vocabSize=10)
cv_model = cv.fit(reviews_tokenized)
reviews_vectorized = cv_model.transform(reviews_tokenized)
reviews_vectorized.head(10)

# Print the vocabulary:
cv_model.vocabulary

# Compute term frequency using HashingTF

# TBD


# from pyspark.ml.feature import Hashing
# ## Exercises


# ## Cleanup

# Stop the SparkSession:
spark.stop()


# **Note:** `spark.ml` does not provide text segmentation functionality.


# ## TODO
# * Task