# # Exploring data

# * Loading data into a pandas DataFrame
# * Exploring a single variable


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('explore').master('local').getOrCreate()

# Load the rides data:
rides = spark.read.csv('duocar/rides_fargo.txt', sep='\t', header=True, inferSchema=True)

# Import pandas and matplotlib:
import pandas as pd
import matplotlib.pyplot as plt


# ## Loading a sample into a pandas DataFrame
rides_pd = rides.sample(withReplacement=False, fraction = 0.1, seed=12345).toPandas()
rides_pd.shape
rides_pd.dtypes
rides_pd.head(5)
rides_pd.describe()


# ## Exploring a single variable

# ### Exploring a categorical variable

# Create a frequency table:
rides.groupBy('service').count().orderBy('service').show()

# Create a bar chart:
rides.groupBy('service').count().orderBy('service').toPandas().plot(kind='bar', x='service', y='count')

# **Note:** We are assuming that the grouped DataFrame is small enough to fit in local memory.

# **Question:** Is there a way to manually specify the ordering of the categories?

# ### Exploring a continuous variable

# Compute summary statistics:
rides.describe('distance').show()

# Use aggregation functions to get additional summary statistics:
rides.agg({'distance': 'skewness', 'distance': 'kurtosis'}).show()

# **Question:** Why does this not work?

from pyspark.sql.functions import skewness, kurtosis
rides.agg(skewness('distance'), kurtosis('distance')).show()

# Compute approximate quantiles:
rides.approxQuantile('distance', probabilities=[0.0, 0.05, 0.25, 0.5, 0.75, 0.95, 1.0], relativeError=0.1)

# **Note:** Set `relativeError = 0.0` for exact (and possibly expensive) quantiles.

# A histogram is more information:
rides.select('distance').toPandas().plot(kind='hist')


# ## Exploring multiple variables


# ## Cleanup

# Stop the SparkSession:
spark.stop()