# # Summarizing and grouping data

# * Summarizing data
# * Grouping data


# ## Setup

# Create a SparkSession.
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('NAME').master('local').getOrCreate()

# Load the rider data.
riders = spark.read.csv('duocar/riders_fargo.txt', sep='\t', header=True, inferSchema=True)

# ## Summarizing Data

riders.describe('home_lat').show()

# ## Compute the number of rows (observations)
from pyspark.sql.functions import count
riders.agg(count('home_lat')).show()
riders.groupBy('student').agg(count('home_lat')).show()

# ## Compute the number of distinct rows (observations)
from pyspark.sql.functions import countDistinct
riders.agg(countDistinct('home_lat')).show()
riders.groupBy('student').agg(countDistinct('home_lat')).show()

# ## Compute the first and last value
from pyspark.sql.functions import approx_count_distinct
riders.agg(approx_count_distinct('home_lat')).show()

from pyspark.sql.functions import first,last
riders.agg(first('home_lat'), last('home_lat')).show()

# ## Compute minimum and maximum
from pyspark.sql.functions import min, max
riders.agg(min('home_lat'), max('home_lat')).show()

# ## Compute sum and sum of distinct values.

# **DEVNOTE:** When would we use `sumDistinct`?

# ## Compute skewness and kurtosis
from pyspark.sql.functions import skewness, kurtosis
riders.agg(skewness('home_lat'), kurtosis('home_lat')).show()

# ## Grouping 

riders.agg({'home_lat': 'count'}).show()

#riders.agg(count('home_lat')).show()

# ## Cross-tabulation

riders.groupby('sex', 'ethnicity').count().orderBy('sex', 'ethnicity').show()

# ## Pivoting

# Why doesn't this work?  Null values?
#riders.groupBy('sex').pivot('ethnicity').agg({'home_lat': 'avg'}).show()

riders.groupby('sex', 'ethnicity').count().show()

# Drop the missing values:
riders.dropna().groupby('sex','ethnicity').count().show()

riders.dropna().groupby('sex').pivot('ethnicity').count().show()

riders.dropna().groupby('sex').pivot('ethnicity').agg({'home_lat': 'count'}).show()

riders.dropna().groupby('sex').pivot('ethnicity').mean('home_lat').show()

riders.dropna().groupby('sex').pivot('ethnicity').agg({'home_lat': 'mean'}).show()

# **Note:** The `pivot` method does not like missing values.

# **DEVNOTE:**  Is there a way to change the print precision?

# ## Cleanup

# Stop the SparkSession.
spark.stop()