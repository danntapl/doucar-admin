# # Window Functions

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import count, sum

# Create SparkSession.
spark = SparkSession.builder.master('local').getOrCreate()

# Create DataFrame.
df = spark.range(10)
df.show()

# Create simple window specification.
ws = Window.rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Compute cumulative count and sum.
df.select('id', count('id').over(ws).alias('cum_cnt'), sum('id').over(ws).alias('cum_sum')).show()

# **TODO:** Generate number of rides in last week, month, quarter, year, etc.

spark.stop()