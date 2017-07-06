# # Transforming Data - Transforming DataFrames

# In this module we will demonstrate some basic transformations:
# * Working with columns
#   * Selecting columns
#   * Adding columns
#   * Dropping columns
#   * Changing the column name
#   * Changing the column type
# * Working with rows
#   * Ordering rows
#   * Keeping a fixed number of rows
#   * Keeping distinct rows
#   * Filtering rows
#   * Sampling rows

# **Note:** This is often more than one way to do these transformations.


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local').appName('transform1').getOrCreate()

# **Developer Note:** We are running Spark locally to reduce overhead for these small datasets.

# Load the rider data from HDFS:
riders = spark.read.csv('duocar/riders_fargo.txt', sep='\t', header=True, inferSchema=True)
riders.show(5)


# ## Working with Columns


# ### Selecting Columns

# Use the `select` method to select specific columns:
riders.select('birth_date', 'student', 'sex').show(5)

# Use * to select all columns:
riders.select('*').show(5)

# ### Adding Columns

# Use the `withColumn` method to add a new column:
riders.select('student').withColumn('student_boolean', riders.student == 1).show()

# **Notes:**
# * We have chained the `select` and `withColumn` methods.
# * We have introduced another way to access a column: `riders.student`.
# * We have introduced a Boolean expression `riders.student == 1`.

# ### Dropping Columns

# Use the `drop` method to drop specific columns:
riders.drop('first_name', 'last_name', 'ethnicity').show(5)

# ### Changing the column name

# Use the `withColumnRenamed` method to rename a column:
riders.withColumnRenamed('start_date', 'join_date').printSchema()

# Chain multiple methods to rename more than one column:
riders.withColumnRenamed('start_date', 'join_date').withColumnRenamed('sex', 'gender').printSchema()

# ### Changing the column type

# Recall that `home_block` was read in as a (long) integer:
riders.printSchema()

# Use the `withColumn` (DataFrame) method in conjuction with the `cast` (Column) method to change its type:
riders.withColumn('home_block', riders.home_block.cast('string')).printSchema()

# **Note:** If we need to change the name and/or type of many columns, then we may want to consider specifying the schema on read.


# ## Working with rows

# ### Ordering rows

# Use the `sort` or `orderBy` method to sort a DataFrame by particular columns:
riders.select('birth_date', 'student').sort('birth_date', ascending=True).show()
riders.select('birth_date', 'student').orderBy('birth_date', ascending=False).show()

# Use the `asc` or `desc` column method instead of the `ascending` arguement:
riders.select('birth_date', 'student').orderBy(riders.birth_date.desc()).show()

# **Note:** We will see the `sortWithinPartions` method later.

# ### Selecting a fixed number of rows

# Use the `limit` method to select a fixed number of rows:
riders.select('student', 'sex').limit(5).show()

# ### Selecting distinct rows

# Use the `distinct` method to select distinct rows:
riders.select('student', 'sex').distinct().show()

# You can also use the `dropDuplicates` method:
riders.select('student', 'sex').dropDuplicates().show()

# ### Filtering rows

# Use the `filter` or `where` method along with a predicate function to select particular rows:
riders.filter(riders.student == 1).count()
riders.where(riders.sex == 'female').count()
riders.filter(riders.student == 1).where(riders.sex == 'female').count()

# **Note:** A predicate function returns True or False as output.

# ### Sampling rows

# Use the `sample` to select a random sample of rows with or without replacement.
riders.count()
riders.sample(withReplacement=False, fraction=0.1, seed=12345).count()

# Use the `sampleBy` method to select a stratefied random sample:
riders.groupBy('sex').count().show()
riders.sampleBy('sex', fractions={'male': 0.2, 'female': 0.8}, seed=54321).groupBy('sex').count().show()
# Here we have sampled 20% of male riders and 80% of female riders.

# ## Cleanup

# Stop the `SparkSession`.
spark.stop()