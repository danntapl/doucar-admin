# # Transforming Data - Transforming DataFrames

# In this module we demonstrate some basic transformations on DataFrames:
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
# * Working with missing values

# **Note:** There is often more than one way to do these transformations.  In
# particular, there is almost always a Spark SQL version of each
# transformation.


# ## Setup

# Create a SparkSession:

from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("transform1").getOrCreate()

# **Note:** We are running Spark in local mode to reduce overhead on these
# relatively small datasets.

# Load the rider data from HDFS:

riders = spark.read.csv("/duocar/raw/riders/", header=True, inferSchema=True)
riders.show(5)


# ## Working with Columns

# ### Selecting Columns

# Use the `select` method to select specific columns:

riders.select("birth_date", "student", "sex").show(5)

# We can specify a column in multiple ways:

riders.select("first_name").show(1)
riders.select(riders.first_name).show(1)
riders.select(riders["first_name"]).show(1)
from pyspark.sql.functions import col, column
riders.select(col("first_name")).show(1)
riders.select(column("first_name")).show(1)

# Use `*` to select all columns:

riders.select("*").show(5)

# ### Adding Columns

# Use the `withColumn` method to add a new column:
riders.select("student").withColumn("student_boolean", riders.student == 1).show()

# **Notes:**
# * We have chained the `select` and `withColumn` methods.
# * We have introduced another way to access a column: `riders.student`.
# * We have introduced a Boolean expression `riders.student == 1`.

# A SQL version of this is as follows:

riders.selectExpr("student", "student = 1 as student_boolean").show(5)

# ### Dropping Columns

# Use the `drop` method to drop specific columns:
riders.drop("first_name", "last_name", "ethnicity").show(5)

# ### Changing the column name

# Use the `withColumnRenamed` method to rename a column:
riders.withColumnRenamed("start_date", "join_date").printSchema()

# Chain multiple methods to rename more than one column:
riders.withColumnRenamed("start_date", "join_date").withColumnRenamed("sex", "gender").printSchema()

# ### Changing the column type

# Recall that `home_block` was read in as a (long) integer:
riders.printSchema()

# Use the `withColumn` (DataFrame) method in conjuction with the `cast` (Column) method to change its type:
riders.withColumn("home_block", riders.home_block.cast("string")).printSchema()

# **Note:** If we need to change the name and/or type of many columns, then we may want to consider specifying the schema on read.


# ## Working with rows

# ### Ordering rows

# Use the `sort` or `orderBy` method to sort a DataFrame by particular columns:
riders.select("birth_date", "student").sort("birth_date", ascending=True).show()
riders.select("birth_date", "student").orderBy("birth_date", ascending=False).show()

# Use the `asc` or `desc` column method instead of the `ascending` argument:

riders.select("birth_date", "student").orderBy(riders.birth_date.desc()).show()

# **Note:** You can also use the `asc` and `desc` functions.

# **Note:** We will see the `sortWithinPartions` method later.

# ### Selecting a fixed number of rows

# Use the `limit` method to select a fixed number of rows:

riders.select("student", "sex").limit(5).show()

# ### Selecting distinct rows

# Use the `distinct` method to select distinct rows:

riders.select("student", "sex").distinct().show()

# You can also use the `dropDuplicates` method:

riders.select("student", "sex").dropDuplicates().show()

# ### Filtering rows

# Use the `filter` or `where` method along with a predicate function to select particular rows:

riders.filter(riders.student == 1).count()
riders.where(riders.sex == "female").count()
riders.filter(riders.student == 1).where(riders.sex == "female").count()

# **Note:** A predicate function returns True or False as output.

# ### Sampling rows

# Use the `sample` method to select a random sample of rows with or without replacement.

riders.count()
riders.sample(withReplacement=False, fraction=0.1, seed=12345).count()

# Use the `sampleBy` method to select a stratified random sample:

riders.groupBy("sex").count().show()
riders.sampleBy("sex", fractions={"male": 0.2, "female": 0.8}, seed=54321).groupBy("sex").count().show()

# Here we have sampled 20% of male riders and 80% of female riders.


# ## Working with missing values

# Note the missing (null) values in the following DataFrame:
riders_selected = riders.select("id", "sex", "ethnicity")
riders_selected.show(25)

# Drop rows with any missing values:
riders_selected.dropna(how="any", subset=["sex", "ethnicity"]).show(25)

# Drop rows with all missing values:
riders_selected.na.drop(how="all", subset=["sex", "ethnicity"]).show(25)

# **Note**: `dropna` and `na.drop` are equivalent.

# Replace missing values with a common value:
riders_selected.fillna("OTHER/UNKNOWN", ["sex", "ethnicity"]).show(25)

# Replace missing values with different values:
riders_missing = riders_selected.na.fill({"sex": "OTHER/UNKNOWN", "ethnicity": "MISSING"})
riders_missing.show(25)

# **Note**: `fillna` and `na.fill` are equivalent.

# Replace arbitrary values with a common value:
riders_missing.replace(["OTHER/UNKNOWN", "MISSING"], "NA", ["sex", "ethnicity"]).show(25)

# Replace arbitrary values with different values:
riders_missing.replace({"OTHER/UNKNOWN": "NA", "MISSING": "NO RESPONSE"}, ["sex", "ethnicity"]).show(25)

# **Note:** `replace` and `na.replace` are equivalent.  `replace` can be used to replace
# sentinel values (e.g., -9999) that represent missing values in numerical columns.

# See the
# [DataFrameNaFunctions](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameNaFunctions)
# class for more details.


# ## Exercises

# Read the raw driver data from HDFS into a Spark DataFrame.

# How young is the youngest driver?  How old is the oldest driver?

# How many female drivers does DuoCar have?  How many non-white, female drivers?

# Create a new DataFrame without any personally identifiable information (PII).

# Replace the missing values in the **rides.service** column with "Car" for
# standard DuoCar service.


# ## Cleanup

# Stop the `SparkSession`.

spark.stop()


# ## References

# [DataFrame class](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame)

# [Column class](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.Column)

# [pyspark.sql.functions module](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions)

# [DataFrameNaFunctions class](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameNaFunctions)
