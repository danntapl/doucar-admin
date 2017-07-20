# # Transforming Data - Transforming DataFrame Columns

# In this module we demonstrate how to transform DataFrame columns.

# * Working with numerical columns
# * Working with string columns
# * Working with datetime columns
# * Working with Boolean columns
# * Working with missing values


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("transform2").getOrCreate()

# Load the raw rides data:
rides = spark.read.csv("/duocar/raw/rides/", header=True, inferSchema=True)

# Load the raw driver data:
drivers = spark.read.csv("/duocar/raw/drivers", header=True, inferSchema=True)

# Load the raw rider data:
riders = spark.read.csv("/duocar/raw/riders/", header=True, inferSchema=True)


# ## Working with numerical columns

# ### Example 1: Converting ride distance from meters to miles

from pyspark.sql.functions import round
rides.select("distance", round(rides.distance / 1609.344, 2).alias("distance_in_miles")).show(5)

# **Notes:**
# * We have used the fact that 1 mile = 1609.344 meters.
# * We have used the `round` function to round the result to two decimal places.
# * We have used the `alias` method to rename the column.

# To add a new column use the `withColumn` method with a new column name:
rides.withColumn("distance_in_miles", round(rides.distance / 1609.344, 2)).show(5)

# To replace the existing column use the `withColumn` method with the existing
# column name:
rides.withColumn("distance", round(rides.distance / 1609.344, 2)).show(5)

# ### Example 2: Converting the ride id from an integer to a string

# Convert the `id` key to a left-zero-padded string:
from pyspark.sql.functions import format_string
rides.select("id", format_string("%010d", "id").alias("id_fixed")).show(5)

# **Note:** We have used the [printf format
# string](https://en.wikipedia.org/wiki/Printf_format_string) "%010d" to
# achieve the desired format.

# ### Example 3: Converting the student flag from an integer to a Boolean

riders.select("student", (riders["student"] == 1).alias("student_boolean")).show(5)


# ## Working with string columns

# ### Example 4: Normalizing rider sex

# Trim whitespace and convert rider sex to uppercase:
from pyspark.sql.functions import trim, upper
riders.select("sex", upper(trim(riders.sex)).alias("gender")).show(5)

# ### Example 5: Extracting Census Block Group from the rider's Census Block

# The [Census Block Group](https://en.wikipedia.org/wiki/Census_block_group) is
# the first 12 digits of the [Census
# Block](https://en.wikipedia.org/wiki/Census_block):
from pyspark.sql.functions import substring
riders.select("home_block", substring("home_block", 1, 12).alias("home_block_group")).show(5)

# ### Example 6: Regular Expressions

# Use a regular expression to extract the Census Block Group:
from pyspark.sql.functions import regexp_extract
riders.select("home_block", regexp_extract(riders.home_block.cast("string"), "^(\d{12}).*", 1).alias("home_block_group")).show(5)


# ## Working with datetime columns

# ### Example 7: Fix birth date

from pyspark.sql.functions import to_date
riders.select("birth_date", to_date("birth_date").alias("birth_date_fixed")).show(5)

# **Note:** We could use the `withColumn` method as above to add a new column or replace an existing one.

# ### Example 8: Compute rider age

from pyspark.sql.functions import to_date, current_date, months_between, floor
riders.select("birth_date", current_date().alias("today"))\
    .withColumn("age", floor(months_between("today", "birth_date") / 12))\
    .show(5)

# **Note:** Spark implicity casts `birth_date` or `today` as necessary.  It is
# probably safer to explicity cast one of these columns before computing the
# number of months between.

# ### Example 9: Fix ride date and time

from pyspark.sql.functions import unix_timestamp, from_unixtime
rides.select("date_time", from_unixtime(unix_timestamp("date_time", "yyyy-MM-dd HH:mm")).alias("date_time_fixed")).show(5)

# **Note:** `date_time` is a string whereas `date_time_fixed` is a timestamp.


# ## Working with Boolean columns

# ### Example 10: Create a Boolean column
riders.select("student", (riders.student == 1).alias("student_boolean")).show(5)

# **Note:** `riders.student == 1` is called a *Boolean Column expression*.

# We can filter on a Boolean column
riders.select("student", (riders.student == 1).alias("student_boolean")).filter("student_boolean").show(5)

# ### Example 11: Multiple Boolean Column expressions

# Predefine the Boolean Column expressions:
studentFilter = riders.student == 1
maleFilter = riders["sex"] == "male"

# Combine using the AND operator:
riders.select("student", "sex", studentFilter & maleFilter).show(15)

# Combine using the OR operator:
riders.select("student", "sex", studentFilter | maleFilter).show(15)

# Note the difference in how nulls are treated in the computation.

# ### Example 12: Using multiple boolean expressions in a filter

riders.filter(maleFilter & studentFilter).select("student", "sex").show(5)

# This is equivalent to
riders.filter(maleFilter).filter(studentFilter).select("student", "sex").show(5)


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
riders_selected.fillna("UNKNOWN", ["sex", "ethnicity"]).show(25)

# Replace missing values with different values:
riders_missing = riders_selected.na.fill({"sex": "UNKNOWN", "ethnicity": "MISSING"})
riders_missing.show(25)

# **Note**: `fillna` and `na.fill` are equivalent.

# Replace arbitrary values with a common value:
riders_missing.replace(["UNKNOWN", "MISSING"], "NA", ["sex", "ethnicity"]).show(25)

# Replace arbitrary values with different values:
riders_missing.replace({"UNKNOWN": "NA", "MISSING": "NO RESPONSE"}, ["sex", "ethnicity"]).show(25)

# **Note:** `replace` and `na.replace` are equivalent.  `replace` can be used to replace
# sentinel values (e.g., -9999) that represent missing values in numerical columns.

# See the
# [DataFrameNaFunctions](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameNaFunctions)
# class for more details.


# ## Exercises

# Convert the **rides.driver_id** column to a string column.

# Extract the year from the **rides.date_time** column.

# Replace the missing values in the **rides.service** column
# with "Car" for standard DuoCar service.

# Convert **rides.duration** from seconds to  minutes.

# Convert the **rides.cancelled** column to a boolean column.

# Convert the **rides.star_rating** column to a double column.


# ## Cleanup

# Stop the `SparkSession`:
spark.stop()


# ## References

# [DataFrame class](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame)

# [Column class](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.Column)

# [pyspark.sql.functions module](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions)

# [DataFrameNaFunctions class](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameNaFunctions)
