# # Transforming Data - Transforming DataFrame Columns

# * Working with numerical columns
# * Working with string columns
# * Working with datetime columns
# * Working with Boolean columns
# * Working with missing values

# ## Setup

# Create a SparkSession
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('explore_rides').master('local').getOrCreate()

# Load the rides data
rides = spark.read.csv('duocar/rides_fargo.txt', sep='\t', header=True, inferSchema=True)

# Load the driver data
drivers = spark.read.csv('duocar/drivers_fargo.txt', sep='\t', header=True, inferSchema=True)

# Load the rider data
riders = spark.read.csv('duocar/riders_fargo.txt', sep='\t', header=True, inferSchema=True)


# ## Working with numerical columns

# ### Example 1: Converting ride distance from meters to miles

from pyspark.sql.functions import round
rides.select('distance', round(rides.distance / 1609.344, 2).alias('distance_in_miles')).show(10)

# **Notes:**
# * We have used the fact that 1 mile = 1609.344 meters.
# * We have used the `round` function to round the result to two decimal places.

# To add a new column use the `withColumn` method with a new column name:
rides.withColumn('distance_in_miles', round(rides.distance / 1609.344, 2)).show()

# To replace the existing column use the `withColumn` method with the existing column name:
rides.withColumn('distance', round(rides.distance / 1609.344, 2)).show()

# ### Example 2: Converting the ride id from an integer to a string

# Convert the `id` key to a left-zero-padded string:
from pyspark.sql.functions import format_string
rides.select('id', format_string('%010d', 'id').alias('id_fixed')).show(10)

# **Note:** We have used the [printf format string](https://en.wikipedia.org/wiki/Printf_format_string) '%010d' to achieve the desired format.

# ### Example 3: Converting the student flag from an integer to a Boolean

riders.select('student', (riders['student'] == 1).alias('student_boolean')).show(10)


# ## Working with string columns

# ### Example 4: Normalizing rider sex

# Trim whitespace and convert rider sex to uppercase:
from pyspark.sql.functions import trim, upper
riders.select('sex', upper(trim(riders.sex)).alias('gender')).show(10)

# ### Example 5: Extracting Census Block Group from the rider's Census Block

# The Census Block Group is the first 12 digits of the Census Block:
from pyspark.sql.functions import substring
riders.select('home_block', substring('home_block', 1, 12).alias('home_block_group')).show(10)

# ### Example 6: Regular Expressions

# **TODO:** Create regular expression example.


# ## Working with datetime columns

# ### Example 7: Fix birth date

from pyspark.sql.functions import to_date
riders.select('birth_date', to_date('birth_date').alias('birth_date_fixed')).show(10)

# **Note:** We could use the `withColumn` method as above to add a new column or replace an existing one.

# ### Example 8: Compute rider age

from pyspark.sql.functions import to_date, current_date, months_between, floor
riders.select('birth_date', current_date().alias('today')).withColumn('age', floor(months_between('today', 'birth_date') / 12)).show(10)

# **Note:** Spark cast `birth_date` to a `Date` type before computing the number of months between.

# ### Example 9: Fix ride date and time

from pyspark.sql.functions import unix_timestamp, from_unixtime
rides.select('date_time', from_unixtime(unix_timestamp('date_time', 'yyyy-MM-dd HH:mm')).alias('date_time_fixed')).show(10)

# **Note:** `date_time` is a string whereas `date_time_fixed` is a timestamp.


# ## Working with Boolean columns

# ### Example 10: Create a Boolean column
riders.select('student', (riders.student == 1).alias('student_boolean')).show(20)

# **Note:** `riders.student == 1` is called a *Boolean Column expression*.

# We can filter on a Boolean column
riders.select('student', (riders.student == 1).alias('student_boolean')).filter('student_boolean').show(20)

# ### Example 11: Multiple Boolean Column expressions

# Predefine the Boolean Column expressions:
studentFilter = riders.student == 1
maleFilter = riders['sex'] == 'male'

# Combine using the AND operator:
riders.select('student', 'sex', studentFilter & maleFilter).show()

# Combine using the OR operator:
riders.select('student', 'sex', studentFilter | maleFilter).show()

# Note the difference in how nulls are treated in the computation.

# ### Example 12: Using multiple boolean expressions in a filter

riders.filter(maleFilter & studentFilter).show(5)

# This is equivalent to
riders.filter(maleFilter).filter(studentFilter).show(5)


# ## Working with missing values

# ### Example X: Replace missing values in car service

# The missing values in level of service correspond to standard DuoCar service:
rides.fillna('Standard', 'service').show(10)


# ## Cleanup

# Stop the `SparkSession`:
spark.stop()