# # Connecting to Spark

# This script contains solutions to the exercises.  You can run this script in
# a new session or execute selected statements in an existing session.  Note
# that there is generally more than one way to solve a problem.


# ## Exercises

# (1) Create a SparkSession that connects to Spark in local mode.  Configure
# the SparkSession to use two cores.  
from pyspark.sql import SparkSession
spark = SparkSession \
  .builder \
  .master("local") \
  .appName("connect_solutions") \
  .getOrCreate()

# (2) Create a small DataFrame.
person = spark \
  .createDataFrame([(42, "Sarah", 11), (43, "Aaron", 11), (44, "Eddie", 12)], \
                   schema = ["id", "name", "city_id"])

# Print the schema.
person.printSchema()

# View the DataFrame.
person.show()

# Count the number of records.
person.count()

# (3) Explore the Spark Job UI.

# * The Spark Job UI is accessible from the grid pulldown menu in the
# upper-right of CDSW.

# (4) Stop the SparkSession.
spark.stop()

# (5) Explore the Spark History Server UI.

# * The Spark History Server UI is accessible from the grid pulldown menu in
# the upper-right of CDSW.
