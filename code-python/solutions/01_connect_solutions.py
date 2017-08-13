# # Connecting to Spark

# Sample answers.  You can run this standalone, or execute selected statements in 
# a wider session.

# ## Exercises

# Create a SparkSession that connects to Spark in local mode.  Configure the SparkSession to use two cores.  

from pyspark.sql import SparkSession
spark = SparkSession\
  .builder\
  .master("local")\
  .appName("exercise_01")\
  .getOrCreate()


# Create a small DataFrame.  
# Print the schema.  View the DataFrame.  Count the number of records.

person = spark\
  .createDataFrame([(42, "Sarah", 11), (43, "Aaron", 11), (44, "Eddie", 12)],\
                   schema = ["id", "name", "city_id"])

person.printSchema()

person.show()

person.count()


# Explore the Spark Job UI.


# Stop the SparkSession.

spark.stop()

# Explore the Spark History Server UI.

# * (Viewable via the menu icon on the far right of the menu bar.)

# ## Finis