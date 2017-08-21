# # Reading and Writing Data

# Sample answers.  You can run this standalone, or execute selected statements 
# in a wider session.


# ## Setup

# Create SparkSession
from pyspark.sql import SparkSession
spark = SparkSession\
  .builder\
  .master("local")\
  .appName("read-solutions")\
  .getOrCreate()


# ## Exercises

# Read the raw driver file into a Spark DataFrame.

drivers = spark.read.csv("/duocar/raw/drivers/", header=True, inferSchema=True)

drivers.first()

# Save the driver DataFrame as a JSON file in your CDSW practice directory.

!hdfs dfs -mkdir practice
drivers.write.format("json").save("practice/drivers_json")


# Read the driver JSON file into a Spark DataFrame.

drivers_from_json = spark\
  .read\
  .format("json")\
  .load("practice/drivers_json")
  
drivers_from_json.first()

  
# ## Cleanup

# Clear stored data from practice directory
!hdfs dfs -rm -r -skipTrash practice/drivers_json

# Stop session
spark.stop()

