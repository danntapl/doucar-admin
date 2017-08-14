# # Transforming Data - Transforming DataFrames

# Sample answers.  You can run this standalone, or execute selected statements 
# in a wider session.


# ## Setup

from pyspark.sql import SparkSession
spark = SparkSession\
  .builder\
  .master("local")\
  .appName("transform1-exercise")\
  .getOrCreate()


# ## Exercises

# Read the raw driver data from HDFS into a Spark DataFrame.

drivers = spark.read.csv("/duocar/raw/drivers/", header=True, inferSchema=True)


# How young is the youngest driver?  How old is the oldest driver?

# Age in years from a birthday, from stackoverflow
from datetime import date
def calculate_age(born):
  today = date.today()
  return today.year - born.year - \
    ((today.month, today.day) < (born.month, born.day))

# Youngest driver
from pyspark.sql.functions import max, to_date
youngest = drivers\
  .withColumn("birth_date", to_date("birth_date"))\
  .select(max("birth_date").alias("birth_date"))\
  .first()["birth_date"]

youngest
calculate_age(youngest)

# Oldest driver
from pyspark.sql.functions import min, to_date
oldest = drivers\
  .withColumn("birth_date", to_date("birth_date"))\
  .select(min("birth_date").alias("birth_date"))\
  .first()["birth_date"]

oldest
calculate_age(oldest)


# How many female drivers does DuoCar have?  How many non-white, female drivers?

drivers\
  .filter(drivers.sex == "female")\
  .count()

drivers\
  .filter(drivers.sex == "female")\
  .filter(drivers.ethnicity != "White")\
  .count()
  

# Create a new DataFrame without any personally identifiable information (PII).

drivers_nopii = drivers.drop("first_name", "last_name")

# Note, it is possible that annonymized drivers can be reidentified with data
# like `birth_date`, `home_block`, and `home_lat/lon`.  So, a more rigorous
# annonymization might look like this:

from pyspark.sql.functions import year
drivers_nopii = drivers\
  .drop("first_name"\
        ,"last_name"\
        ,"home_block"\
        , "home_lat"\
        , "home_lon")\
  .withColumn("birth_year", year("birth_date"))\
  .drop("birth_date")


# Replace the missing values in the **rides.service** column with "Car" for
# standard DuoCar service.

rides = spark.read.csv("/duocar/raw/rides", header=True, inferSchema=True)
rides_fixed = rides.fillna("Car", "service")


# ## Cleanup

