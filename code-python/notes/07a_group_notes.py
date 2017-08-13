# # Summarizing and grouping data

# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("group").getOrCreate()

# Load the joined rides data:
rides = spark.read.parquet("/duocar/joined/")
rides.persist()


# Display of rollup, to help visualize the result

def indent_null(test_variable):
  if test_variable is None:
    return "      "
  else:
    return ""
  
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
indent_null_udf = udf(indent_null, returnType=StringType())

from pyspark.sql.functions import col, concat, count
rides\
  .rollup("rider_student", "service")\
  .agg(count("*").alias("count"))\
  .withColumn("display_count",\
              concat(col("count")\
                     ,indent_null_udf(col("rider_student"))\
                     ,indent_null_udf(col("service"))))\
  .drop("count")\
  .orderBy("rider_student", "service", ascending=False)\
  .show()
  
  
# Another solution to the problem where pivot doesn't like nulls:
# Convert null to the string "Not given" before the pivot.

def null_ethnicity(e_value):
  if e_value is None:
    return "Not given"
  else:
    return e_value

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
fix_ethnicity_udf = udf(null_ethnicity, returnType=StringType())

from pyspark.sql.functions import col
rides\
  .withColumn("rider_ethnicity_fixed", \
              fix_ethnicity_udf(col("rider_ethnicity")))\
  .groupBy("rider_sex")\
  .pivot("rider_ethnicity_fixed")\
  .count()\
  .show()

spark.stop()
