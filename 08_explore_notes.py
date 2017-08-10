# # Developer and Instructor Notes

# **Note:** This file contains snippets of code.  It will not run on its own.


# ## The `toPandas` method

# The `toPandas` method uses the pandas `from_records` method:
rides_sdf.toPandas??


# ## Conversion of Spark BooleanType with null values

# Without null values:
sdf = spark.sparkContext.parallelize([True, False]).toDF('boolean')
sdf.printSchema()
pdf = sdf.toPandas()
pdf.info()
pdf.applymap(type)

# With null values:
sdf = spark.sparkContext.parallelize([True, False, None]).toDF('boolean')
sdf.printSchema()
pdf = sdf.toPandas()
pdf.info()
pdf.applymap(type)


# ## Conversion of Spark FloatType with null values

# Without null values:
sdf = spark.sparkContext.parallelize([1.0, 2.0]).toDF('float')
sdf.printSchema()
pdf = sdf.toPandas()
pdf.info()
pdf.applymap(type)

# With null values:
sdf = spark.sparkContext.parallelize([1.0, 2.0, None]).toDF('float')
sdf.printSchema()
pdf = sdf.toPandas()
pdf.info()
pdf.applymap(type)


# ## The pandas `describe` method

# The results of the pandas `describe` method are data type dependent.

# String
rides_pdf['ride_id'].describe()

# Boolean
rides_pdf['cancelled'].describe()

# Integer
rides_pdf['utc_offset'].describe()

# Float

# Datetime

# Date


# ## A bar chart using pandas plotting functionality

# Create a bar chart using pandas built-in matplotlib functionality:
rides_sdf.groupBy("service").count().orderBy("service").toPandas().plot(kind="bar", x="service", y="count")


# ## Passing dictionaries to the `agg` method

# This returns what I expect:
rides_sdf.agg({"distance": "skewness", "duration": "kurtosis"}).show()

# This does not return what I expect:
rides_sdf.agg({"distance": "skewness", "distance": "kurtosis"}).show()

# This makes sense once I recall that a Python `dict` must have unique keys:
{"distance": "skewness", "distance": "kurtosis"}


# ## A histogram using the pandas plotting functionality

# Create a histogram using pandas' built-in plotting functionality:
rides_sdf.select("distance").toPandas().plot(kind="hist")


# ## Count the number of null values in each column:
rides_sdf.persist()
for column in rides_sdf.columns:
  null_count = rides_sdf.filter(col(column).isNull()).count()
  print "%-20s %10s" % (column, null_count)
