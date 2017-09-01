# # Exploring data

# Now that we have enhanced our ride data, we can begin a more systematic
# exploration of the relationships among the variables.  The insight gathered
# during this analysis may be used to improve DuoCar's day-to-day business
# operations or it may serve as preparation for more sophisticated analysis and
# modeling using machine learning algorithms.

# Possible work flows for big data
# * Work with all of the data on the cluster
#   * Produces accurate reports
#   * Limits analysis to tabular reports
#   * Requires more computation
# * Work with a sample of the data in local memory
#   * Opens up a wide range of tools
#   * Enables more rapid iterations
#   * Produces sampled results
# * Summarize on the cluster and visualize summarized data in local memory
#   * Produces accurate counts
#   * Allows for wide range of analysis and visualization tools

# In this module we use Spark in conjunction with some popular Python libraries
# to explore the DuoCar data.


# ## Setup

# Import some useful packages and modules:
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Create a SparkSession:
spark = SparkSession.builder.master("local").appName("explore").getOrCreate()

# Load the enhanced rides data from HDFS:
rides_sdf = spark.read.parquet("/duocar/joined")

# Create a random sample and load it into a pandas DataFrame:
rides_pdf = rides_sdf.sample(withReplacement=False, fraction=0.01, seed=12345).toPandas()

# **Note:** In this module we will use "sdf" to denote Spark DataFrames and
# "pdf" to denote pandas DataFrames.


# ## Exploring a single variable

# In this section we use Spark and Spark in conjunction with pandas,
# matplotlib, and seaborn to explore a single variable (i.e., column).  Many of
# the techniques presented here can be useful when inspecting variables too.

# ### Exploring a categorical variable

# Let us explore type of car service, which is an example of a categorical
# variable.

# We can use the `groupBy` method in Spark to create a one-way frequency table:
sdf = rides_sdf.groupBy("service").count().orderBy("service")
sdf.show()

# We can convert the grouped Spark DataFrame to a pandas DataFrame:
pdf = sdf.toPandas()
pdf

# **Note**: Remember that we are loading data into local memory.  In this case
# we are safe since the summarized DataFrame is relatively small.

# Now we can use pandas built-in plotting functionality to plot the summarized
# result:
pdf.plot.bar(x="service", y="count")

# We can produce a more aesthetically pleasing bar chart using seaborn:
order = ["Car", "Noir", "Grand", "Elite"]  # desired order of categories
sns.barplot(x="service", y="count", data=pdf, order=order)

# Alternatively, we can create a bar chart from the sampled pandas DataFrame:
sns.countplot(x="service", data=rides_pdf, order=order)


# ### Exploring a continuous variable

# We can use the `describe` method to compute basic summary statistics:
rides_sdf.describe("distance").show()

# and aggregate functions to get additional summary statistics:
rides_sdf.agg(skewness("distance"), kurtosis("distance")).show()

# We can use the `approxQuantile` method to compute approximate quantiles:
rides_sdf.approxQuantile("distance", probabilities=[0.0, 0.05, 0.25, 0.5, 0.75, 0.95, 1.0], relativeError=0.1)

# **Note:** Set `relativeError = 0.0` for exact (and possibly expensive)
# quantiles.

# However, a histogram is generally more informative than summary statistics.
# We can use pandas' built-in plotting functionality to get a quick-and-dirty
# histogram:
pdf = rides_sdf \
  .select("distance") \
  .dropna() \
  .sample(False, 0.01) \
  .toPandas()
pdf.plot.hist()

# We can also use seaborn to create an unnormalized histogram:
sns.distplot(pdf["distance"], kde=False)

# or a normalized histogram with rug plot and kernel density estimate:
sns.distplot(pdf["distance"], kde=True, rug=True)

# A boxplot displays much of the information computed via the `approxQuantile`
# method:
sns.boxplot(x="distance", data=pdf)


# ## Exploring a pair of variables

# ### Categorical-Categorical

# Let us explore the distribution of a rider's sex by student status.

# Create a two-way frequency table:
sdf = rides_sdf.groupBy("rider_student", "rider_sex").count().orderBy("rider_student", "rider_sex")
sdf.show()

# Convert the Spark DataFrame to a pandas DataFrame:
pdf = sdf.toPandas()
pdf

# Produce a bar chart using Seaborn:
sns.barplot(x="rider_student", y="count", hue="rider_sex", data=pdf)

# Replace missing values:
pdf = sdf.fillna("other/unknown").toPandas()
sns.barplot(x="rider_student", y="count", hue="rider_sex", data=pdf)

# ### Categorical-Continuous

# Let us explore the distribution of ride distance by rider student status.

# We can produce tabular reports in Spark:
rides_sdf \
  .groupBy("rider_student") \
  .agg(count("distance"), mean("distance"), stddev("distance")) \
  .orderBy("rider_student") \
  .show()

# Alternatively, we can produce visualizations on a sample:
sample_pdf = rides_sdf \
  .select("rider_student", "distance") \
  .sample(False, 0.01, 12345) \
  .toPandas()

# seaborn provides a number of different ways at looking at this data.  The bar
# plot and point point display the information captured in the table above.

# A bar plot:
sns.barplot(x="rider_student", y = "distance", data=sample_pdf)

# A point plot:
sns.pointplot(x="rider_student", y = "distance", data=sample_pdf)

# The following sequence of plots reveal more information about the
# distributions.

# A strip plot:
sns.stripplot(x="rider_student", y="distance", data=sample_pdf, jitter=True)

# A swarm plot:
sns.swarmplot(x="rider_student", y="distance", data=sample_pdf)

# A letter value plot:
sns.lvplot(x="rider_student", y="distance", data=sample_pdf)

# A box plot:
sns.boxplot(x="rider_student", y="distance", data=sample_pdf)

# A violin plot:
sns.violinplot(x="rider_student", y="distance", data=sample_pdf)

# ### Continuous-Continuous

# Use the `corr`, `covar_samp`, and `covar_pop` aggregate functions to measure
# the linear relationship between two variables:
rides_sdf.agg(corr("distance", "duration"),
              covar_samp("distance", "duration"),
              covar_pop("distance", "duration")).show()

sns.jointplot(x="distance", y="duration", data=rides_pdf)

sns.jointplot(x="distance", y="duration", data=rides_pdf, kind="reg")

# Maybe a quadratic fit (order = 2) is better?
sns.jointplot(x="distance", y="duration", data=rides_pdf, kind="reg", order=2)

# We can use the `pairplot` to examine several pairs at once:
tmp_pdf = rides_sdf \
  .select(col("origin_lat").cast("float"), col("dest_lat").cast("float")) \
  .sample(False, 0.01) \
  .toPandas()
sns.pairplot(tmp_pdf)

# **Note:** seaborn does not like Decimal types, so we have cast the columns to
# floats before converting to a pandas DataFrame.


# ## Exploring more than two variables

# There are numerous ways to explore more than two variables.  The appropriate
# table or plot depends on the variable types and particular question you are
# trying to answer.  We highlight a few common approaches below.

# ### N-way summary tables

# We can use grouping and aggregate functions in Spark to produce summaries.

# **Example:** Three categorical variables
rides_sdf \
  .groupby("rider_student", "rider_sex", "service") \
  .count() \
  .show()

# **Example:** Two categorical variables and one continuous variable
rides_sdf \
  .cube("rider_student", "rider_sex") \
  .agg(grouping_id(), mean("distance"), stddev("distance")) \
  .orderBy("rider_student", "rider_sex") \
  .show()

# **Example:** Two categorical variables and two continuous variables
rides_sdf \
  .groupBy("rider_student", "rider_sex") \
  .agg(corr("distance", "duration")) \
  .orderBy("rider_student") \
  .show()
  
# ### Faceted plots

# Generally, carefully crafted visualizations are more enlightening.  Before we
# produce more visualizations, let us fill in the missing values for rider_sex
# using pandas functionality:
rides_pdf["rider_sex"] = rides_pdf["rider_sex"].fillna("missing")

# **Question:** Does this syntax look somewhat familiar?

# **Example:** Three categorical variables

# In CDSW, it is best to encapsulated multi-layered plots within functions:
def tmp_plot():
  g = sns.FacetGrid(data=rides_pdf, row="rider_sex", col="rider_student")
  g = g.map(sns.countplot, "service", order=order)  # order is defined above
tmp_plot()

# **Example:** Two categorical variables and one continuous variable
def tmp_plot():
  g = sns.FacetGrid(data=rides_pdf, row="rider_sex", col="rider_student")
  g = g.map(plt.hist, "distance")
tmp_plot()

# **Example:** Two categorical variables and two continuous variables

# We can use `FacetGrid` to explore the relationship between two continuous
# variables as a function of two categorical variables.  For example, let us
# explore the relationship of ride distance and ride duration as a function of
# rider sex and student status:
def tmp_plot():  # Wrap plot build into function for CDSW
  g = sns.FacetGrid(data=rides_pdf, row="rider_sex", col="rider_student")
  g = g.map(plt.scatter, "distance", "duration")
tmp_plot()


# ## Exercises

# (1) Explore the distribution of ride rating.

# (2) Explore the distribution of ride duration.

# (3) Explore the relationship between ride rating and rider student status.


# ## Cleanup

# Stop the SparkSession:
spark.stop()


# ## References

# [The SciPy Stack](https://scipy.org/)

# [pandas](http://pandas.pydata.org/)

# [matplotlib](https://matplotlib.org/index.html)

# [seaborn](https://seaborn.pydata.org/index.html)

# [Bokeh](http://bokeh.pydata.org/en/latest/)

# [Plotly](https://plot.ly/)
