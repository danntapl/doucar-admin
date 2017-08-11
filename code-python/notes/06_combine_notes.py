# # Developer Notes


# ## Cross Joins

# Cross joins via the `join` DataFrame method are turned off by default.

# This results in an error:
spark.conf.set("spark.sql.crossJoin.enabled", "false")  # default setting
spark.conf.get("spark.sql.crossJoin.enabled")
scientists.join(offices).show()

# This results in a cross join (an inner join with no join expression):
spark.conf.set("spark.sql.crossJoin.enabled", "true")
spark.conf.get("spark.sql.crossJoin.enabled")
scientists.join(offices).show()

# I can not figure out how to use the `cross` argument.  This does not seem to work:
scientists.join(offices, how="cross").show()


# ## Column and Row Binding

# Is there any way to do column and row binding a la R in PySpark?


# ## Deterministic Splitting

# **Question:** Is there a way to split on the value of a variable,
# that is, return a DataFrame for each value of a (categorical or Boolean) variable?
# Does this question even make sense in the Spark context?
