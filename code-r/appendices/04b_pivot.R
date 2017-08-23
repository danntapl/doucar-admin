# # The sparklyr `sdf_pivot()` function

# 04_transform1.R does not mention the `sdf_pivot()`
# function. An example of this function is below.

# Scenario: Let's look at the weather data
# and explore the relationship between the `Events`
# column (which reports on weather events that day,
# like rain, snow, thunderstorms, and fog)
# and the `CloudCover` column 
# (which reports cloud cover that day in *okta* units,
# which have a minimum of 0 and a maximum of 8).
# Even though `CloudCover` has numeric values, we can
# consider it to be a categorical variable because 
# its values have a small number of discrete levels.

# Read the weather data:

weather <- spark_read_parquet(
  sc = spark,
  name = "weather",
  path = "/duocar/clean/weather"
)

# Pivot the data using `sdf_pivot()` to
# investigate the correlation between these two 
# categorical variables `Events` and `CloudCover`:

weather %>% 
  sdf_pivot(Events ~ CloudCover)

# Clean up the output by replacing
# missing strings with "None" and missing
# numeric values with 0:

weather %>% 
  sdf_pivot(Events ~ CloudCover) %>%
  na.replace("None", 0)

# What is the apparent relationship between cloud cover
# and some of these weather events?
