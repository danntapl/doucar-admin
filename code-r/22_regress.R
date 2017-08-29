# # Regression modeling with sparklyr

# This module demonstrates how to fit and evaluate a
# linear regression model using sparklyr's machine
# learning functions, in addition to feature transformers
# and other sparklyr functions.


# ## Setup

library(sparklyr)
library(dplyr)
config <- spark_config()
config$spark.driver.host <- Sys.getenv("CDSW_IP_ADDRESS")
spark <- spark_connect(
  master = "local",
  app_name = "dplyr",
  config = config
)

rides <- spark_read_parquet(
  sc = spark,
  name = "rides",
  path = "/duocar/joined_all/"
)


# ## Prepare and split data

# Filter out cancelled rides, add a boolean variable 
# `reviewed` indicating whether the rider left a review,
# and apply the string indexer and one-hot encoder 
# to the vehicle color column. Finally, randomly split 
# the data into a training set (70%) and a test set (30%).

samples <- rides %>%
  filter(!cancelled) %>%
  mutate(reviewed = is.na(review)) %>%
  ft_string_indexer(
    input.col = "vehicle_color",
    output.col = "vehicle_color_index"
  ) %>%
  ft_one_hot_encoder(
    input.col = "vehicle_color_index",
    output.col = "vehicle_color_code"
  ) %>% 
  sdf_partition(train = 0.7, test = 0.3)


# ## Fit the regression model

# Now fit a linear regression model to the data, to try to
# predict star rating based on whether or not the rider 
# wrote a review, the vehicle year, and the vehicle color.

# There are two ways to specify the model. You can pass 
# the response and features arguments as character vectors:

model <- samples$train %>%
  ml_linear_regression(
    response = "star_rating",
    features = c("reviewed", "vehicle_year", "vehicle_color_code")
  )

# Or you can use R formula notation:

model <- samples$train %>%
  ml_linear_regression(
    star_rating ~ reviewed + vehicle_year + vehicle_color_code
  )


# ## Examine the regression model

# You can print the model object:

model

# Or a richer summary of the model object:

summary(model)

# You can extract specific fields from the model object 
# by name:

names(model)

model$coefficients
model$t.values
model$p.values
model$r.squared
model$root.mean.squared.error


# ## Generate predictions using the model

# Use the `sdf_predict()` function to generate predictions
# using the model.

pred <- model %>% 
  sdf_predict(samples$test)


# ## Cleanup

spark_disconnect(spark)
