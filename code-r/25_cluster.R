# # Clustering with sparklyr

# This module demonstrates how to perform k-means clustering
# using sparklyr.


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
  path = "/duocar/joined/"
)


# ## K-means clustering

# In this example, we cluster rides into 3 groups based
# on origin coordinates.


# ### Specify and fit the k-means clustering model:

model <- rides %>% 
  ml_kmeans(
    centers = 3,
    features = c("origin_lat", "origin_lon")
  )


# ###  Examine the k-means clustering model

model

names(model)

model$cost

model$centers


# ### See which points the model assigns to which clusters

kmeans_model %>% 
  sdf_predict(rides) %>%
  select(prediction)


# ## Cleanup

spark_disconnect(spark)
