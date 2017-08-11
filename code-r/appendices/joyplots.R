# # Joyplot example

# [Joyplots](http://blog.revolutionanalytics.com/2017/07/joyplots.html)
# in R are super popular right now, so we just had to make one.

# Let's make a joyplot that visualizes the weather data,
# exploring the relationship between the Events column
# (which report on weather events that day,
# like rain, snow, thunderstorms, fog)
# and the CloudCover column 
# (which reports cloud cover that day in okta units,
# which have a min of 0 and a max of 8).

# ## Setup

library(sparklyr)
library(dplyr)

spark <- spark_connect(master = "local", app_name = "inspect")

# Load weather data

weather <- spark_read_parquet(
  sc = spark,
  name = "weather",
  path = "/duocar/clean/weather"
)

# ## Get the data

# Limit the data to the Events and CloudCover columns,
# replace the NA in the Events column (a character column)
# with "None".

events_cloud_cover <- weather %>% 
  select(Events, CloudCover) %>% 
  na.replace(Events = "None") %>%
  collect() 


# ## Install ggjoy

# Now let's install the package 
# [ggjoy](https://github.com/clauswilke/ggjoy)
# which provides `geom_` functions for drawing joyplots
# using ggplot2:

install.packages("ggjoy")

# Now load both ggplot2 and ggjoy:

library(ggplot2)
library(ggjoy)

# ## Making the joyplot

# We use `geom_joy()` and `theme_joy()` along with
# `ggplot()` to make the joyplot:

ggplot(
    events_cloud_cover, 
    aes(x = CloudCover, y = Events)
  ) +
  geom_joy(scale = 1) + 
  theme_joy()


# ## Advanced version

# We could just stop there, but there are some problems
# with that plot:
# * The y axis is in alphabetical order, which is not 
#   meaningful. It would be better to order the y axis
#   by the mean CloudCover for each Event. 
# * There is not enough data to draw curves for the
#   Rain-Snow and Rain-Fog-Snow Events so it would be
#   better to omit those from the plot
# This version fixes those problems:

events_cloud_cover <- weather %>% 
  select(Events, CloudCover) %>% 
  na.replace(Events = "None") %>%
  group_by(Events) %>%
  mutate(
    m = mean(CloudCover),
    n = count()
  ) %>%
  filter(n > 3) %>% 
  arrange(m) %>%
  ungroup() %>%
  select(-m, -n) %>%
  collect() %>%
  mutate(Events=factor(Events, unique(Events)))

# Now let's plot it again, using the same plotting code
# as last time, but using the new data:


ggplot(
    events_cloud_cover, 
    aes(x = CloudCover, y = Events)
  ) +
  geom_joy(scale = 1) + 
  theme_joy()

# ## Non-graphical version

# The above is a graphical way to represent the same information  
# that could be represented in a pivot table using `sdf_pivot()`:

weather %>% 
  sdf_pivot(Events ~ CloudCover) %>%
  na.replace("None", 0)

# ## Cleanup

spark_disconnect(spark)
