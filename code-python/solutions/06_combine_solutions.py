# # Possible Solutions to the Exercises

# **Exercise:** Create a DataFrame with all combinations of vehicle make and vehicle year
# (regardless of whether the combination is observed in the data).

# Generate a DataFrame with all (observed) vehicle makes:
vehicle_make_df = drivers.select("vehicle_make").distinct().orderBy("vehicle_make")
vehicle_make_df.count()
vehicle_make_df.show()

# Generate a DataFrame with all (observed) vehicle years:
vehicle_year_df = drivers.select("vehicle_year").distinct().orderBy("vehicle_year")
vehicle_year_df.count()
vehicle_year_df.show()

# Use the `crossJoin` method to generate all combinations:
combinations = vehicle_make_df.crossJoin(vehicle_year_df)
combinations.count()
combinations.show()

# **Note**: This is not bulletproof. It would not give the desired result if
# a year was missing in the middle of the list.

# **Exercise:** Join the demographic and weather data with the joined rides data.

# **Exercise:** Find any drivers who have not provide a ride?

# A solution using joins:
lazy_drivers1 = drivers.join(rides, drivers.id == rides.driver_id, "left_anti")
lazy_drivers1.count()
lazy_drivers1.select("id").orderBy("id").show()

# A solution using set operations:
lazy_drivers2 = drivers.select("id").subtract(rides.select("driver_id"))
lazy_drivers2.count()
lazy_drivers2.orderBy("id").show()

