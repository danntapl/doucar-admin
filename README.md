# Ride Sharing Example

_This project is targeted at students with previous experience in_

* _R and/or Python_

* _Machine Learning Pipelines_

_If this code is beyond your current understanding, but looks like something you would like to be able to do,
you can learn how to do all of this [from Cloudera](https://www.cloudera.com/more/training/courses/data-scientist-training.html)_


## Goals

In this project, you can explore and perform 
analysis on data from a ficticious ride sharing company, __Duocar__.

The exercise files demonstrate building ML Models in both R and Python.

The data for this example is found in HDSF in the **/duocar** directory.

* __/duocar/raw__ contains CSV & TSV data files in their raw format
* __/duocar/clean__ contains the same data, but with the data types corrected and stored in parquet files
* __/duocar/joined_all__ contains a parquet file that represents a join of driver, rider, ride, review and demographic data

You will find examples of loading this data int DataFrames in the associated example files.

## Data Schema

### demographics
* block_group: string
* median_income: integer
* median_age: decimal(3,1)

### drivers
 * id: string
 * birth_date: date
 * start_date: date
 * first_name: string
 * last_name: string
 * sex: string
 * ethnicity: string
 * student: boolean
 * home_block: string
 * home_lat: decimal(9,6)
 * home_lon: decimal(9,6)
 * vehicle_make: string
 * vehicle_model: string
 * vehicle_year: integer
 * vehicle_color: string
 * vehicle_grand: boolean
 * vehicle_noir: boolean
 * vehicle_elite: boolean
 * rides: integer
 * stars: integer

### ride_reviews
* ride_id: string
* review: string

### riders
 * id: string
 * birth_date: date
 * start_date: date
 * first_name: string
 * last_name: string
 * sex: string
 * ethnicity: string
 * student: boolean
 * home_block: string
 * home_lat: decimal(9,6)
 * home_lon: decimal(9,6)
 * work_lat: decimal(9,6)
 * work_lon: decimal(9,6)

### rides
 * id: string
 * driver_id: string
 * rider_id: string
 * date_time: timestamp
 * utc_offset: integer
 * service: string
 * origin_lat: decimal(9,6)
 * origin_lon: decimal(9,6)
 * dest_lat: decimal(9,6)
 * dest_lon: decimal(9,6)
 * distance: integer
 * duration: integer
 * cancelled: boolean
 * star_rating: integer
 
### weather
 * Station_ID: string
 * Date: date
 * Max_TemperatureF: integer
 * Mean_TemperatureF: integer
 * Min_TemperatureF: integer
 * Max_Dew_PointF: integer
 * MeanDew_PointF: integer
 * Min_DewpointF: integer
 * Max_Humidity: integer
 * Mean_Humidity: integer
 * Min_Humidity: integer
 * Max_Sea_Level_PressureIn: decimal(4,2)
 * Mean_Sea_Level_PressureIn: decimal(4,2)
 * Min_Sea_Level_PressureIn: decimal(4,2)
 * Max_VisibilityMiles: integer
 * Mean_VisibilityMiles: integer
 * Min_VisibilityMiles: integer
 * Max_Wind_SpeedMPH: integer
 * Mean_Wind_SpeedMPH: integer
 * Max_Gust_SpeedMPH: integer
 * PrecipitationIn: decimal(4,2)
 * CloudCover: integer
 * Events: string
 * WindDirDegrees: integer

### joined_all

* ride_id: string
* rider_id: string
* driver_id: string 
* date_time: timestamp 
* utc_offset: integer 
* service: string 
* origin_lat: decimal(9,6) 
* origin_lon: decimal(9,6) 
* dest_lat: decimal(9,6) 
* dest_lon: decimal(9,6) 
* distance: integer 
* duration: integer 
* cancelled: boolean 
* star_rating: integer 
* driver_birth_date: date 
* driver_start_date: date 
* driver_first_name: string 
* driver_last_name: string 
* driver_sex: string 
* driver_ethnicity: string 
* driver_student: boolean 
* driver_home_block: string 
* driver_home_lat: decimal(9,6) 
* driver_home_lon: decimal(9,6) 
* vehicle_make: string 
* vehicle_model: string 
* vehicle_year: integer 
* vehicle_color: string 
* vehicle_grand: boolean 
* vehicle_noir: boolean 
* vehicle_elite: boolean 
* driver_rides: integer 
* driver_stars: integer 
* rider_birth_date: date 
* rider_start_date: date 
* rider_first_name: string 
* rider_last_name: string 
* rider_sex: string
* rider_ethnicity: string 
* rider_student: boolean 
* rider_home_block: string 
* rider_home_lat: decimal(9,6) 
* rider_home_lon: decimal(9,6)
* rider_work_lat: decimal(9,6)
* rider_work_lon: decimal(9,6) 
* review: string 
* driver_median_income: integer 
* rider_median_income: integer 
* Max_TemperatureF: integer 
* Mean_TemperatureF: integer 
* Min_TemperatureF: integer 
* Max_Dew_PointF: integer 
* MeanDew_PointF: integer 
* Min_DewpointF: integer 
* Max_Humidity: integer 
* Mean_Humidity: integer
* Min_Humidity: integer 
* Max_Sea_Level_PressureIn: decimal(4,2)
* Mean_Sea_Level_PressureIn: decimal(4,2) 
* Min_Sea_Level_PressureIn: decimal(4,2) 
* Max_VisibilityMiles: integer 
* Mean_VisibilityMiles: integer 
* Min_VisibilityMiles: integer
* Max_Wind_SpeedMPH: integer 
* Mean_Wind_SpeedMPH: integer 
* Max_Gust_SpeedMPH: integer 
* PrecipitationIn: decimal(4,2) )
* CloudCover: integer 
* Events: string
* WindDirDegrees: integer 

### joined
This is a subset of the data in joined_all that is smaller and easier to work with.  
It includes driver, rider, ride and review data

 * ride_id: string
 * rider_id: string
 * driver_id: string
 * date_time: timestamp
 * utc_offset: integer
 * service: string
 * origin_lat: decimal(9,6)
 * origin_lon: decimal(9,6)
 * dest_lat: decimal(9,6)
 * dest_lon: decimal(9,6)
 * distance: integer
 * duration: integer
 * cancelled: boolean
 * star_rating: integer
 * driver_birth_date: date
 * driver_start_date: date
 * driver_first_name: string
 * driver_last_name: string
 * driver_sex: string
 * driver_ethnicity: string
 * driver_student: boolean
 * driver_home_block: string
 * driver_home_lat: decimal(9,6)
 * driver_home_lon: decimal(9,6)
 * vehicle_make: string
 * vehicle_model: string
 * vehicle_year: integer
 * vehicle_color: string
 * vehicle_grand: boolean
 * vehicle_noir: boolean
 * vehicle_elite: boolean
 * driver_rides: integer
 * driver_stars: integer
 * rider_birth_date: date
 * rider_start_date: date
 * rider_first_name: string
 * rider_last_name: string
 * rider_sex: string
 * rider_ethnicity: string
 * rider_student: boolean
 * rider_home_block: string
 * rider_home_lat: decimal(9,6)
 * rider_home_lon: decimal(9,6)
 * rider_work_lat: decimal(9,6)
 * rider_work_lon: decimal(9,6)
 * review: string