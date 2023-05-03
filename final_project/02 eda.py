# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

start_date = str(dbutils.widgets.get('01.start_date'))
end_date = str(dbutils.widgets.get('02.end_date'))
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

print(start_date,end_date,hours_to_forecast, promote_model)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS historic_weather_b
# MAGIC USING DELTA LOCATION 'dbfs:/FileStore/tables/G10/bronze_historic_weather.delta';
# MAGIC
# MAGIC SELECT * from historic_weather_b

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS historic_bike_trip_b
# MAGIC USING DELTA LOCATION 'dbfs:/FileStore/tables/G10/bronze_historic_weather.delta';
# MAGIC
# MAGIC SELECT * FROM historic_bike_trip_b

# COMMAND ----------

from pyspark.sql.functions import col, from_unixtime

# COMMAND ----------

# MAGIC %md
# MAGIC #INSIGHTS ON BRONZE DATA SOURCES USING PANDAS PROFILING

# COMMAND ----------

import pandas as pd
import pandas_profiling

# COMMAND ----------

# MAGIC %md
# MAGIC Historic Weather Profiling

# COMMAND ----------

df = spark.table("historic_weather_b")
displayHTML(pandas_profiling.ProfileReport(df.toPandas()).html)

# COMMAND ----------

# MAGIC %md
# MAGIC Historic Bike Trip Profiling

# COMMAND ----------

df = spark.table("historic_bike_trip_b")
displayHTML(pandas_profiling.ProfileReport(df.toPandas()).html)

# COMMAND ----------

# MAGIC %md
# MAGIC Historic Weather Profiling

# COMMAND ----------

dfWeather = spark.read.load("/FileStore/tables/bronze_nyc_weather.delta")
displayHTML(pandas_profiling.ProfileReport(dfWeather.toPandas()).html)

# COMMAND ----------

# MAGIC %md
# MAGIC Station Status Profiling

# COMMAND ----------

#Find Station ID to use to filter in Station Status Table

from pyspark.sql.functions import col
dfInfo = spark.read.load(BRONZE_STATION_INFO_PATH).filter((col("name") == GROUP_STATION_ASSIGNMENT))
display(dfInfo)


# COMMAND ----------

#station id = 66dc686c-0aca-11e7-82f6-3863bb44ef7c
#filter for only statuses with this ID

from pyspark.sql.functions import col, from_unixtime
dfStatus = spark.read.load(BRONZE_STATION_STATUS_PATH).filter(col("station_id") == '66dc686c-0aca-11e7-82f6-3863bb44ef7c')
display (dfStatus.withColumn("DateTime", from_unixtime(col("last_reported"))))


# COMMAND ----------

#run pandas profiling on df with only data from our station

displayHTML(pandas_profiling.ProfileReport(dfStatus.toPandas()).html)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %md
# MAGIC #MONTHLY TRENDS

# COMMAND ----------

df = spark.table("historic_bike_trip_b")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC <h6>Create a temp view that adds columns for hour, day, month, and year from the bronze historic bike trip data</h6>

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW date_bike_G10_db AS (
# MAGIC   SELECT  YEAR(started_at) as year, MONTH(started_at) as month, DAY(started_at) as day, HOUR(started_at) AS hour, * FROM historic_bike_trip_b
# MAGIC );
# MAGIC
# MAGIC SELECT * FROM date_bike_G10_db;

# COMMAND ----------

# MAGIC %md
# MAGIC <h6>Determine number of rides per year</h6>

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT year, COUNT(ride_id) AS count FROM date_bike_G10_db
# MAGIC GROUP BY year
# MAGIC SORT BY year
# MAGIC
# MAGIC --number of bike trips by year

# COMMAND ----------

# MAGIC %md
# MAGIC <h6> Confirms that data from 2021 and 2023 does not contain data for all 12 months. This could cause the disingenious results in evaluating yearly/monthly/daily trends. Move forward with data from only 2022 in order to capture monthly / daily trends over a year period</h6>

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT month, count(*) as count FROM date_bike_G10_db
# MAGIC WHERE year == 2022
# MAGIC GROUP BY month
# MAGIC SORT BY month
# MAGIC
# MAGIC -- Highest use in the Summer, then Fall / Spring, then Winter

# COMMAND ----------

# MAGIC %md
# MAGIC <h6> Find total number of trips in each month for the year 2022</h6>

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT month, count(ride_id) as count FROM date_bike_G10_db
# MAGIC WHERE year == 2022
# MAGIC GROUP BY month
# MAGIC SORT BY count DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT month, count(ride_id) as count FROM date_bike_G10_db
# MAGIC WHERE year == 2023
# MAGIC GROUP BY month
# MAGIC SORT BY count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC <h6>Compare total number of rides in first 3 months of 2022 to first 3 months in 2023</h6>

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT month, year, count(ride_id) as count FROM date_bike_G10_db
# MAGIC WHERE month IN (1,2,3) AND year IN (2022, 2023)
# MAGIC GROUP BY month, year
# MAGIC SORT BY month, year

# COMMAND ----------

# MAGIC %md
# MAGIC ##Takeaways from Exploration of Monthly Bike Trends
# MAGIC </br>
# MAGIC <ul>
# MAGIC <li>Bike use higher in the warmer (Summer, Fall, Spring) months</li>
# MAGIC <li>Total number of rides in the first 3 moths of 2023 higher than total rides for the first 3 months of 2022. This could suggest that use of citibikes and the station is growing</li>

# COMMAND ----------

# MAGIC %md
# MAGIC #DAILY TRIP TRENDS

# COMMAND ----------

# MAGIC %md
# MAGIC <h6>Create functions to return the day name and day number based on a date</h6>

# COMMAND ----------


@udf

# Return the day name for the given date
def dayOfWeek (year, month, day):
    x = str(year) + "-" + str(month) + "-" + str(day)
    d = pd.Timestamp(x)
    return d.day_name()

spark.udf.register("dayOfWeek", dayOfWeek)

#return the day number for the given date (Monday = 0, Tuesday = 1, etc.)
def dayNumber (year, month, day):
    x = str(year) + "-" + str(month) + "-" + str(day)
    d = pd.Timestamp(x)
    return d.dayofweek

spark.udf.register("dayNumber", dayNumber)

# COMMAND ----------

# MAGIC %md
# MAGIC <h6>Find total number of trips for each day of the week</h6>

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DayName, DayNumber, count(ride_id) as count FROM(
# MAGIC   SELECT concat(year, "-", month, "-", day) AS date, dayOfWeek(year, month, day) AS DayName, dayNumber(year, month, day) AS DayNumber, * FROM date_bike_G10_db
# MAGIC   WHERE year == 2022
# MAGIC   )
# MAGIC GROUP BY DayName, DayNumber
# MAGIC SORT BY count DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT month, DayName, DayNumber, count(ride_id) as count FROM(
# MAGIC   SELECT concat(year, "-", month, "-", day) AS date, dayOfWeek(year, month, day) AS DayName, dayNumber(year, month, day) AS DayNumber, * FROM date_bike_G10_db
# MAGIC   WHERE year == 2022
# MAGIC   )
# MAGIC GROUP BY month, DayNumber, DayName
# MAGIC SORT BY month, DayNumber

# COMMAND ----------

# MAGIC %md
# MAGIC ##Takeaways from Exploration of Daily Bike Trends
# MAGIC </br>
# MAGIC <ul>
# MAGIC <li>Bike use higher on weekdays than on the weekends</li>
# MAGIC <li>Suggests that the main use of the stations is by commuters</li>

# COMMAND ----------

# MAGIC %md
# MAGIC #HOURLY TRIP TRENDS

# COMMAND ----------

# MAGIC %md
# MAGIC <h6>Find total number of trips for each hour of the day</h6>

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT hour, count(ride_id) as count FROM(
# MAGIC   SELECT concat(year, "-", month, "-", day) AS date, dayOfWeek(year, month, day) AS DayName, dayNumber(year, month, day) AS DayNumber, * FROM date_bike_G10_db
# MAGIC   WHERE year == 2022
# MAGIC   )
# MAGIC GROUP BY hour
# MAGIC SORT BY count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC <h6>Investigate correlation between day and month (not that helpful)</h6>

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT month, DayName, DayNumber, hour, count(ride_id) as count FROM(
# MAGIC   SELECT concat(year, "-", month, "-", day) AS date, dayOfWeek(year, month, day) AS DayName, dayNumber(year, month, day) AS DayNumber, * FROM date_bike_G10_db
# MAGIC   WHERE year == 2022
# MAGIC   )
# MAGIC GROUP BY month, DayNumber, DayName, hour
# MAGIC SORT BY count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Takeaways from Exploration of Hourly Bike Use
# MAGIC </br>
# MAGIC <ul>
# MAGIC <li> Top hour for bike use is 5:00 PM </li>
# MAGIC <li> Top time of day for bike use is afternoon / evening from 2:00 PM - 6:00 PM </li>
# MAGIC <li> 7:00 AM - 9:00 AM also are in the top ten </li>
# MAGIC <li> 1:00 PM is in the top 10 as well, which points to the use of bikes for lunch time trips </li>
# MAGIC <li> These trends suggest that bikes are heavily used by commuters to go to/from work/school/etc.</li>
# MAGIC </ul>  

# COMMAND ----------

# MAGIC %md
# MAGIC #HOLIDAY TRENDS

# COMMAND ----------

# MAGIC %md
# MAGIC <h6>Function to determine if a given date is a US Holiday</h6>

# COMMAND ----------

import holidays
from datetime import date

us_holidays = holidays.US()

@udf
def isHoliday(year, month, day):
    return date(year, month, day) in us_holidays

spark.udf.register("isHoliday", isHoliday)


# COMMAND ----------

# MAGIC %md
# MAGIC <h6>Determine average number of bike rides per day on Holidays vs Non Holidays</h6>

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT holiday, avg(count) as count FROM(
# MAGIC    SELECT date, count(*) as count , max(holiday) as holiday
# MAGIC    FROM (
# MAGIC      SELECT concat(year, " ", month, " ", day) AS date, isHoliday(year, month, day) AS holiday, * FROM date_bike_G10_db
# MAGIC    )
# MAGIC GROUP BY date
# MAGIC  )
# MAGIC GROUP BY holiday

# COMMAND ----------

# MAGIC %md
# MAGIC ##Takeaways from Exploration of Holiday Bike Use
# MAGIC </br>
# MAGIC <ul>
# MAGIC <li>The number of bike trips, on average, is lower on holidays than not on holidays</li>
# MAGIC </ul> 

# COMMAND ----------

# MAGIC %md
# MAGIC #WEATHER TRENDS

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM historic_weather_b

# COMMAND ----------

# MAGIC %md
# MAGIC <h6>Create a temp view that adds columns for date, day name, and day number from the bronze historic weather data</h6>

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW historic_trips_with_days_G10_db AS(
# MAGIC   SELECT concat(year, "-", month, "-", day) AS date, dayOfWeek(year, month, day) AS DayName, dayNumber(year, month, day) AS DayNumber, * FROM date_bike_G10_db
# MAGIC   WHERE year == 2022
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC <h6>Create temp view that joins bronze bike trip data with bronze weather data. Determine total number of trips for each weather description</h6>

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW historic_bike_weather_G10_db AS(
# MAGIC     SELECT date, DayName, dayNumber, day, hour, ride_id, feels_like, humidity, wind_speed, main, description, snow_1h, rain_1h, pop
# MAGIC     FROM historic_trips_with_days_G10_db AS B JOIN historic_weather_b AS W
# MAGIC     ON W.time BETWEEN B.started_at AND B.ended_at
# MAGIC     );
# MAGIC     
# MAGIC SELECT COUNT(ride_id) as count, description
# MAGIC FROM historic_bike_weather_G10_db
# MAGIC  GROUP BY description
# MAGIC  ORDER BY count DESC;
# MAGIC  
# MAGIC  --- Most rides occur when there is no precipitation

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(ride_id) as count, description, dayNumber, DayName
# MAGIC FROM historic_bike_weather_G10_db
# MAGIC  GROUP BY dayNumber, DayName, description
# MAGIC  ORDER BY DayName
# MAGIC  
# MAGIC  --rides based on conditions versus day of the week

# COMMAND ----------

# MAGIC %md
# MAGIC <h6>Function to convert Kelvin to Fahrenheit and put a specfic temperature value into a temperature range (i.e. 63 to 60's, 78 to 70's)</h6>

# COMMAND ----------

@udf

def kelvinToFahrenheit(kelvin):
    return round(kelvin * 1.8 - 459.67)

spark.udf.register("kelvinToFahrenheit", kelvinToFahrenheit)


def tempRange (temp):
    if abs(temp) >=10:
        while (abs(temp) >= 10):
            temp = temp // 10    
        return temp * 10
    else:
        return 0


spark.udf.register("tempRange", tempRange)

# COMMAND ----------

# MAGIC %md
# MAGIC <h6>Determine total number of rides for each temperature range</h6>

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(ride_id) as count, tempRange(feels_like_F) as feels_like_range FROM (
# MAGIC     SELECT ride_id, kelvinToFahrenheit(feels_like) as feels_like_F
# MAGIC       FROM historic_bike_weather_G10_db
# MAGIC )
# MAGIC  GROUP BY feels_like_range
# MAGIC  ORDER BY count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC <h6>Determine total number of rides for each range of chance of precipitation. Re-use/re-purpose the function that was used above to put temperatures into ranges</h6>

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(ride_id) as count, tempRange(pop *100) as chance_of_precip
# MAGIC       FROM historic_bike_weather_G10_db
# MAGIC  GROUP BY chance_of_precip
# MAGIC  ORDER BY count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Takeaways from Exploration of Weather Data
# MAGIC </br>
# MAGIC <ul>
# MAGIC <li>Bike usage increases when chance of precipitation is lower</li>
# MAGIC <li>Bike usage increases as temperature increases - until it hits the 80's and then it starts to decrease again. Usage peaks in the 70's</li>
# MAGIC </ul>

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #STATION TRENDS

# COMMAND ----------

# MAGIC %md
# MAGIC <h6>Get list of origination stations and count of trips from that station that ended at our station</h6>

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT start_station_name, count(ride_id) as count FROM(
# MAGIC   SELECT concat(year, "-", month, "-", day) AS date, dayOfWeek(year, month, day) AS DayName, dayNumber(year, month, day) AS DayNumber, * FROM date_bike_G10_db
# MAGIC   WHERE year == 2022 
# MAGIC     AND start_station_name != '8 Ave & W 33 St'
# MAGIC   )
# MAGIC GROUP BY start_station_name
# MAGIC SORT BY count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC <h6>Get list of ending stations and count of trips ending at that station that began at our station</h6>

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT end_station_name, count(ride_id) as count FROM(
# MAGIC   SELECT concat(year, "-", month, "-", day) AS date, dayOfWeek(year, month, day) AS DayName, dayNumber(year, month, day) AS DayNumber, * FROM date_bike_G10_db
# MAGIC   WHERE year == 2022 
# MAGIC     AND end_station_name != '8 Ave & W 33 St'
# MAGIC   )
# MAGIC GROUP BY end_station_name
# MAGIC SORT BY count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Takeaways from Exploration of Station Data
# MAGIC </br>
# MAGIC <ul>
# MAGIC <li>3.71% of rides that ended at our station originated at 11 Ave & W 41 St</li>
# MAGIC <li>3.78% of rides that began at our station ended at W 35 St & 8 Ave</li>
# MAGIC <li>These are both almost double values of the stations with the next highest number of ride originations / destinations</li>

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))

# COMMAND ----------


