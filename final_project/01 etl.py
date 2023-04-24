# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

from pyspark.sql.functions import to_date, col,lit
start_date = str(dbutils.widgets.get('01.start_date'))
end_date = str(dbutils.widgets.get('02.end_date'))
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

print(start_date,end_date,hours_to_forecast, promote_model)
print("YOUR CODE HERE...")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DB

# COMMAND ----------

spark.conf.set("GROUP_DB_NAME.events", GROUP_DB_NAME)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE ${GROUP_DB_NAME.events}

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %md
# MAGIC ### Historic Bike data 
# MAGIC ##### historic_bike_trip_b - bronze
# MAGIC - Stream read historic bike data 

# COMMAND ----------


# bike_schema = "ride_id STRING, rideable_type STRING, started_at TIMESTAMP, ended_at TIMESTAMP, start_station_name STRING, start_station_id STRING, end_station_name STRING, end_station_id STRING, start_lat DOUBLE, start_lng DOUBLE, end_lat DOUBLE, end_lng DOUBLE, member_casual STRING"

# COMMAND ----------

bronze_bike_schema = "started_at TIMESTAMP, ended_at TIMESTAMP, start_lat DOUBLE, start_lng DOUBLE, end_lat DOUBLE, end_lng DOUBLE"
bronze_bike_checkPoint = f"{GROUP_DATA_PATH}bronze_historic_bike.checkpoint"
bronze_bike_delta = f"{GROUP_DATA_PATH}bronze_historic_bike.delta"

# COMMAND ----------

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format" , "csv")
    .option("cloudFiles.schemaHints", bronze_bike_schema)
    .option("cloudFiles.schemaLocation", bronze_bike_checkPoint)
    .option("header", "True")
    .load(BIKE_TRIP_DATA_PATH)
    .filter((col("start_station_name") == GROUP_STATION_ASSIGNMENT) | (col("end_station_name") == GROUP_STATION_ASSIGNMENT))
    .writeStream
    .format("delta")
    .option("checkpointLocation", bronze_bike_checkPoint)
    .trigger(availableNow = True)
    .outputMode("append")
    .start(bronze_bike_delta)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE historic_bike_trip_b AS 
# MAGIC SELECT * FROM delta. `dbfs:/FileStore/tables/G10/bronze_historic_bike.delta`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Historic Weather data
# MAGIC ##### historic_weather_b - bronze

# COMMAND ----------

bronze_weather_schema = "dt INTEGER, temp DOUBLE, feels_like DOUBLE, pressure INTEGER, humidity INTEGER, dew_point DOUBLE, uvi DOUBLE, clouds INTEGER, visibility INTEGER, wind_speed DOUBLE, wind_deg INTEGER, pop DOUBLE, snow_1h DOUBLE, id INTEGER, main STRING, description STRING, icon STRING, loc STRING, lat DOUBLE, lon DOUBLE, timezone STRING, timezone_offset INTEGER, rain_1h DOUBLE"
bronze_weather_checkPoint = f"{GROUP_DATA_PATH}bronze_historic_weather.checkpoint"
bronze_weather_delta = f"{GROUP_DATA_PATH}bronze_historic_weather.delta"

# COMMAND ----------

(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", "csv")
 .option("cloudFiles.schemaHints", bronze_weather_schema)
 .option("cloudFiles.schemaLocation", bronze_weather_checkPoint)
 .option("header", "True")
 .load(NYC_WEATHER_FILE_PATH)
 .withColumn("time", col("dt").cast("timestamp"))
 .writeStream
 .format("delta")
 .option("checkpointLocation", bronze_weather_checkPoint)
 .trigger(availableNow = True)
 .outputMode("append")
 .start(bronze_weather_delta)
)




# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE historic_weather_b AS 
# MAGIC SELECT * FROM delta. `dbfs:/FileStore/tables/G10/bronze_historic_weather.delta`

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY historic_weather_b

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Station Info

# COMMAND ----------

# DBTITLE 1,Display Bike Station Information
display(spark.read.format('delta').load(BRONZE_STATION_INFO_PATH).filter(col("name") == GROUP_STATION_ASSIGNMENT))
# display(spark.read.format('delta').load(BRONZE_STATION_INFO_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Bronze Station Status

# COMMAND ----------

# DBTITLE 1,Display the Bike Station Status Information
# statusDf = spark.read.format('delta').load(BRONZE_STATION_STATUS_PATH).filter(col("station_id") == "66dc686c-0aca-11e7-82f6-3863bb44ef7c")
# statusDf = statusDf.withColumn( "last_reported", col("last_reported").cast("timestamp")).sort(col("last_reported").desc())
statusDf = spark.read.format("delta").load(BRONZE_STATION_STATUS_PATH)
display(statusDf)


# COMMAND ----------

# MAGIC %md 
# MAGIC ### Bronze NYC Weather

# COMMAND ----------

# DBTITLE 1,Display the current (within the hour) NYC Weather Information
display(spark.read.format('delta').load(BRONZE_NYC_WEATHER_PATH).sort(col("time").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Table

# COMMAND ----------

# MAGIC %md
# MAGIC ### bike weather silver table

# COMMAND ----------

import holidays
from datetime import date

us_holidays = holidays.US()

@udf
def isHoliday(year, month, day):
    return date(year, month, day) in us_holidays
spark.udf.register("isHoliday", isHoliday)


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW time_and_netChange_G10_db AS 
# MAGIC SELECT
# MAGIC CAST(main_time as date) AS dates,
# MAGIC month(main_time) as month,
# MAGIC dayofweek(main_time) AS dayofweek,
# MAGIC HOUR(main_time) AS hour,
# MAGIC SUM(changed) AS net_change
# MAGIC FROM (
# MAGIC SELECT 
# MAGIC   CASE 
# MAGIC   WHEN end_station_name = "8 Ave & W 33 St"
# MAGIC   THEN ended_at
# MAGIC   ELSE started_at
# MAGIC END AS main_time,
# MAGIC   CASE 
# MAGIC   WHEN end_station_name = "8 Ave & W 33 St"
# MAGIC   THEN 1
# MAGIC   ELSE -1
# MAGIC END AS changed
# MAGIC FROM historic_bike_trip_b
# MAGIC )
# MAGIC GROUP BY dates, hour 
# MAGIC ORDER BY dates DESC, hour DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW time_weather_netChange_G10_db AS 
# MAGIC SELECT B.dates, month, dayofweek, B.hour, feels_like, description, isHoliday(year(B.dates), month, day(B.dates)) AS holiday, net_change 
# MAGIC FROM time_and_netChange_G10_db AS B 
# MAGIC LEFT JOIN 
# MAGIC (SELECT 
# MAGIC CAST(time as date) AS dates,
# MAGIC HOUR(time) as hour,
# MAGIC *
# MAGIC FROM historic_weather_b) AS W
# MAGIC ON B.dates == W.dates AND B.hour == W.hour
# MAGIC ORDER BY B.dates DESC, B.hour DESC 

# COMMAND ----------

silver_bike_weather_delta = f"{GROUP_DATA_PATH}silver_historic_bike_weather.delta/"
(spark.table("time_weather_netChange_G10_db")
    .write
    .format("delta")
    .mode("overwrite")
    .save(silver_bike_weather_delta)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bike_weather_netChange_s AS 
# MAGIC SELECT * FROM delta. `dbfs:/FileStore/tables/G10/silver_historic_bike_weather.delta/`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bike_weather_netChange_s

# COMMAND ----------



# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
