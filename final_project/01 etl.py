# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

from pyspark.sql.functions import to_date, col,lit
spark.conf.set("spark.sql.session.timeZone", "America/New_York")

start_date = str(dbutils.widgets.get('01.start_date'))
end_date = str(dbutils.widgets.get('02.end_date'))
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

print(start_date,end_date,hours_to_forecast, promote_model)
print("YOUR CODE HERE...")

# COMMAND ----------

# MAGIC %md
# MAGIC ### set DB
# MAGIC - set DB to make sure we are working on proper Database

# COMMAND ----------

spark.conf.set("GROUP_DB_NAME.events", GROUP_DB_NAME)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE ${GROUP_DB_NAME.events}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Historic Bike data (bronze - historic_bike_trip_b )

# COMMAND ----------

# define schema 
bronze_bike_schema = "started_at TIMESTAMP, ended_at TIMESTAMP, start_lat DOUBLE, start_lng DOUBLE, end_lat DOUBLE, end_lng DOUBLE"

# define checkpoint location and delta path 
bronze_bike_checkPoint = f"{GROUP_DATA_PATH}bronze_historic_bike.checkpoint"
bronze_bike_delta = f"{GROUP_DATA_PATH}bronze_historic_bike.delta"

# COMMAND ----------

# stream csv files to delta files 
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format" , "csv")
    .option("cloudFiles.schemaHints", bronze_bike_schema)
    .option("cloudFiles.schemaLocation", bronze_bike_checkPoint)
    .option("header", "True")
    .load(BIKE_TRIP_DATA_PATH)
    .filter(~((col("start_station_name") == GROUP_STATION_ASSIGNMENT) & (col("end_station_name") == GROUP_STATION_ASSIGNMENT)))
    .filter((col("start_station_name") == GROUP_STATION_ASSIGNMENT) | (col("end_station_name") == GROUP_STATION_ASSIGNMENT))
    .withColumn("coming", lit(col("end_station_name") == GROUP_STATION_ASSIGNMENT))
    .writeStream
    .format("delta")
    .option("checkpointLocation", bronze_bike_checkPoint)
    .partitionBy("coming")
    .trigger(once = True)
    .outputMode("append")
    .start(bronze_bike_delta)
    .awaitTermination()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create hitoric_weather_b table
# MAGIC CREATE OR REPLACE TABLE historic_bike_trip_b AS 
# MAGIC SELECT * FROM delta. `dbfs:/FileStore/tables/G10/bronze_historic_bike.delta`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Historic Weather data Table (bronze_historic_weather_b)

# COMMAND ----------

# define schema 
bronze_weather_schema = "dt INTEGER, temp DOUBLE, feels_like DOUBLE, pressure INTEGER, humidity INTEGER, dew_point DOUBLE, uvi DOUBLE, clouds INTEGER, visibility INTEGER, wind_speed DOUBLE, wind_deg INTEGER, pop DOUBLE, snow_1h DOUBLE, id INTEGER, main STRING, description STRING, icon STRING, loc STRING, lat DOUBLE, lon DOUBLE, timezone STRING, timezone_offset INTEGER, rain_1h DOUBLE"

# define checkpoint location and delta path 
bronze_weather_checkPoint = f"{GROUP_DATA_PATH}bronze_historic_weather.checkpoint"
bronze_weather_delta = f"{GROUP_DATA_PATH}bronze_historic_weather.delta"

# COMMAND ----------

# stream csv files to delta files 
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
 .partitionBy("description")
 .trigger(once = True)
 .outputMode("append")
 .start(bronze_weather_delta)
 .awaitTermination()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create hitoric_weather_b table
# MAGIC CREATE OR REPLACE TABLE historic_weather_b AS 
# MAGIC SELECT * FROM delta. `dbfs:/FileStore/tables/G10/bronze_historic_weather.delta`

# COMMAND ----------

# %sql
# SELECT * FROM historic_weather_b

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Station Info

# COMMAND ----------

# DBTITLE 1,Display Bike Station Information
display(spark.read.format('delta').load(BRONZE_STATION_INFO_PATH).filter(col("name") == GROUP_STATION_ASSIGNMENT))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Bronze Station Status

# COMMAND ----------

# DBTITLE 1,Display the Bike Station Status Information
# statusDf = spark.read.format("delta").load(BRONZE_STATION_STATUS_PATH).filter(col("station_id") == "66dc686c-0aca-11e7-82f6-3863bb44ef7c").withColumn("ts", col("last_reported").cast("timestamp")).sort(col("ts").desc())
# display(statusDf)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Bronze NYC Weather

# COMMAND ----------

# DBTITLE 1,Display the current (within the hour) NYC Weather Information
# display(spark.read.format('delta').load(BRONZE_NYC_WEATHER_PATH).sort(col("time").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Net change table (Silver - train_bike_weather_netChange_s)

# COMMAND ----------

# define and register function 
import holidays
from datetime import date

us_holidays = holidays.US()

@udf
def isHoliday(year, month, day):
    return date(year, month, day) in us_holidays
spark.udf.register("isHoliday", isHoliday)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create net change table using historic_bike_trip_b
# MAGIC CREATE OR REPLACE TEMP VIEW time_and_netChange_G10_db AS 
# MAGIC SELECT
# MAGIC main_time as ts,
# MAGIC year(main_time) as year,
# MAGIC month(main_time) as month,
# MAGIC dayofmonth(main_time) as dayofmonth,
# MAGIC dayofweek(main_time) AS dayofweek,
# MAGIC HOUR(main_time) AS hour,
# MAGIC SUM(changed) AS net_change
# MAGIC FROM (
# MAGIC SELECT 
# MAGIC   CASE 
# MAGIC   WHEN coming
# MAGIC   THEN date_format(ended_at, 'yyyy-MM-dd HH:00:00')
# MAGIC   ELSE date_format(started_at, 'yyyy-MM-dd HH:00:00')
# MAGIC END AS main_time,
# MAGIC   CASE 
# MAGIC   WHEN coming
# MAGIC   THEN 1
# MAGIC   ELSE -1
# MAGIC END AS changed
# MAGIC FROM historic_bike_trip_b
# MAGIC )
# MAGIC GROUP BY main_time

# COMMAND ----------

# MAGIC %sql
# MAGIC -- join net change table and weather table 
# MAGIC CREATE OR REPLACE TEMP VIEW time_weather_netChange_G10_db AS 
# MAGIC SELECT B.ts, year, month, dayofmonth, dayofweek, B.hour, feels_like, rain_1h , description, isHoliday(year(B.ts), month, day(B.ts)) AS holiday, net_change 
# MAGIC FROM time_and_netChange_G10_db AS B 
# MAGIC LEFT JOIN 
# MAGIC (SELECT 
# MAGIC date_format(time, 'yyyy-MM-dd HH:00:00') as ts,
# MAGIC *
# MAGIC FROM historic_weather_b) AS W
# MAGIC ON B.ts == W.ts
# MAGIC ORDER BY B.ts

# COMMAND ----------

# define delta path
silver_bike_weather_delta = f"{GROUP_DATA_PATH}silver_historic_bike_weather.delta"

# write delta file
(spark.table("time_weather_netChange_G10_db")
    .write
    .format("delta")
    .mode("overwrite")
    .save(silver_bike_weather_delta)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create train_bike_weather_netChange_s 
# MAGIC CREATE OR REPLACE TABLE train_bike_weather_netChange_s AS 
# MAGIC SELECT * FROM delta. `dbfs:/FileStore/tables/G10/silver_historic_bike_weather.delta/`

# COMMAND ----------

# %sql
# SELECT * FROM train_bike_weather_netChange_s

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
