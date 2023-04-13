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

# MAGIC %sql
# MAGIC DESCRIBE SCHEMA G10_db

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

bike_schema = "started_at TIMESTAMP, ended_at TIMESTAMP, start_lat DOUBLE, start_lng DOUBLE, end_lat DOUBLE, end_lng DOUBLE"
bike_checkPoint = f"{GROUP_DATA_PATH}bronze/historic_bike/_checkpoint/"


# COMMAND ----------

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format" , "csv")
    .option("cloudFiles.schemaHints", bike_schema)
    .option("cloudFiles.schemaLocation", bike_checkPoint)
    .option("header", "True")
    .load(BIKE_TRIP_DATA_PATH)
    .filter((col("start_station_name") == GROUP_STATION_ASSIGNMENT) | (col("end_station_name") == GROUP_STATION_ASSIGNMENT))
    .writeStream
    .format("delta")
    .option("checkpointLocation", bike_checkPoint)
    .outputMode("append")
    .table("historic_bike_trip_b")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM historic_bike_trip_b

# COMMAND ----------

# MAGIC %md
# MAGIC ### Historic Weather data
# MAGIC ##### historic_weather_b - bronze

# COMMAND ----------

weather_schema = "dt INTEGER, temp DOUBLE, feels_like DOUBLE, pressure INTEGER, humidity INTEGER, dew_point DOUBLE, uvi DOUBLE, clouds INTEGER, visibility INTEGER, wind_speed DOUBLE, wind_deg INTEGER, pop DOUBLE, snow_1h DOUBLE, id INTEGER, main STRING, description STRING, icon STRING, loc STRING, lat DOUBLE, lon DOUBLE, timezone STRING, timezone_offset INTEGER, rain_1h DOUBLE"
weather_checkPoint = f"{GROUP_DATA_PATH}bronze/historic_weather/_checkpoint/"


# COMMAND ----------

(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", "csv")
 .option("cloudFiles.schemaHints", weather_schema)
 .option("cloudFiles.schemaLocation",weather_checkPoint)
 .option("header", "True")
 .load(NYC_WEATHER_FILE_PATH)
 .withColumn("time", col("dt").cast("timestamp"))
 .writeStream
 .format("delta")
 .option("checkpointLocation", weather_checkPoint)
 .outputMode("append")
 .table("historic_weather_b")
)




# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM historic_weather_b

# COMMAND ----------

# dbutils.fs.rm(f"{BIKE_TRIP_DATA_PATH}_schemas", recurse = True )
# dbutils.fs.rm(f"{BIKE_TRIP_DATA_PATH}commits", recurse = True)
# dbutils.fs.rm(f"{BIKE_TRIP_DATA_PATH}metadata", recurse = True)
# dbutils.fs.rm(f"{BIKE_TRIP_DATA_PATH}offsets", recurse = True)
# dbutils.fs.rm(f"{BIKE_TRIP_DATA_PATH}sources", recurse = True)
# dbutils.fs.rm(f"{BIKE_TRIP_DATA_PATH}checkpoint", recurse = True )

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

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
