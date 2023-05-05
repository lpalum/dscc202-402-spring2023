# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

from pyspark.sql.functions import col
spark.conf.set("spark.sql.session.timeZone", "America/New_York")

# COMMAND ----------

# DBTITLE 0,YOUR APPLICATIONS CODE HERE...
start_date = str(dbutils.widgets.get('01.start_date'))
end_date = str(dbutils.widgets.get('02.end_date'))
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

print(start_date,end_date,hours_to_forecast, promote_model)


# COMMAND ----------

# create and register function
import holidays
from datetime import date

us_holidays = holidays.US()

@udf
def isHoliday(year, month, day):
    return date(year, month, day) in us_holidays
spark.udf.register("isHoliday", isHoliday)

# COMMAND ----------

# create weather_tmp_G10_db from delta file
spark.read.format('delta').load(BRONZE_NYC_WEATHER_PATH).createOrReplaceTempView("weather_tmp_G10_db")

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Create time_weather_G10_db which will be used for inference 
# MAGIC CREATE OR REPLACE TEMP VIEW time_weather_G10_db AS 
# MAGIC SELECT 
# MAGIC time as ts,
# MAGIC year(time) as year,
# MAGIC month(time) as month,
# MAGIC dayofmonth(time) as dayofmonth,
# MAGIC dayofweek(time) AS dayofweek,
# MAGIC HOUR(time) AS hour,
# MAGIC feels_like,
# MAGIC COALESCE(`rain.1h`, 0 ) as rain_1h,
# MAGIC explode(weather.description),
# MAGIC isHoliday(year(time), month(time), day(time)) AS holiday
# MAGIC FROM weather_tmp_G10_db
# MAGIC ORDER BY time 

# COMMAND ----------

# %sql
# SELECT * FROM time_weather_G10_db

# COMMAND ----------

# MAGIC %md
# MAGIC ## Actual netchange table (gold - real_netChange_g)

# COMMAND ----------

# see station info 
display(spark.read.format('delta').load(BRONZE_STATION_INFO_PATH).filter(col("name") == GROUP_STATION_ASSIGNMENT))

# COMMAND ----------

# assign station id 
station_id = "66dc686c-0aca-11e7-82f6-3863bb44ef7c"
# define delta path 
gold_actual_netChange_delta = f"{GROUP_DATA_PATH}gold_actual_netChange.delta"

# COMMAND ----------

# with basic data processing create station_status_G10_db
from pyspark.sql.functions import to_date, cast, hour,col
statusDf = (
    spark.read.format('delta')
    .load(BRONZE_STATION_STATUS_PATH).filter(col("station_id") == station_id)
)
statusDf = (statusDf.withColumn( "ts",col("last_reported").cast("timestamp"))
                    .withColumn("hour", hour("ts"))
                    .withColumn("date", to_date("ts"))
                    .sort(col("ts").desc())
                    )
statusDf.select("ts", "date", "hour", "num_docks_available").createOrReplaceTempView("station_status_G10_db")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- with station_status_G10_db create real_netChange_G10_db
# MAGIC CREATE OR REPLACE TEMP VIEW real_netChange_G10_db AS
# MAGIC SELECT
# MAGIC   date_format(ts, 'yyyy-MM-dd HH:00:00') as ts,
# MAGIC   netChange
# MAGIC FROM (
# MAGIC   SELECT 
# MAGIC   *,
# MAGIC   num_docks_available - LEAD(num_docks_available) over (order by ts) netChange,
# MAGIC   ROW_NUMBER() OVER(ORDER BY ts DESC) as rn
# MAGIC FROM(
# MAGIC   SELECT *
# MAGIC   FROM
# MAGIC   station_status_G10_db
# MAGIC   WHERE ts IN (
# MAGIC     SELECT MIN(ts) AS ts 
# MAGIC     FROM station_status_G10_db
# MAGIC     GROUP BY date, hour 
# MAGIC )
# MAGIC )
# MAGIC ORDER BY ts DESC 
# MAGIC )
# MAGIC WHERE rn > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM real_netChange_G10_db

# COMMAND ----------

# write a table to delta path 
(
    spark.table("real_netChange_G10_db")
    .write
    .format("delta")
    .mode("overwrite")
    .save(gold_actual_netChange_delta)
)


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create real_netChange_g table 
# MAGIC CREATE OR REPLACE TABLE real_netChange_g AS
# MAGIC SELECT * FROM delta. `dbfs:/FileStore/tables/G10/gold_actual_netChange.delta/`

# COMMAND ----------

# %sql
# SELECT * FROM real_netChange_g

# COMMAND ----------

# get current time
spark.conf.set("spark.sql.session.timeZone", "America/New_York")


display(spark.sql("select date_trunc('hour', current_timestamp()) as current_time"))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Forecast of Bike Inventory

# COMMAND ----------

from pyspark.sql.functions import date_format
statusDf = spark.read.format("delta").load(BRONZE_STATION_STATUS_PATH).filter(col("station_id") == "66dc686c-0aca-11e7-82f6-3863bb44ef7c").withColumn("ts", date_format(col("last_reported").cast("timestamp"), 'yyyy-MM-dd HH:00:00')).sort(col("ts").desc())
statusDf = statusDf.select("num_docks_available" ,"num_bikes_available","ts").limit(1)

# COMMAND ----------

row = statusDf.collect()[0]
num_docks_available = row[0]
num_bikes_available = row[1]
last_reported_time = row[2]
station_capacity = 96

print("last_reported_time:" ,last_reported_time)
print("num docks: ", num_docks_available)
print("num bikes: ",num_bikes_available)
print("station capacity: ", station_capacity)


# COMMAND ----------

import json


# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
