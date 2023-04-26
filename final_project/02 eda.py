# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

start_date = str(dbutils.widgets.get('01.start_date'))
end_date = str(dbutils.widgets.get('02.end_date'))
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

print(start_date,end_date,hours_to_forecast, promote_model)
print("YOUR CODE HERE...")

# COMMAND ----------

# MAGIC %pip install --upgrade holidays

# COMMAND ----------

# MAGIC %sql 
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %md
# MAGIC # Monthly trip trends

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW date_bike_G10_db AS (
# MAGIC   SELECT  YEAR(started_at) as year, MONTH(started_at) as month, DAY(started_at) as day, HOUR(started_at) AS hour, * FROM historic_bike_trip_b
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT year, COUNT(*) AS count FROM date_bike_G10_db
# MAGIC GROUP BY year
# MAGIC SORT BY year

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use only 2022 data 

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT month, count(*) as count FROM date_bike_G10_db
# MAGIC WHERE year == 2022
# MAGIC GROUP BY month
# MAGIC SORT BY month

# COMMAND ----------

# MAGIC %md 
# MAGIC Count increases at summer

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT day, count(*) as count FROM date_bike_G10_db
# MAGIC WHERE year == 2022
# MAGIC GROUP BY day
# MAGIC SORT BY day

# COMMAND ----------

# MAGIC %md 
# MAGIC # holiday Trend

# COMMAND ----------

import holidays
from datetime import date

# COMMAND ----------

us_holidays = holidays.US()

# COMMAND ----------

@udf
def isHoliday(year, month, day):
    return date(year, month, day) in us_holidays
spark.udf.register("isHoliday", isHoliday)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT holiday, avg(count) as count FROM(
# MAGIC   SELECT date, count(*) as count , max(holiday) as holiday
# MAGIC   FROM (
# MAGIC     SELECT concat(year, " ", month, " ", day) AS date, isHoliday(year, month, day) AS holiday, * FROM date_bike_G10_db
# MAGIC   )
# MAGIC GROUP BY date
# MAGIC )
# MAGIC GROUP BY holiday

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT holiday, count(*) as count FROM(
# MAGIC   SELECT *, isHoliday(year, month, day) AS holiday FROM date_bike_G10_db
# MAGIC )
# MAGIC GROUP BY holiday

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM date_bike_G10_db

# COMMAND ----------

# MAGIC %md
# MAGIC # Weather

# COMMAND ----------

import pandas_profiling

# COMMAND ----------

df = spark.table("historic_weather_b")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM historic_weather_b

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW historic_bike_weather_G10_db AS(
# MAGIC    SELECT day, hour, feels_like, humidity, wind_speed, main, description
# MAGIC  FROM date_bike_G10_db AS B JOIN historic_weather_b AS W
# MAGIC  ON W.time BETWEEN B.started_at AND B.ended_at
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM historic_bike_weather_G10_db

# COMMAND ----------

displayHTML(pandas_profiling.ProfileReport(df.toPandas()).html)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM historic_weather_b

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT to_date(time) from historic_weather_b

# COMMAND ----------



# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
