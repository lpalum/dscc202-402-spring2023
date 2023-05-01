# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# DBTITLE 0,YOUR APPLICATIONS CODE HERE...
start_date = str(dbutils.widgets.get('01.start_date'))
end_date = str(dbutils.widgets.get('02.end_date'))
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

print(start_date,end_date,hours_to_forecast, promote_model)

print("YOUR CODE HERE...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table for Inference 

# COMMAND ----------

import holidays
from datetime import date

us_holidays = holidays.US()

@udf
def isHoliday(year, month, day):
    return date(year, month, day) in us_holidays
spark.udf.register("isHoliday", isHoliday)

# COMMAND ----------

spark.read.format('delta').load(BRONZE_NYC_WEATHER_PATH).createOrReplaceTempView("weather_tmp_G10_db")

# COMMAND ----------

# MAGIC %sql
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

# MAGIC %sql
# MAGIC SELECT * FROM time_weather_G10_db

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold table (actual netchange-y)

# COMMAND ----------

display(spark.read.format('delta').load(BRONZE_STATION_INFO_PATH).filter(col("name") == GROUP_STATION_ASSIGNMENT))

# COMMAND ----------

station_id = "66dc686c-0aca-11e7-82f6-3863bb44ef7c"

# COMMAND ----------

gold_actual_netChange_delta = f"{GROUP_DATA_PATH}gold_actual_netChange.delta"

# COMMAND ----------

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

(
    spark.table("real_netChange_G10_db")
    .write
    .format("delta")
    .mode("overwrite")
    .save(gold_actual_netChange_delta)
)


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE real_netChange_g AS
# MAGIC SELECT * FROM delta. `dbfs:/FileStore/tables/G10/gold_actual_netChange.delta/`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM real_netChange_g

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
