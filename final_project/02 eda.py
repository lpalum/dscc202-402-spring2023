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

pip install -U pandas-profiling

# COMMAND ----------

# DBTITLE 1,Imports
from pathlib import Path
from pyspark.sql.functions import *

import matplotlib.pyplot as plt
import seaborn as sns

import numpy as np
import requests

import pandas_profiling
import pandas as pd
from pandas_profiling.utils.cache import cache_file

# COMMAND ----------

# DBTITLE 1,historic_trip_data
historic_trip_data = (spark.read.format("delta").load("dbfs:/FileStore/tables/G11/historic_trip_data_bronze"))
historic_trip_data.display()
historic_trip_data.printSchema()


# COMMAND ----------

from pyspark.sql.functions import *
df = (historic_trip_data.withColumn("month", month("started_at")))
df1 = (df.select("month", "rideable_type"))
df1.display()

# COMMAND ----------

df.groupBy(df1.month).count().orderBy(df.month).show()

# COMMAND ----------

from pyspark.sql.functions import *
df2 = (historic_trip_data.withColumn("day", dayofyear("started_at")))
df3 = (df2.select("day", "rideable_type"))
df3.display()

# COMMAND ----------

df4 = df2.groupBy(df3.day).count().orderBy(df2.day).show()

# COMMAND ----------

pip install holidays

# COMMAND ----------


from datetime import date
import holidays
from pyspark.sql.functions import *

us_holidays = holidays.US()

for p in holidays.US(years = 2020).items():  
    print(p) 

hol = (historic_trip_data.withColumn("holiday", ("started_at")))
#hol1 = (hol.select("day", "rideable_type"))
#hol1.display()


# COMMAND ----------

historic_trip_data_df = historic_trip_data.select("*").toPandas()
historic_trip_data_df['started_at']= pd.to_datetime(historic_trip_data_df['started_at'])
historic_trip_data_df['ended_at']= pd.to_datetime(historic_trip_data_df['ended_at'])

historic_trip_data_profile = pandas_profiling.ProfileReport(historic_trip_data_df)
historic_trip_data_profile

# COMMAND ----------

# DBTITLE 1,bronze_station_status
bronze_station_status = (spark.read.format("delta").load("dbfs:/FileStore/tables/G11/bronze_station_status"))
bronze_station_status.display()

# COMMAND ----------

bronze_station_status_df = bronze_station_status.select("*").toPandas()
bronze_station_status_profile = pandas_profiling.ProfileReport(bronze_station_status_df)
bronze_station_status_profile

# COMMAND ----------

# DBTITLE 1,bronze_station_info
bronze_station_info = (spark.read.format("delta").load("dbfs:/FileStore/tables/G11/bronze_station_info"))
bronze_station_info.display()

# COMMAND ----------

bronze_station_info_df = bronze_station_info.select("*").toPandas()
bronze_station_info_profile = pandas_profiling.ProfileReport(bronze_station_info_df)
bronze_station_info_profile

# COMMAND ----------

# DBTITLE 1,historic_weather
historic_weather = (spark.read.format("delta").load("dbfs:/FileStore/tables/G11/historic_weather_df"))
historic_weather.display()

# COMMAND ----------

#Counts how many disctinct descriptions of the weather
print("Distinct Count: " + str(historic_weather.select("description").distinct().count()))
print("Distinct Count: " + str(historic_weather.select("description").distinct().count()))

# COMMAND ----------

historic_weather_df = historic_weather.select("*").toPandas()
historic_weather_profile = pandas_profiling.ProfileReport(historic_weather_df)
historic_weather_profile

# COMMAND ----------

# DBTITLE 1,More in depth EDA


# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))

# COMMAND ----------


