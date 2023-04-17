# Databricks notebook source
start_date = str(dbutils.widgets.get('01.start_date'))
end_date = str(dbutils.widgets.get('02.end_date'))
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

print(start_date,end_date,hours_to_forecast, promote_model)
print("YOUR CODE HERE...")

# COMMAND ----------

# MAGIC %run ./includes/includes

# COMMAND ----------

import json

# Return Success
#dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))

# COMMAND ----------

from pathlib import Path

import numpy as np
import requests


# COMMAND ----------

historic_trip_data_df = (spark.read
     .format("delta")
     .load("dbfs:/FileStore/tables/G11/historic_trip_data_bronze"))
historic_trip_data_df.display()
historic_trip_data_df.printSchema()


# COMMAND ----------

from pyspark.sql.functions import *
df = (historic_trip_data_df.withColumn("month", month("started_at")))
df1 = (df.select("month", "rideable_type"))
df1.display()

# COMMAND ----------

df.groupBy(df1.month).count().orderBy(df.month).show()

# COMMAND ----------

bronze_station_status_df = (spark.readStream
                           .format("delta")
                           .load("dbfs:/FileStore/tables/G11/bronze_station_status"))
bronze_station_status_df.display()

# COMMAND ----------

pip install -U pandas-profiling

# COMMAND ----------

bronze_station_info_df = (spark.readStream
                         .format("delta")
                         .load("dbfs:/FileStore/tables/G11/bronze_station_info"))
bronze_station_info_df.display()

# COMMAND ----------

#For historic_trip_data_df

import pandas_profiling
import pandas as pd
from pandas_profiling.utils.cache import cache_file

historic_trip_data_df = (spark.read.format("delta").load("dbfs:/FileStore/tables/G11/historic_trip_data_bronze"))

df = historic_trip_data_df.select("*").toPandas()


# COMMAND ----------

profile = pandas_profiling.ProfileReport(df)
profile

# COMMAND ----------

#For bronze_station_status_df

import pandas_profiling
import pandas as pd
from pandas_profiling.utils.cache import cache_file

bronze_station_status_df = (spark.read.format("delta").load("dbfs:/FileStore/tables/G11/bronze_station_status"))
df = bronze_station_status_df.select("*").toPandas()

# COMMAND ----------

profile = pandas_profiling.ProfileReport(df)
profile

# COMMAND ----------

import pandas_profiling
import pandas as pd
from pandas_profiling.utils.cache import cache_file

bronze_station_info_df = (spark.read.format("delta").load("dbfs:/FileStore/tables/G11/bronze_station_info"))

df = bronze_station_info_df.select("*").toPandas()


# COMMAND ----------

profile = pandas_profiling.ProfileReport(df)
profile

# COMMAND ----------

historic_weather_df = (spark.readStream.format("delta").load("dbfs:/FileStore/tables/G11/historic_weather"))
historic_weather_df.display()

historic_weather_df = (spark.read.format("delta").load("dbfs:/FileStore/tables/G11/historic_weather"))
historic_weather_df.display()
historic_weather_df.printSchema()


# COMMAND ----------

import pandas_profiling
import pandas as pd
from pandas_profiling.utils.cache import cache_file

historic_weather_data_df = (spark.read.format("delta").load("dbfs:/FileStore/tables/G11/historic_weather_df"))
df = historic_weather_data_df.select("*").toPandas()

# COMMAND ----------

#Reading stream for historic trip data bronze
historic_trip_data_df = (spark.read
     .format("delta")
     .load("dbfs:/FileStore/tables/G11/historic_trip_data_bronze"))
historic_trip_data_df.display()
historic_trip_data_df.printSchema()
#here we have a bar chart displaying the end spots of the bikes and which spots are most common
#It is also grouped by if its a member or a casual user

# COMMAND ----------

historic_weather_df = (spark.read
                      .option("header", True)
                      .csv(NYC_WEATHER_FILE_PATH))
historic_weather_df.display()

# COMMAND ----------

#Counts how many disctinct descriptions of the weather
print("Distinct Count: " + str(historic_weather_df.select("description").distinct().count()))
print("Distinct Count: " + str(historic_weather_df.select("description").distinct().count()))

# COMMAND ----------

#Displays the basic summary of the dataframe this can be used to make decisions
historic_trip_data_df.describe().show()

# COMMAND ----------

#Displays the basic summary of the dataframe this can be used to make decisions
historic_weather_df.describe().show()

# COMMAND ----------

historic_trip_data_df

# COMMAND ----------

from pyspark.sql.functions import *
df = (historic_trip_data_df.withColumn("month", month("started_at")))
df1 = (df.select("month", "rideable_type"))
df1.display()


# COMMAND ----------

df.groupBy(df1.month).count().orderBy(df.month).show()

# COMMAND ----------

pip install -U pandas-profiling

# COMMAND ----------

#setting up the pandas profiling
import pandas_profiling
import pandas as pd
from pandas_profiling.utils.cache import cache_file


trip_prof = historic_trip_data_df.select("*").toPandas()

# COMMAND ----------

profile = pandas_profiling.ProfileReport(trip_prof)
profile

# COMMAND ----------

()()bronze_station_status_df = (spark.readStream
                           .format("delta")
                           .load("dbfs:/FileStore/tables/G11/bronze_station_status"))
bronze_station_status_df.display()

# COMMAND ----------

bronze_station_info_df = (spark.readStream
                         .format("delta")
                         .load("dbfs:/FileStore/tables/G11/bronze_station_info"))
bronze_station_info_df.display()

# COMMAND ----------



# COMMAND ----------



display(df)

