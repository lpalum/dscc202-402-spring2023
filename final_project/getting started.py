# Databricks notebook source
# MAGIC %run "./includes/includes"

# COMMAND ----------

display(dbutils.fs.ls(BIKE_TRIP_DATA_PATH))

# COMMAND ----------

wdf=spark.read.format('csv').option("header","True").option("inferSchema","True").load(NYC_WEATHER_FILE_PATH)
display(wdf)

# COMMAND ----------

bdf=spark.read.format('csv').option("header","True").option("inferSchema","True").load(BIKE_TRIP_DATA_PATH+'202111_citibike_tripdata.csv')
display(bdf)

# COMMAND ----------

lup,status_df = get_bike_station_status()
dt = datetime.datetime.fromtimestamp(lup)
print(dt)
status_df.head()

# COMMAND ----------

lup,station_df = get_bike_stations()
dt = datetime.datetime.fromtimestamp(lup)
print(dt)
station_df.head()

# COMMAND ----------

get_current_weather()
