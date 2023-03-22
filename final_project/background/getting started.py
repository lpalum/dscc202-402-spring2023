# Databricks notebook source
# DBTITLE 1,Variables to be used within your project
# MAGIC %run "../includes/includes"

# COMMAND ----------

# DBTITLE 1,Display Historic Trip Files
display(dbutils.fs.ls(BIKE_TRIP_DATA_PATH))

# COMMAND ----------

wdf=spark.read.format('csv').option("header","True").option("inferSchema","True").load(NYC_WEATHER_FILE_PATH)
display(wdf)

# COMMAND ----------

bdf=spark.read.format('csv').option("header","True").option("inferSchema","True").load(BIKE_TRIP_DATA_PATH+'202111_citibike_tripdata.csv')
display(bdf)

# COMMAND ----------

display(spark.read.format('delta').load(BRONZE_STATION_INFO_PATH))

# COMMAND ----------

display(spark.read.format('delta').load(BRONZE_STATION_STATUS_PATH))

# COMMAND ----------

display(spark.read.format('delta').load(BRONZE_NYC_WEATHER_PATH))
