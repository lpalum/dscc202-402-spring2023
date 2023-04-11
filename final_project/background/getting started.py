# Databricks notebook source
# DBTITLE 1,Variables to be used within your project
# MAGIC %run "../includes/includes"

# COMMAND ----------

# DBTITLE 1,Display Historic Trip Files
display(dbutils.fs.ls(BIKE_TRIP_DATA_PATH))

# COMMAND ----------

bdf=spark.read.format('csv').option("header","True").option("inferSchema","True").load(BIKE_TRIP_DATA_PATH+'202111_citibike_tripdata.csv')
display(bdf)

# COMMAND ----------

bdf.printSchema()

# COMMAND ----------

# DBTITLE 1,Display Historic Weather Data
wdf=spark.read.format('csv').option("header","True").option("inferSchema","True").load(NYC_WEATHER_FILE_PATH+'NYC_Weather_Data.csv')
display(wdf)

# COMMAND ----------

wdf.printSchema()

# COMMAND ----------

# DBTITLE 1,Display Bike Station Information
display(spark.read.format('delta').load(BRONZE_STATION_INFO_PATH))

# COMMAND ----------

# DBTITLE 1,Display the Bike Station Status Information
display(spark.read.format('delta').load(BRONZE_STATION_STATUS_PATH))

# COMMAND ----------

# DBTITLE 1,Display the current (within the hour) NYC Weather Information
display(spark.read.format('delta').load(BRONZE_NYC_WEATHER_PATH))
