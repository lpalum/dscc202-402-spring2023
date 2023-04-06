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

# ingest weather data 

wdf=spark.read.format('csv').option("header","True").option("inferSchema","True").load(NYC_WEATHER_FILE_PATH)
display(wdf)

# COMMAND ----------


bdf=spark.read.format('csv').option("header","True").option("inferSchema","True").load(BIKE_TRIP_DATA_PATH+'202111_citibike_tripdata.csv')
display(bdf)

# COMMAND ----------

# DBTITLE 1,Display Bike Station Information
display(spark.read.format('delta').load(BRONZE_STATION_INFO_PATH).filter(col("name") == "8 Ave & W 33 St"))


# COMMAND ----------

# DBTITLE 1,Display the Bike Station Status Information
statusDf = spark.read.format('delta').load(BRONZE_STATION_STATUS_PATH).filter(col("station_id") == "66dc686c-0aca-11e7-82f6-3863bb44ef7c")
statusDf = statusDf.withColumn( "last_reported", col("last_reported").cast("timestamp")).sort(col("last_reported").desc())
display(statusDf)


# COMMAND ----------

# DBTITLE 1,Display the current (within the hour) NYC Weather Information
display(spark.read.format('delta').load(BRONZE_NYC_WEATHER_PATH).sort(col("time").desc()))

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
