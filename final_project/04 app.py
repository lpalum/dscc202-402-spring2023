# Databricks notebook source

pip install folium

# COMMAND ----------

# MAGIC %run ./includes/includes

# COMMAND ----------

start_date = str(dbutils.widgets.get('01.start_date'))
end_date = str(dbutils.widgets.get('02.end_date'))
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

print(start_date,end_date,hours_to_forecast, promote_model)


# COMMAND ----------


@udf

def kelvinToFahrenheit(kelvin):
    return kelvin * 1.8 - 459.67

spark.udf.register("kelvinToFahrenheit", kelvinToFahrenheit)    
    

# COMMAND ----------

import folium
from pyspark.sql.functions import *

#set time zone

spark.conf.set("spark.sql.session.timeZone", "America/New_York")

#Get and display current timestamp

df2= spark.sql("select current_timestamp()")
df3 = spark.sql("select to_unix_timestamp(date_trunc('hour', current_timestamp()))")
unixTime=df3.collect()[0][0]
displayHTML("<h2>Current Timestamp: </h2>")
display(df2)


#Get and display current weather (Temp and Percent Chance of Precip)

dfWeather = spark.read.load(BRONZE_NYC_WEATHER_PATH)
displayHTML("<br><br><h2>Current Weather Information:</h2>")
display(dfWeather.select('dt','temp','pop').filter(dfWeather.dt ==unixTime).withColumn("Temperature",round(kelvinToFahrenheit(col('temp')))).withColumn('Chance of Precipitation', col('pop')).drop('dt','temp','pop'))


# Create map of station location and header for map

displayHTML("<br><br><h2>Station: <br> <br> 8 Ave & W 33 St</h2>")
map=folium.Map(location=[40.751551,-73.993934], zoom_start=17, min_zoom=17, max_zoom=17)
map.add_child(folium.Marker(location=[40.751551,-73.993934],popup='8 Ave & W 33 St',icon=folium.Icon(color='red')))
map


# COMMAND ----------

import json


# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))

# COMMAND ----------


