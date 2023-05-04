# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

# DBTITLE 0,YOUR APPLICATIONS CODE HERE...
start_date = str(dbutils.widgets.get('01.start_date'))
end_date = str(dbutils.widgets.get('02.end_date'))
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

print(start_date,end_date,hours_to_forecast, promote_model)

print("YOUR CODE HERE...")

# COMMAND ----------

import plotly.express as px
import pandas as pd
import matplotlib.pyplot as plt
from mlflow.tracking.client import MlflowClient
import datetime
from pyspark.sql.functions import *



# COMMAND ----------

currentdate = pd.Timestamp.now(tz='US/Eastern').round(freq="H")
fmt = '%Y-%m-%d %H:%M:%S'
currenthour = currentdate.strftime("%Y-%m-%d %H") 
currentdate = currentdate.strftime(fmt) 
print("The current timestamp is:",currentdate)

# COMMAND ----------

client = MlflowClient()
prod_model = client.get_latest_versions(GROUP_MODEL_NAME, stages=['Production'])
stage_model = client.get_latest_versions(GROUP_MODEL_NAME, stages=['Staging'])

# COMMAND ----------

print("Production Model Details: ")
print(prod_model)

# COMMAND ----------

print("Staging Model Details: ")
print(stage_model)

# COMMAND ----------

pip install folium

# COMMAND ----------

import folium

print("Assigned Station: ", GROUP_STATION_ASSIGNMENT)

# Create a map centered at the given latitude and longitude
lat, lon = 40.722062, -73.997278
map = folium.Map(location=[lat, lon], zoom_start=12)

# Add a marker at the given latitude and longitude
folium.Marker(location=[lat, lon], icon=folium.Icon(color='blue')).add_to(map)

# Display the map
map

# COMMAND ----------

weather_df = (spark.read
    .format("delta")
    .load('dbfs:/FileStore/tables/G11/silver/weather')
    .toPandas())
print("Current Weather: ")
print(weather_df[weather_df.dt==currentdate].reset_index(drop=True))

# COMMAND ----------



# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
