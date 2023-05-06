# Databricks notebook source
pip install folium

# COMMAND ----------

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
import mlflow

ARTIFACT_PATH = GROUP_MODEL_NAME

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

info = (spark.read
    .format("delta")
    .load('dbfs:/FileStore/tables/G11/silver/station_info'))
print("Station Capacity: ", info.collect()[0][0])

# COMMAND ----------

station_status = (spark.read
    .format("delta")
    .load('dbfs:/FileStore/tables/G11/silver/station_status'))

display(station_status.filter(col("last_reported") <= currenthour).sort(desc("last_reported")).head(1))

# COMMAND ----------

real_time_inventory = station_status.withColumn("unix_rounded", (round(unix_timestamp("last_reported")/3600)*3600).cast("timestamp"))
real_time_inventory = real_time_inventory.withColumn("rounded_hour", date_format(from_unixtime(col("unix_rounded").cast("long")), "yyyy-MM-dd HH:mm:ss"))
real_time_inventory = real_time_inventory.drop("unix_rounded")

from pyspark.sql.functions import col, lag, coalesce
from pyspark.sql.window import Window

w = Window.orderBy("rounded_hour")
real_time_inventory = real_time_inventory.withColumn("diff", col("num_bikes_available") - lag(col("num_bikes_available"), 1).over(w))
real_time_inventory = real_time_inventory.withColumn("diff", coalesce(col("diff"), col("num_bikes_available")))
real_time_inventory = real_time_inventory.orderBy("rounded_hour", ascending=False)
from pyspark.sql.functions import monotonically_increasing_id
real_time_inventory = real_time_inventory.withColumn("index", monotonically_increasing_id())
from pyspark.sql.functions import when
diff = real_time_inventory.withColumn("difference", when(col('index').between(0, 7), None).otherwise(col('diff')))


#display(real_time_inventory)
#display(diff)

# COMMAND ----------

weather = (spark.read
    .format("delta")
    .load('dbfs:/FileStore/tables/G11/silver/weather'))
data = weather.join(real_time_inventory, weather.dt == real_time_inventory.rounded_hour, "inner")
test_data = data.toPandas()
test_data = test_data.rename(columns={'dt':'ds'}).rename(columns={'diff': 'y'})
test_data['ds'] = test_data['ds'].apply(pd.to_datetime)
#print(test_data)

# COMMAND ----------

client = MlflowClient()
latest_version_info = client.get_latest_versions(ARTIFACT_PATH, stages=['Production'])
latest_production_version = latest_version_info[0].version
print("The latest production version of the model '%s' is '%s'." %(ARTIFACT_PATH, latest_production_version))

# Predict on the future based on the production model
model_prod_uri = f'models:/{ARTIFACT_PATH}/production'
model_prod = mlflow.prophet.load_model(model_prod_uri)
prod_forecast = model_prod.predict(test_data)
prod_forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]

# COMMAND ----------

display(test_data)

# COMMAND ----------

prophet_plot = model_prod.plot(prod_forecast)

# COMMAND ----------

prophet_plot2 = model_prod.plot_components(prod_forecast)

# COMMAND ----------

# DBTITLE 1,Residual of production model
test_data.ds = pd.to_datetime(test_data.ds)
prod_forecast.ds = pd.to_datetime(prod_forecast.ds)
results = prod_forecast[['ds','yhat']].merge(test_data,on="ds")
results['residual'] = results['yhat'] - results['y']

# Plot the residuals

fig = px.scatter(
    results, x='yhat', y='residual',
    marginal_y='violin',
    trendline='ols'
)
fig.show()

# COMMAND ----------

latest_version_info = client.get_latest_versions(ARTIFACT_PATH, stages=['Staging'])
latest_staging_version = latest_version_info[0].version
print("The latest staging version of the model '%s' is '%s'." %(ARTIFACT_PATH, latest_staging_version))

# Predict on the future based on the staging model
model_staging_uri = f'models:/{ARTIFACT_PATH}/staging'
model_staging = mlflow.prophet.load_model(model_staging_uri)
staging_forecast = model_staging.predict(test_data)
staging_forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]

# COMMAND ----------



# COMMAND ----------

prophet_plot = model_staging.plot(staging_forecast)

# COMMAND ----------

prophet_plot2 = model_staging.plot_components(staging_forecast)

# COMMAND ----------

# DBTITLE 1,Residual of Staging Model
test_data.ds = pd.to_datetime(test_data.ds)
staging_forecast.ds = pd.to_datetime(staging_forecast.ds)
results = staging_forecast[['ds','yhat']].merge(test_data,on="ds")
results['residual'] = results['yhat'] - results['y']

# Plot the residuals

fig = px.scatter(
    results, x='yhat', y='residual',
    marginal_y='violin',
    trendline='ols'
)
fig.show()

# COMMAND ----------

# DBTITLE 1,Here it is only forecasting the next 4 hours
#I managed to get it to forecast only the hours specified in the widget but can't figure out how to show that more in detail
from datetime import timedelta
currentdate = pd.to_datetime(currentdate)
end_pred = currentdate + timedelta(hours=hours_to_forecast)
future_dates = pd.DataFrame(pd.date_range(start=currentdate, end= end_pred, freq='H'), columns=['ds'])
future_df = spark.createDataFrame(future_dates)
df = weather.join(future_df, future_df.ds == weather.dt, 'inner')
test = df.toPandas()
df.display()
forecast = model_prod.predict(test)
print(forecast)
model_prod.plot(forecast)

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
