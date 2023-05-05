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

from mlflow.tracking.client import MlflowClient
from sklearn.preprocessing import LabelEncoder
from pyspark.sql.functions import *
import plotly.express as px
import logging
import mlflow
import folium
logging.getLogger("py4j").setLevel(logging.ERROR)
spark.conf.set("spark.sql.session.timeZone", "America/New_York")

# COMMAND ----------

# Get and display current timestamp
current_time = spark.sql("select current_timestamp()")
unixTime = spark.sql("select to_unix_timestamp(date_trunc('hour', current_timestamp()))").collect()[0][0]
displayHTML("<h2>Current Timestamp:</h2>")
display(current_time)

# COMMAND ----------

# Get and display Production and Staging Model version
client = MlflowClient()
prod_model = client.get_latest_versions(GROUP_MODEL_NAME, stages=["Production"])
staging_model = client.get_latest_versions(GROUP_MODEL_NAME, stages=["Staging"])
displayHTML(f"<h2>Production Model version:</h2><h3>The latest production version of the model {GROUP_MODEL_NAME} is {prod_model[0].version}.</h3><h2>Staging Model version:</h2><h3>The latest staging version of the model {GROUP_MODEL_NAME} is {staging_model[0].version}.</h3>")

# COMMAND ----------

# Create map of station location and header for map
displayHTML(f"<h2>Station Name: {GROUP_STATION_ASSIGNMENT}</h2>")
map=folium.Map(location=[40.751551,-73.993934], zoom_start=17, min_zoom=17, max_zoom=17)
map.add_child(folium.Marker(location=[40.751551,-73.993934], popup=GROUP_STATION_ASSIGNMENT, icon=folium.Icon(color='red')))
map

# COMMAND ----------

# Get and display current weather (Temp and Percent Chance of Precip)
@udf
def kelvinToFahrenheit(kelvin):
    return kelvin * 1.8 - 459.67
spark.udf.register("kelvinToFahrenheit", kelvinToFahrenheit)

dfWeather = spark.read.load(BRONZE_NYC_WEATHER_PATH)
displayHTML("<h2>Current Weather:</h2>")
display(dfWeather.select('dt','temp','pop').filter(dfWeather.dt==unixTime).withColumn(u"Temperature (°F)", round(kelvinToFahrenheit(col('temp')))).withColumn('Chance of Precipitation', col('pop')).drop('dt','temp','pop'))

# COMMAND ----------

# Total docks and total bikes available at this station
statusDf = spark.read.load(BRONZE_STATION_STATUS_PATH).filter(col("station_id")=="66dc686c-0aca-11e7-82f6-3863bb44ef7c").withColumn("ts", date_format(col("last_reported").cast("timestamp"), 'yyyy-MM-dd HH:00:00')).sort(col("ts").desc())
statusInfo = statusDf.select("num_docks_available", "num_bikes_available", "ts").limit(1).collect()[0]
num_docks_available = statusInfo[0]
num_bikes_available = statusInfo[1]
last_report_time = statusInfo[2]
station_capacity = 96
displayHTML(f"<br><h2>Docks and Bikes Information:</h2><h3>Last reported time: {last_report_time}</h3><h3>Total docks at this station: {station_capacity}</h3><h3>Total docks available at this station: {num_docks_available}</h3><h3>Total bikes available at this station: {num_bikes_available}</h3>")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Preprocessing for Test Data

# COMMAND ----------

# Load Dataset from time_weather_G10_db
test_data = spark.sql('select * from time_weather_G10_db').toPandas()

# Rename the timestamp column to 'ds' and the target column to 'y'
test_data = test_data.rename(columns={'ts': 'ds'}).rename(columns={'net_change': 'y'})

# Change str to datetime
test_data['ds'] = test_data['ds'].apply(pd.to_datetime)

# Fill missing values of 'feel_like' and 'rain_1h' with mean value
test_data["feels_like"].fillna(test_data["feels_like"].mean(), inplace=True)
test_data["rain_1h"].fillna(test_data["rain_1h"].mean(), inplace=True)

# Create a LabelEncoder instance and apply it to the 'description' column
test_data['description'] = LabelEncoder().fit_transform(test_data['description'])

# Replace 'false' with 0 and 'true' with 1 in the 'holiday' column
test_data['holiday'] = test_data['holiday'].replace({'false': 0, 'true': 1})

test_data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Forecast Based on the Production and Staging Model

# COMMAND ----------

# Predict on the future based on the production model
model_prod_uri = f'models:/{GROUP_MODEL_NAME}/production'
model_prod = mlflow.prophet.load_model(model_prod_uri)
prod_forecast = model_prod.predict(test_data)[['ds', 'yhat']]

# Predict on the future based on the staging model
model_staging_uri = f'models:/{GROUP_MODEL_NAME}/staging'
model_staging = mlflow.prophet.load_model(model_staging_uri)
staging_forecast = model_staging.predict(test_data)[['ds', 'yhat']]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compare Two Models and Perform a Model Stage Transition

# COMMAND ----------

# Update the production model if needed and staging model is better
if promote_model:
    test_truth = spark.sql('select * from real_netChange_g').toPandas()
    test_truth['ts'] = test_truth['ts'].apply(pd.to_datetime)
    prod_results = prod_forecast.merge(test_truth, left_on='ds', right_on='ts')
    prod_mae = np.mean(prod_results['yhat'] - prod_results['netChange'])
    staging_results = staging_forecast.merge(test_truth, left_on='ds', right_on='ts')
    staging_mae = np.mean(staging_results['yhat'] - staging_results['netChange'])
    
    # If staging model has lower MAE, then move staging model to 'production' and production model to 'archive'
    if staging_mae < prod_mae:
        latest_production = client.get_latest_versions(GROUP_MODEL_NAME, stages=["Production"])[0]
        client.transition_model_version_stage(name=GROUP_MODEL_NAME, version=latest_production.version, stage='Archive')
        latest_staging = client.get_latest_versions(GROUP_MODEL_NAME, stages=["Staging"])[0]
        client.transition_model_version_stage(name=GROUP_MODEL_NAME, version=latest_staging.version, stage='Production')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get the Latest Production and Staging Model

# COMMAND ----------

# Predict on the future based on the production model
model_prod_uri = f'models:/{GROUP_MODEL_NAME}/production'
model_prod = mlflow.prophet.load_model(model_prod_uri)
prod_forecast = model_prod.predict(test_data)[['ds', 'yhat']]

# Predict on the future based on the staging model
model_staging_uri = f'models:/{GROUP_MODEL_NAME}/staging'
model_staging = mlflow.prophet.load_model(model_staging_uri)
staging_forecast = model_staging.predict(test_data)[['ds', 'yhat']]

# Combine two dataframes together
prod_forecast['stage'] = 'prod'
staging_forecast['stage'] = 'staging'
df_forecast = pd.concat([prod_forecast, staging_forecast]).sort_values(['ds', 'stage']).reset_index(drop=True)
df_forecast

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the Gold forecast prod/stage Table

# COMMAND ----------

# A gold data table should store inference and monitoring data


# COMMAND ----------

# Calculate the recent num_bikes_available
bike_forecast = df_forecast[(df_forecast.ds > last_report_time) & (df_forecast.stage == 'prod')].reset_index(drop=True)
bike_forecast['bikes_available'] = bike_forecast['yhat'].cumsum().round().astype(int) + num_bikes_available
bike_forecast['ds'] = bike_forecast['ds'].dt.strftime("%Y-%m-%d %H:%M:%S")

# Forecast the available bikes for the next hours_to_forecast hours
displayHTML(f"<h2>Forecast the available bikes for the next {hours_to_forecast} hours:</h2>")
display(bike_forecast[['ds', 'bikes_available']].head(int(hours_to_forecast)))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Forecast of Bike Inventory

# COMMAND ----------

# Highlight any stock out or full station conditions over the predicted period
bike_forecast['station_capacity'] = station_capacity
bike_forecast['ds'] = pd.to_datetime(bike_forecast['ds'], format="%Y-%m-%d %H:%M:%S")
fig = px.line(bike_forecast, x='ds', y=['bikes_available','station_capacity'], color_discrete_sequence=['blue','black'])
fig.update_layout(title=f'{GROUP_STATION_ASSIGNMENT} bike forecast')
fig.update_xaxes(title_text='time')
fig.update_yaxes(title_text='bikes_available')
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Residual Plot - Model Comparison

# COMMAND ----------

# Calcluate residuals
test_truth = spark.sql('select * from real_netChange_g').toPandas()
test_truth['ts'] = test_truth['ts'].apply(pd.to_datetime)
results = df_forecast.merge(test_truth, left_on='ds', right_on='ts')
results['residual'] = results['yhat'] - results['netChange']

# Plot the residuals
fig = px.scatter(results, x='yhat', y='residual', color='stage', marginal_y='violin', trendline='ols')
fig.update_layout(title=f'{GROUP_STATION_ASSIGNMENT} rental forecast model performance comparison')
fig.show()

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
