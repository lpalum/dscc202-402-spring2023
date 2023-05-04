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

from pyspark.sql.functions import to_date, cast, hour,col
from mlflow.tracking.client import MlflowClient
from sklearn.preprocessing import LabelEncoder
import plotly.express as px
import logging
import mlflow

ARTIFACT_PATH = "G10_model"
logging.getLogger("py4j").setLevel(logging.ERROR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table for Inference 

# COMMAND ----------

# create and register function
import holidays
from datetime import date

us_holidays = holidays.US()

@udf
def isHoliday(year, month, day):
    return date(year, month, day) in us_holidays
spark.udf.register("isHoliday", isHoliday)

# COMMAND ----------

# create weather_tmp_G10_db from delta file
spark.read.format('delta').load(BRONZE_NYC_WEATHER_PATH).createOrReplaceTempView("weather_tmp_G10_db")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create time_weather_G10_db which will be used for inference 
# MAGIC CREATE OR REPLACE TEMP VIEW time_weather_G10_db AS 
# MAGIC SELECT 
# MAGIC time as ts,
# MAGIC year(time) as year,
# MAGIC month(time) as month,
# MAGIC dayofmonth(time) as dayofmonth,
# MAGIC dayofweek(time) AS dayofweek,
# MAGIC HOUR(time) AS hour,
# MAGIC feels_like,
# MAGIC COALESCE(`rain.1h`, 0 ) as rain_1h,
# MAGIC explode(weather.description) AS description,
# MAGIC isHoliday(year(time), month(time), day(time)) AS holiday
# MAGIC FROM weather_tmp_G10_db
# MAGIC ORDER BY time 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM time_weather_G10_db

# COMMAND ----------

# MAGIC %md
# MAGIC ## Actual netchange table (gold - real_netChange_g)

# COMMAND ----------

# see station info 
display(spark.read.format('delta').load(BRONZE_STATION_INFO_PATH).filter(col("name") == GROUP_STATION_ASSIGNMENT))

# COMMAND ----------

# assign station id 
station_id = "66dc686c-0aca-11e7-82f6-3863bb44ef7c"
# define delta path 
gold_actual_netChange_delta = f"{GROUP_DATA_PATH}gold_actual_netChange.delta"

# COMMAND ----------

# with basic data processing create station_status_G10_db
statusDf = (
    spark.read.format('delta')
    .load(BRONZE_STATION_STATUS_PATH).filter(col("station_id") == station_id)
)
statusDf = (statusDf.withColumn( "ts",col("last_reported").cast("timestamp"))
                    .withColumn("hour", hour("ts"))
                    .withColumn("date", to_date("ts"))
                    .sort(col("ts").desc())
                    )
statusDf.select("ts", "date", "hour", "num_docks_available").createOrReplaceTempView("station_status_G10_db")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- with station_status_G10_db create real_netChange_G10_db
# MAGIC CREATE OR REPLACE TEMP VIEW real_netChange_G10_db AS
# MAGIC SELECT
# MAGIC   date_format(ts, 'yyyy-MM-dd HH:00:00') as ts,
# MAGIC   netChange
# MAGIC FROM (
# MAGIC   SELECT 
# MAGIC   *,
# MAGIC   num_docks_available - LEAD(num_docks_available) over (order by ts) netChange,
# MAGIC   ROW_NUMBER() OVER(ORDER BY ts DESC) as rn
# MAGIC FROM(
# MAGIC   SELECT *
# MAGIC   FROM
# MAGIC   station_status_G10_db
# MAGIC   WHERE ts IN (
# MAGIC     SELECT MIN(ts) AS ts 
# MAGIC     FROM station_status_G10_db
# MAGIC     GROUP BY date, hour 
# MAGIC )
# MAGIC )
# MAGIC ORDER BY ts DESC 
# MAGIC )
# MAGIC WHERE rn > 1

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM real_netChange_G10_db

# COMMAND ----------

# write a table to delta path 
(
    spark.table("real_netChange_G10_db")
    .write
    .format("delta")
    .mode("overwrite")
    .save(gold_actual_netChange_delta)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create real_netChange_g table 
# MAGIC CREATE OR REPLACE TABLE real_netChange_g AS
# MAGIC SELECT * FROM delta. `dbfs:/FileStore/tables/G10/gold_actual_netChange.delta/`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM real_netChange_g

# COMMAND ----------

# DBTITLE 1,Data Preprocessing for Test Data
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

# DBTITLE 1,Forecast on the Production Model
# Load the latest version of the production model
client = MlflowClient()
latest_version_info = client.get_latest_versions(ARTIFACT_PATH, stages=["Production"])
latest_production_version = latest_version_info[0].version
print("The latest production version of the model '%s' is '%s'." % (ARTIFACT_PATH, latest_production_version))

# Predict on the future based on the production model
model_prod_uri = f'models:/{ARTIFACT_PATH}/production'
model_prod = mlflow.prophet.load_model(model_prod_uri)
prod_forecast = model_prod.predict(test_data)
prod_forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]

# COMMAND ----------

prophet_plot = model_prod.plot(prod_forecast)

# COMMAND ----------

prophet_plot2 = model_prod.plot_components(prod_forecast)

# COMMAND ----------

# DBTITLE 1,Forecast on the Staging Model
# Load the latest version of the staging model
client = MlflowClient()
latest_version_info = client.get_latest_versions(ARTIFACT_PATH, stages=["Staging"])
latest_staging_version = latest_version_info[0].version
print("The latest staging version of the model '%s' is '%s'." % (ARTIFACT_PATH, latest_staging_version))

# Predict on the future based on the staging model
model_staging_uri = f'models:/{ARTIFACT_PATH}/staging'
model_staging = mlflow.prophet.load_model(model_staging_uri)
staging_forecast = model_staging.predict(test_data)
staging_forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]

# COMMAND ----------

# DBTITLE 1,Create a Residual Plot
# Merge all results into one table and calcluate residuals
test_truth = spark.sql('select * from real_netChange_g').toPandas()
test_truth['ts'] = test_truth['ts'].apply(pd.to_datetime)
results = test_data.merge(test_truth, how='right', left_on='ds', right_on='ts')

prod_results = results[['ds']].merge(prod_forecast[['ds','yhat']], on='ds')
prod_results['stage'] = 'prod'
staging_results = results[['ds']].merge(staging_forecast[['ds','yhat']], on='ds')
staging_results['stage'] = 'staging'
prod_staging = pd.concat([prod_results, staging_results])
results = results.merge(prod_staging, how='right', on='ds')

results['residual'] = results['yhat'] - results['netChange']
results

# COMMAND ----------

# Plot the residuals
fig = px.scatter(results, x='yhat', y='residual', color='stage', marginal_y='violin', trendline='ols')
fig.update_layout(title=f"{GROUP_STATION_ASSIGNMENT} rental forecast model performance comparison")
fig.show()

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
