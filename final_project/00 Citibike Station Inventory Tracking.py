# Databricks notebook source
from datetime import datetime as dt
from datetime import timedelta
import json

dbutils.widgets.removeAll()

dbutils.widgets.text('01.start_date', "2021-10-01")
dbutils.widgets.text('02.end_date', "2023-03-01")
dbutils.widgets.text('03.hours_to_forecast', '4')
dbutils.widgets.text('04.station_to_model', 'W 21 St & 6 Ave')


start_date = str(dbutils.widgets.get('01.start_date'))
end_date = str(dbutils.widgets.get('02.end_date'))
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
station_to_model = dbutils.widgets.get('04.station_to_model')
print(start_date,end_date,hours_to_forecast,station_to_model)

# COMMAND ----------

# DBTITLE 1,ETL
# Run the Data Prepartion
result = dbutils.notebook.run("01 etl", 3600, {"01.start_date":start_date, "02.end_date":end_date,"03.hours_to_forecast":hours_to_forecast,"04.station_to_model":station_to_model})

# Check the results
assert json.loads(result)["exit_code"] == "OK", "Data Preparation Failed!" # Check to see that it worked

# COMMAND ----------

# DBTITLE 1,EDA
# Run the Data Prepartion
result = dbutils.notebook.run("02 eda", 3600, {"01.start_date":start_date, "02.end_date":end_date,"03.hours_to_forecast":hours_to_forecast,"04.station_to_model":station_to_model})

# Check the results
assert json.loads(result)["exit_code"] == "OK", "Data Preparation Failed!" # Check to see that it worked

# COMMAND ----------

# DBTITLE 1,Model Development
# Run the Data Prepartion
result = dbutils.notebook.run("03 mdl", 3600, {"01.start_date":start_date, "02.end_date":end_date,"03.hours_to_forecast":hours_to_forecast,"04.station_to_model":station_to_model})

# Check the results
assert json.loads(result)["exit_code"] == "OK", "Data Preparation Failed!" # Check to see that it worked

# COMMAND ----------

# DBTITLE 1,Inventory Prediction
# Run the Data Prepartion
result = dbutils.notebook.run("04 app", 3600, {"01.start_date":start_date, "02.end_date":end_date,"03.hours_to_forecast":hours_to_forecast,"04.station_to_model":station_to_model})

# Check the results
assert json.loads(result)["exit_code"] == "OK", "Data Preparation Failed!" # Check to see that it worked
