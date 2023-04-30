# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

start_date = str(dbutils.widgets.get('01.start_date'))
end_date = str(dbutils.widgets.get('02.end_date'))
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

print(start_date,end_date,hours_to_forecast, promote_model)
print("YOUR CODE HERE...")

# COMMAND ----------

import mlflow
import json
import pandas as pd
import numpy as np
from prophet import Prophet, serialize
from prophet.diagnostics import cross_validation, performance_metrics
import seaborn as sns
import matplotlib.pyplot as plt
import itertools

ARTIFACT_PATH = "G10_model"
np.random.seed(12345)

# COMMAND ----------

# DBTITLE 1,Load Dataset
df = spark.sql('select * from train_bike_weather_netChange_s').toPandas()
display(df)
print(f"{len(df)} hours of data loaded ({round(len(df)/24,2)} days)")

# Visualize data using seaborn
sns.set(rc={'figure.figsize':(12,8)})
sns.lineplot(x=df.index, y=df["net_change"])
plt.show()

# COMMAND ----------

# DBTITLE 1,Create a Baseline Model
# rename the timestamp column to "ds" and the target column to "y"
df = df.rename(columns={'ts': 'ds'})
df = df.rename(columns={'net_change': 'y'})

# create a Prophet model with all features as covariates
baseline_model = Prophet()
for feature in df.columns:
    if feature != 'ds' and feature != 'y':
        baseline_model.add_regressor(feature)

# Fit the model on the training dataset
baseline_model.fit(df)

# specify time intervals in hours
initial_hours = 24*7  # 7 days
period_hours = 24*30  # 30 days
horizon_hours = 24*90  # 90 days

# perform cross-validation
initial_timedelta = pd.Timedelta(hours=initial_hours)
period_timedelta = pd.Timedelta(hours=period_hours)
horizon_timedelta = pd.Timedelta(hours=horizon_hours)

# Cross validation
baseline_model_cv = cross_validation(model=baseline_model, initial=initial_timedelta, period=period_timedelta, horizon=horizon_timedelta, parallel="threads")
baseline_model_cv.head()

# Model performance metrics
baseline_model_p = performance_metrics(baseline_model_cv, rolling_window=1)
baseline_model_p.head()

# Get the performance value
print(f"MAPE of baseline model: {baseline_model_p['mape'].values[0]}")

# COMMAND ----------

# DBTITLE 1,Hyperparameter Tuning
# Set up parameter grid
param_grid = {  
    'changepoint_prior_scale': [0.001],  # , 0.05, 0.08, 0.5
    'seasonality_prior_scale': [0.01],  # , 1, 5, 10, 12
    'seasonality_mode': ['additive', 'multiplicative']
}

# Generate all combinations of parameters
all_params = [dict(zip(param_grid.keys(), v)) for v in itertools.product(*param_grid.values())]

print(f"Total training runs {len(all_params)}")

# Helper routine to extract the parameters that were used to train a specific instance of the model
def extract_params(pr_model):
    return {attr: getattr(pr_model, attr) for attr in serialize.SIMPLE_ATTRIBUTES}

# Create a list to store MAPE values for each combination
mapes = []

# Use cross validation to evaluate all parameters
for params in all_params:
    with mlflow.start_run():
        # Fit a model using one parameter combination + holidays
        m = Prophet(**params) 
        holidays = pd.DataFrame({"ds": [], "holiday": []})
        m.add_country_holidays(country_name='US')
        m.fit(df) 

        # Cross-validation
        df_cv = cross_validation(model=m, initial='710 days', period='180 days', horizon='365 days', parallel="threads")
        # Model performance
        df_p = performance_metrics(df_cv, rolling_window=1)

        metric_keys = ["mse", "rmse", "mae", "mape", "mdape", "smape", "coverage"]
        metrics = {k: df_p[k].mean() for k in metric_keys}
        params = extract_params(m)

        print(f"Logged Metrics: \n{json.dumps(metrics, indent=2)}")
        print(f"Logged Params: \n{json.dumps(params, indent=2)}")

        mlflow.prophet.log_model(m, artifact_path=ARTIFACT_PATH)
        mlflow.log_params(params)
        mlflow.log_metrics(metrics)
        model_uri = mlflow.get_artifact_uri(ARTIFACT_PATH)
        print(f"Model artifact logged to: {model_uri}")

        # Save model performance metrics for this combination of hyper parameters
        mapes.append((df_p['mape'].values[0],model_uri))
        

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
