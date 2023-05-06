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

from prophet.diagnostics import cross_validation, performance_metrics
from mlflow.tracking.client import MlflowClient
from sklearn.preprocessing import LabelEncoder
from prophet import Prophet, serialize
import matplotlib.pyplot as plt
from datetime import datetime
import plotly.express as px
import seaborn as sns
import pandas as pd
import numpy as np
import itertools
import logging
import mlflow
np.random.seed(12345)
logging.getLogger("py4j").setLevel(logging.ERROR)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Dataset

# COMMAND ----------

# Load Dataset from silver table
df = spark.sql('select * from train_bike_weather_netChange_s').toPandas()
df

# COMMAND ----------

# Display a summary table for all features
df.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Preprocessing

# COMMAND ----------

# Rename the timestamp column to 'ds' and the target column to 'y'
df = df.rename(columns={'ts': 'ds'}).rename(columns={'net_change': 'y'})

# Change str to datetime
df['ds'] = df['ds'].apply(pd.to_datetime)

# Fill missing values of 'feel_like' and 'rain_1h' with mean value
df["feels_like"].fillna(df["feels_like"].mean(), inplace=True)
df["rain_1h"].fillna(df["rain_1h"].mean(), inplace=True)

# Create a LabelEncoder instance and apply it to the 'description' column
df['description'] = LabelEncoder().fit_transform(df['description'])

# Replace 'false' with 0 and 'true' with 1 in the 'holiday' column
df['holiday'] = df['holiday'].replace({'false': 0, 'true': 1})

df

# COMMAND ----------

# Visualize data using seaborn
sns.set(rc={'figure.figsize':(16, 8)})
sns.lineplot(x=df['ds'], y=df['y'])
plt.legend(['net_change'])
plt.show()

# COMMAND ----------

# Helper routine to extract the parameters that are used to train a specific instance of the model
def extract_params(pr_model):
    params = {attr: getattr(pr_model, attr) for attr in serialize.SIMPLE_ATTRIBUTES}
    del_list = ['Juneteenth National Independence Day (Observed)', "New Year's Day (Observed)", 'Independence Day (Observed)', 'Christmas Day (Observed)', 'Veterans Day (Observed)', 'Juneteenth National Independence Day', 'holiday']
    for i in del_list:
        if i in params['component_modes']['additive']:
            params['component_modes']['additive'].remove(i)
        if i in params['component_modes']['multiplicative']:
            params['component_modes']['multiplicative'].remove(i)
    return params

# COMMAND ----------

# MAGIC %md
# MAGIC ### Register a Baseline Model as Production if Not Exists

# COMMAND ----------

# Build a Baseline Model only if it doesn't exist
client = MlflowClient()
try:
    client.get_latest_versions(GROUP_MODEL_NAME, stages=["Production"])
except:
    # Create a Prophet model with all features as covariates + holidays
    baseline_model = Prophet()
    for feature in df.columns:
        if feature != 'ds' and feature != 'y':
            baseline_model.add_regressor(feature)
    baseline_model.add_country_holidays(country_name='US')

    # Fit the model on the training dataset
    baseline_model.fit(df)

    # Cross validation
    date_diff = (datetime.strptime(end_date, "%Y-%m-%d") - datetime.strptime(start_date, "%Y-%m-%d")).days
    initial_days = str(int(date_diff / 4)) + ' days'
    period_days = str(int(date_diff / 16)) + ' days'
    horizon_days = str(int(date_diff / 8)) + ' days'
    baseline_model_cv = cross_validation(model=baseline_model, initial=initial_days, period=period_days, horizon=horizon_days, parallel="threads")

    # Model performance metrics
    baseline_model_p = performance_metrics(baseline_model_cv, rolling_window=1)

    # Get the performance value
    print(f"MAE of baseline model: {baseline_model_p['mae'].values[0]}")

    # Log the baseline model
    with mlflow.start_run():
        metric_keys = ["mse", "rmse", "mae", "mdape", "smape", "coverage"]
        metrics = {k: baseline_model_p[k].mean() for k in metric_keys}
        params = extract_params(baseline_model)

        mlflow.prophet.log_model(baseline_model, artifact_path=GROUP_MODEL_NAME)
        mlflow.log_params(params)
        mlflow.log_metrics(metrics)

        model_uri = mlflow.get_artifact_uri(GROUP_MODEL_NAME)
        baseline_params = {'mae': metrics['mae'], 'model': model_uri}
        print(json.dumps(baseline_params, indent=2))

    # Register the baseline model and move it to production
    model_details = mlflow.register_model(model_uri=baseline_params['model'], name=GROUP_MODEL_NAME)
    client.transition_model_version_stage(name=model_details.name, version=model_details.version, stage="Production")
    
    model_version_details = client.get_model_version(name=model_details.name, version=model_details.version)
    print(f"The current version of the model is: {model_version_details.version}. The stage is: '{model_version_details.current_stage}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hyperparameter Tuning

# COMMAND ----------

# Set up parameter grid
param_grid = {  
    'changepoint_prior_scale': [0.001],   # [0.001, 0.01, 0.1, 0.5]
    'seasonality_prior_scale': [0.01],    # [0.01, 0.1, 1.0, 10.0]
    'seasonality_mode': ['additive', 'multiplicative']
}

# Generate all combinations of parameters
all_params = [dict(zip(param_grid.keys(), v)) for v in itertools.product(*param_grid.values())]

print(f"Total training runs {len(all_params)}")

# Create a list to store MAE values for each combination
maes = []

# Use cross validation to evaluate all parameters
for params in all_params:
    with mlflow.start_run():
        # Fit a model with all features as covariates + holidays
        m = Prophet(**params)
        for feature in df.columns:
            if feature != 'ds' and feature != 'y':
                m.add_regressor(feature)
        m.add_country_holidays(country_name='US')
        m.fit(df)

        # Cross-validation
        date_diff = (datetime.strptime(end_date, "%Y-%m-%d") - datetime.strptime(start_date, "%Y-%m-%d")).days
        initial_days = str(int(date_diff / 4)) + ' days'
        period_days = str(int(date_diff / 16)) + ' days'
        horizon_days = str(int(date_diff / 8)) + ' days'
        df_cv = cross_validation(model=m, initial=initial_days, period=period_days, horizon=horizon_days, parallel="threads")
        # Model performance
        df_p = performance_metrics(df_cv, rolling_window=1)

        metric_keys = ["mse", "rmse", "mae", "mdape", "smape", "coverage"]
        metrics = {k: df_p[k].mean() for k in metric_keys}
        params = extract_params(m)
        print(f"Logged Metrics: \n{json.dumps(metrics, indent=2)}")
        print(f"Logged Params: \n{json.dumps(params, indent=2)}")

        mlflow.prophet.log_model(m, artifact_path=GROUP_MODEL_NAME)
        mlflow.log_params(params)
        mlflow.log_metrics(metrics)
        model_uri = mlflow.get_artifact_uri(GROUP_MODEL_NAME)
        print(f"Model artifact logged to: {model_uri}")

        # Save model performance metrics for this combination of hyper parameters
        maes.append((df_p['mae'].values[0], model_uri))

# COMMAND ----------

# Tuning results
tuning_results = pd.DataFrame(all_params)
tuning_results['mae'] = list(zip(*maes))[0]
tuning_results['model']= list(zip(*maes))[1]
best_params = dict(tuning_results.iloc[tuning_results[['mae']].idxmin().values[0]])
print(json.dumps(best_params, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Register the Best Model and Move it into Staging

# COMMAND ----------

# Register the best model
model_details = mlflow.register_model(model_uri=best_params['model'], name=GROUP_MODEL_NAME)

# Move the model into staging
client.transition_model_version_stage(name=GROUP_MODEL_NAME, version=model_details.version, stage='Staging')

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
