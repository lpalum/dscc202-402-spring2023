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
ARTIFACT_PATH = "G10_model"
logging.getLogger("py4j").setLevel(logging.ERROR)

# COMMAND ----------

# DBTITLE 1,Load Dataset
df = spark.sql('select * from train_bike_weather_netChange_s').toPandas()
df

# COMMAND ----------

# Display a summary table for all features
df.describe()

# COMMAND ----------

# DBTITLE 1,Data Preprocessing
# Rename the timestamp column to 'ds' and the target column to 'y'
df = df.rename(columns={'ts': 'ds'}).rename(columns={'net_change': 'y'})

# Change str to datetime
df['ds'] = df['ds'].apply(pd.to_datetime)

# Fill missing values of 'feel_like' and 'rain_1h' with mean value
df["feels_like"].fillna(df["feels_like"].mean(), inplace=True)
df["rain_1h"].fillna(df["rain_1h"].mean(), inplace=True)

# perform one-hot encoding on the 'description' column
one_hot_df = pd.get_dummies(df['description'])
df = pd.concat([df, one_hot_df], axis=1)
df.drop(columns='description', inplace=True)

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

# DBTITLE 1,Create a Baseline Model
# Create a Prophet model with all features as covariates
baseline_model = Prophet()
for feature in df.columns:
    if feature != 'ds' and feature != 'y':
        baseline_model.add_regressor(feature)

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
print(f"MDAPE of baseline model: {baseline_model_p['mdape'].values[0]}")

# COMMAND ----------

# DBTITLE 1,Hyperparameter Tuning
# Set up parameter grid
param_grid = {  
    'changepoint_prior_scale': [0.001],   # [0.001, 0.01, 0.1, 0.5]
    'seasonality_prior_scale': [0.01],    # [0.01, 0.1, 1.0, 10.0]
    'seasonality_mode': ['additive', 'multiplicative']
}

# Generate all combinations of parameters
all_params = [dict(zip(param_grid.keys(), v)) for v in itertools.product(*param_grid.values())]

print(f"Total training runs {len(all_params)}")

# Helper routine to extract the parameters that were used to train a specific instance of the model
def extract_params(pr_model):
    return {attr: getattr(pr_model, attr) for attr in serialize.SIMPLE_ATTRIBUTES}

# Create a list to store MDAPE values for each combination
mdapes = []

# Use cross validation to evaluate all parameters
for params in all_params:
    with mlflow.start_run():
        # Fit a model using one parameter combination + holidays
        m = Prophet(**params) 
        #m.add_country_holidays(country_name='US')
        m.fit(df)

        # Cross-validation
        df_cv = cross_validation(model=m, initial=initial_days, period=period_days, horizon=horizon_days, parallel="threads")
        # Model performance
        df_p = performance_metrics(df_cv, rolling_window=1)

        metric_keys = ["mse", "rmse", "mae", "mdape", "smape", "coverage"]
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
        mdapes.append((df_p['mdape'].values[0], model_uri))
        

# COMMAND ----------

# Tuning results
tuning_results = pd.DataFrame(all_params)
tuning_results['mdape'] = list(zip(*mdapes))[0]
tuning_results['model']= list(zip(*mdapes))[1]
best_params = dict(tuning_results.iloc[tuning_results[['mdape']].idxmin().values[0]])
print(json.dumps(best_params, indent=2))

# COMMAND ----------

# DBTITLE 1,Make Predictions
# Predict on the future
loaded_model = mlflow.prophet.load_model(best_params['model'])
forecast = loaded_model.predict(loaded_model.make_future_dataframe(hours_to_forecast, freq="H"))
forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]

# COMMAND ----------

prophet_plot = loaded_model.plot(forecast)

# COMMAND ----------

prophet_plot2 = loaded_model.plot_components(forecast)

# COMMAND ----------

# DBTITLE 1,Create a Residual Plot
# Plot the residuals
results = forecast[['ds','yhat']].join(df, lsuffix='_caller', rsuffix='_other')
results['residual'] = results['yhat'] - results['y']
fig = px.scatter(results, x='yhat', y='residual', marginal_y='violin', trendline='ols')
fig.show()

# COMMAND ----------

# DBTITLE 1,Register the Best Model and Move it into Staging
model_details = mlflow.register_model(model_uri=best_params['model'], name=ARTIFACT_PATH)
client = MlflowClient()
client.transition_model_version_stage(name=model_details.name, version=model_details.version, stage='Staging')

# COMMAND ----------

model_version_details = client.get_model_version(name=model_details.name, version=model_details.version)
print("The current model stage is: '{stage}'".format(stage=model_version_details.current_stage))

# COMMAND ----------

latest_version_info = client.get_latest_versions(ARTIFACT_PATH, stages=["Staging"])
latest_staging_version = latest_version_info[0].version
print("The latest staging version of the model '%s' is '%s'." % (ARTIFACT_PATH, latest_staging_version))

# COMMAND ----------

model_staging_uri = "models:/{model_name}/staging".format(model_name=ARTIFACT_PATH)
print("Loading registered model version from URI: '{model_uri}'".format(model_uri=model_staging_uri))
model_staging = mlflow.prophet.load_model(model_staging_uri)

# COMMAND ----------

model_staging.plot(model_staging.predict(model_staging.make_future_dataframe(hours_to_forecast, freq="H")))

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
