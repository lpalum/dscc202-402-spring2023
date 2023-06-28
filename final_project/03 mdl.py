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



from mlflow.tracking.client import MlflowClient

# Visualization
import seaborn as sns
import matplotlib.pyplot as plt
sns.set(color_codes=True)

# Hyperparameter tuning
import itertools

SOURCE_DATA = f"dbfs:/FileStore/tables/G11/silver/inventory"
ARTIFACT_PATH = GROUP_MODEL_NAME
np.random.seed(12345)

data = (spark.read
    .format("delta")
    .load(SOURCE_DATA))

## Helper routine to extract the parameters that were used to train a specific instance of the model
def extract_params(pr_model):
    return {attr: getattr(pr_model, attr) for attr in serialize.SIMPLE_ATTRIBUTES}



# COMMAND ----------

df = data.toPandas()
df = df.rename(columns={'dt':'ds', 'net_hour_change':'y'})
print(df)

# COMMAND ----------

#train test split
train_data = df.sample(frac=0.8, random_state=42)
test_data = df.drop(train_data.index)
x_train, y_train, x_test, y_test = train_data["ds"], train_data["y"], test_data["ds"], test_data["y"]

# COMMAND ----------

import plotly.express as px
fig = px.line(df, x="ds", y="y", title='Net bike change')
fig.show()

# COMMAND ----------

from sklearn.metrics import mean_absolute_error
# Set up parameter grid
param_grid = {  
    'changepoint_prior_scale': [0.01, 0.005],
    'seasonality_prior_scale': [4, 8],
    'seasonality_mode': ['additive'],
    'yearly_seasonality' : [True],
    'weekly_seasonality': [True],
    'daily_seasonality': [True]
}

# Generate all combinations of parameters
all_params = [dict(zip(param_grid.keys(), v)) for v in itertools.product(*param_grid.values())]

print(f"Total training runs {len(all_params)}")

# Create a list to store MAPE values for each combination
maes = []

# Use cross validation to evaluate all parameters
for params in all_params:
    with mlflow.start_run(): 
        # Fit a model using one parameter combination + holidays
        m = Prophet(**params) 
        holidays = pd.DataFrame({"ds": [], "holiday": []})
        m.add_country_holidays(country_name='US')
        m.add_regressor('feels_like')
        m.add_regressor('rain_1h')
        m.add_regressor('temp')
        m.fit(train_data)

        y_pred = m.predict(test_data.dropna())

        mae = mean_absolute_error(y_test.dropna(), y_pred['yhat'])
        mlflow.prophet.log_model(m, artifact_path=ARTIFACT_PATH)
        mlflow.log_params(params)
        mlflow.log_metrics({'mae': mae})
        model_uri = mlflow.get_artifact_uri(ARTIFACT_PATH)
        print(f"Model artifact logged to: {model_uri}")

        # Save model performance metrics for this combination of hyper parameters
        maes.append((mae, model_uri))

# COMMAND ----------

# Tuning results
tuning_results = pd.DataFrame(all_params)
tuning_results['mae'] = list(zip(*maes))[0]
tuning_results['model']= list(zip(*maes))[1]
best_params = dict(tuning_results.iloc[tuning_results[['mae']].idxmin().values[0]])
best_params

# COMMAND ----------

loaded_model = mlflow.prophet.load_model(best_params['model'])
forecast = loaded_model.predict(test_data)
print(f"forecast:\n${forecast.tail(40)}")

# COMMAND ----------

prophet_plot = loaded_model.plot(forecast)

# COMMAND ----------

prophet_plot2 = loaded_model.plot_components(forecast)

# COMMAND ----------

# Finding residuals
test_data.ds = pd.to_datetime(test_data.ds)
forecast.ds = pd.to_datetime(forecast.ds)
results = forecast[['ds','yhat']].merge(test_data,on="ds")
results['residual'] = results['yhat'] - results['y']

# COMMAND ----------

# Plot the residuals

fig = px.scatter(
    results, x='yhat', y='residual',
    marginal_y='violin',
    trendline='ols'
)
fig.show()

# COMMAND ----------

# Register Model to MLFlow

model_details = mlflow.register_model(model_uri=best_params['model'], name=GROUP_MODEL_NAME)

# COMMAND ----------

client = MlflowClient()

# COMMAND ----------

if promote_model:
    client.transition_model_version_stage(
    name=model_details.name,
    version=model_details.version,
    stage='Production')
else:
    client.transition_model_version_stage(
    name=model_details.name,
    version=model_details.version,
    stage='Staging'
)

# COMMAND ----------

model_version_details = client.get_model_version(
    name=model_details.name,
    version=model_details.version
)
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

model_staging.plot(model_staging.predict(test_data))

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))

# COMMAND ----------



# COMMAND ----------


