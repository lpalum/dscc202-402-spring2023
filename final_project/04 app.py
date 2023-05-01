# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

# DBTITLE 0,YOUR APPLICATIONS CODE HERE...
start_date = str(dbutils.widgets.get('01.start_date'))
end_date = str(dbutils.widgets.get('02.end_date'))
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

print(start_date,end_date,hours_to_forecast, promote_model)


# COMMAND ----------

pip install folium

# COMMAND ----------

import folium
from pyspark.sql.functions import *

#Get and display current timestamp

df = spark.sql("select to_utc_timestamp(current_timestamp(), '+04:00')")
currentTS = df.collect()[0][0]
displayHTML("<h2>Current Timestamp: </h2>")
displayHTML(currentTS)

# Create map of station location and header for map

displayHTML("<br><br><h2>Station: 8 Ave & W 33 St</h2>")
map=folium.Map(location=[40.751551,-73.993934], zoom_start=17, min_zoom=17, max_zoom=17)
map.add_child(folium.Marker(location=[40.751551,-73.993934],popup='8 Ave & W 33 St',icon=folium.Icon(color='red')))
map

# COMMAND ----------

|import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))

# COMMAND ----------


