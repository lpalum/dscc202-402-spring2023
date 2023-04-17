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

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))

# COMMAND ----------

#Reading stream for historic trip data bronze
historic_trip_data_df = (spark.read
     .format("delta")
     .load("dbfs:/FileStore/tables/G11/historic_trip_data_bronze"))
historic_trip_data_df.display()
historic_trip_data_df.printSchema()
#here we have a bar chart displaying the end spots of the bikes and which spots are most common
#It is also grouped by if its a member or a casual user

# COMMAND ----------

historic_weather_df = (spark.read
                      .option("header", True)
                      .csv(NYC_WEATHER_FILE_PATH))
historic_weather_df.display()

# COMMAND ----------

#Counts how many disctinct descriptions of the weather
print("Distinct Count: " + str(historic_weather_df.select("description").distinct().count()))

# COMMAND ----------

#Displays the basic summary of the dataframe this can be used to make decisions
historic_trip_data_df.describe().show()

# COMMAND ----------

#Displays the basic summary of the dataframe this can be used to make decisions
historic_weather_df.describe().show()

# COMMAND ----------

from pyspark.sql.functions import *
df = (historic_trip_data_df.withColumn("month", month("started_at")))
df1 = (df.select("month", "rideable_type"))
df1.display()


# COMMAND ----------

df.groupBy(df1.month).count().orderBy(df.month).show()

# COMMAND ----------



# COMMAND ----------

()()bronze_station_status_df = (spark.readStream
                           .format("delta")
                           .load("dbfs:/FileStore/tables/G11/bronze_station_status"))
bronze_station_status_df.display()

# COMMAND ----------

bronze_station_info_df = (spark.readStream
                         .format("delta")
                         .load("dbfs:/FileStore/tables/G11/bronze_station_info"))
bronze_station_info_df.display()

# COMMAND ----------



# COMMAND ----------



display(df)

