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
# comment

# COMMAND ----------

#Initializing stream for historic trip data 
spark.sql("set spark.sql.streaming.schemaInference=true")
historic_trip_data_df = (spark.readStream
                         .option("header", True)
                         .csv(BIKE_TRIP_DATA_PATH))
display(historic_trip_data_df)

# COMMAND ----------

#Filters the historic trip data to contain only our assigned station data
historic_trip_df = historic_trip_data_df.filter("start_station_name == 'Cleveland Pl & Spring St'")
display(historic_trip_df)

# COMMAND ----------

#This command writes the stream for the historic trip data in order to read it in the EDA notebook
historic_trip_checkpoint_path = f"dbfs:/FileStore/tables/G11/historic_trip_data_bronze"
historic_trip_output_path = f"dbfs:/FileStore/tables/G11/historic_trip_data_bronze"
historic_trip_query = (historic_trip_df.writeStream
                      .outputMode("append")
                      .format("delta")
                      .queryName("historic_trip")
                      .option("checkpointLocation", historic_trip_checkpoint_path)
                      .start(historic_trip_output_path))

# COMMAND ----------

#Initializing stream for bronze station status table
#STATION_ID = "66db2fd0-0aca-11e7-82f6-3863bb44ef7c"
bronze_station_status_df = (spark.readStream
                           .format("delta")
                           .load(BRONZE_STATION_STATUS_PATH))
bronze_station_status_df.display()

# COMMAND ----------

#Filtering to have only our station
bronze_station_status_df = bronze_station_status_df.filter("station_id == '66db2fd0-0aca-11e7-82f6-3863bb44ef7c'")
bronze_station_status_df.display()

# COMMAND ----------

#Writing stream for bronze station status
bronze_station_status_path = f"dbfs:/FileStore/tables/G11/bronze_station_status"
bronze_station_status_checkpoint_path = f"dbfs:/FileStore/tables/G11/bronze_station_status"
bronze_station_status_query = (bronze_station_status_df.writeStream
                              .outputMode("append")
                              .format("delta")
                              .queryName("bronze_station_status")
                              .option("checkpointLocation", bronze_station_status_checkpoint_path)
                              .start(bronze_station_status_path))

# COMMAND ----------

bronze_station_info_df = (spark.readStream
                           .format("delta")
                           .load(BRONZE_STATION_INFO_PATH))
bronze_station_info_df.display()

# COMMAND ----------

bronze_station_info_df = bronze_station_info_df.filter("short_name == '5492.05'")
bronze_station_info_df.display()

# COMMAND ----------

bronze_station_info_path = f"dbfs:/FileStore/tables/G11/bronze_station_info"
bronze_station_info_checkpoint_path = f"dbfs:/FileStore/tables/G11/bronze_station_info"
bronze_station_info_query = (bronze_station_info_df.writeStream
                            .outputMode("append")
                            .format("delta")
                            .queryName("bronze_station_info")
                            .option("checkpointLocation", bronze_station_info_checkpoint_path)
                            .start(bronze_station_info_path))

# COMMAND ----------

#Read in historic weather
#Lat = 40.722103786686034
historic_weather_df = (spark.read
                      .option("header", True)
                      .csv(NYC_WEATHER_FILE_PATH))
historic_weather_df.display()


# COMMAND ----------

#Read stream for bronze weather table
bronze_nyc_weather_df = (spark.readStream
                        .format("delta")
                        .load(BRONZE_NYC_WEATHER_PATH))
bronze_nyc_weather_df.display()

# COMMAND ----------

dbfs:/FileStore/tables/G11/bronze_station_infodbutils.fs.mkdirs("dbfs:/FileStore/tables/G11/bronze_station_info")

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/G11/

# COMMAND ----------


