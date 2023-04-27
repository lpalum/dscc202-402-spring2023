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
#dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
# comment

# COMMAND ----------

#Initializing stream for historic trip data 
from pyspark.sql.types import LongType, StringType, StructType, StructField, TimestampType, DoubleType
historic_trip_data_schema = StructType([
    StructField("ride_id", StringType(), False),
    StructField("rideable_type", StringType(), True),
    StructField("started_at", TimestampType(), True),
    StructField("ended_at", TimestampType(), True),
    StructField("start_station_name", StringType(), True),
    StructField("start_station_id", DoubleType(), True),
    StructField("end_station_name", StringType(), True),
    StructField("end_station_id", DoubleType(), True),
    StructField("start_lat", DoubleType(), True),
    StructField("start_lng", DoubleType(), True),
    StructField("end_lat", DoubleType(), True),
    StructField("end_lng", DoubleType(), True),
    StructField("member_casual", StringType(), True)
])
historic_trip_data_df = (spark.readStream
                         .option("header", True)
                         .schema(historic_trip_data_schema)
                         .csv(BIKE_TRIP_DATA_PATH))
#display(historic_trip_data_df)

# COMMAND ----------

#Filters the historic trip data to contain only our assigned station data
from pyspark.sql.functions import col
historic_trip_df = historic_trip_data_df.filter((col("start_station_name") == GROUP_STATION_ASSIGNMENT)
                                      | (col("end_station_name") == GROUP_STATION_ASSIGNMENT))
#display(historic_trip_df)

# COMMAND ----------

#This command writes the stream for the historic trip data in order to read it in the EDA notebook

historic_trip_checkpoint_path = f"dbfs:/FileStore/tables/G11/bronze/historic_trip_data/.checkpoint"
historic_trip_output_path = f"dbfs:/FileStore/tables/G11/bronze/historic_trip_data/"
historic_trip_query = (historic_trip_df.writeStream
                      .outputMode("append")
                      .format("delta")
                      .queryName("historic_trip")
                      .option("checkpointLocation", historic_trip_checkpoint_path)
                      .trigger(availableNow=True)
                      .start(historic_trip_output_path))


# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE 'dbfs:/FileStore/tables/G11/bronze/historic_trip_data'

# COMMAND ----------

#Reading bronze station status tables
#STATION_ID = "66db2fd0-0aca-11e7-82f6-3863bb44ef7c"

bronze_station_status_df = (spark.read
                           .format("delta")
                           .load(BRONZE_STATION_STATUS_PATH))
bronze_station_status_df.display()

# COMMAND ----------

#Filtering to have only our station
bronze_station_status_df = bronze_station_status_df.filter("station_id == '66db2fd0-0aca-11e7-82f6-3863bb44ef7c'")
bronze_station_status_df.display()

# COMMAND ----------

#Writing bronze station status

bronze_station_status_path = f"dbfs:/FileStore/tables/G11/bronze/station_status/"
bronze_station_status_query = (bronze_station_status_df.write
                              .format("delta")
                              .mode("overwrite")
                              .save(bronze_station_status_path))

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE 'dbfs:/FileStore/tables/G11/bronze/station_status'
# MAGIC ZORDER BY num_docks_available

# COMMAND ----------

bronze_station_info_df = (spark.read
                           .format("delta")
                           .load(BRONZE_STATION_INFO_PATH))
bronze_station_info_df.display()

# COMMAND ----------

bronze_station_info_df = bronze_station_info_df.filter("short_name == '5492.05'")
bronze_station_info_df.display()

# COMMAND ----------

bronze_station_info_path = f"dbfs:/FileStore/tables/G11/bronze/station_info"
bronze_station_info_query = (bronze_station_info_df.write
                            .format("delta")
                            .mode("overwrite")
                            .save(bronze_station_info_path))

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE 'dbfs:/FileStore/tables/G11/bronze/station_info'

# COMMAND ----------

#Read in historic weather
from pyspark.sql.types import LongType, StringType, StructType, StructField, TimestampType, DoubleType, IntegerType
spark.sql("set spark.sql.streaming.schemaInference=true")
historic_weather_schema = StructType([
    StructField("dt", LongType(), True),
    StructField("temp", DoubleType(), True),
    StructField("feels_like", DoubleType(), True),
    StructField("pressure", IntegerType(), True),
    StructField("humidity", IntegerType(), True),
    StructField("dew_point", DoubleType(), True),
    StructField("uvi", DoubleType(), True),
    StructField("clouds", IntegerType(), True),
    StructField("visibility", IntegerType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("wind_deg", IntegerType(), True),
    StructField("pop", DoubleType(), True),
    StructField("snow_1h", DoubleType(), True),
    StructField("id", IntegerType(), True),
    StructField("main", StringType(), True),
    StructField("description", StringType(), True),
    StructField("icon", StringType(), True),
    StructField("loc", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("timezone", StringType(), True),
    StructField("timezone_offset", IntegerType(), True),
    StructField("rain_1h", DoubleType(), True)
])
historic_weather_df = (spark.readStream
                      .option("header", True)
                      .schema(historic_weather_schema)
                      .csv(NYC_WEATHER_FILE_PATH))
historic_weather_df.display()

# COMMAND ----------

#This command writes the stream for the historic weather data in order to read it in the EDA notebook

historic_weather_data_path = f"dbfs:/FileStore/tables/G11/bronze/historic_weather_data/"
historic_weather_checkpoint_path = f"dbfs:/FileStore/tables/G11/bronze/historic_weather_data/.checkpoint"
historic_weather_query = (historic_weather_df.writeStream
                         .outputMode("append")
                         .format("delta")
                         .queryName("historic_weather")
                         .option("checkpointLocation", historic_weather_checkpoint_path)
                         .trigger(availableNow=True)
                         .start(historic_weather_data_path))

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE 'dbfs:/FileStore/tables/G11/bronze/historic_weather_data'

# COMMAND ----------

#Read bronze weather table
bronze_nyc_weather_df = (spark.read
                        .format("delta")
                        .load(BRONZE_NYC_WEATHER_PATH))
bronze_nyc_weather_df.display()

# COMMAND ----------

from pyspark.sql.functions import *
weather_exploded_df = (bronze_nyc_weather_df.withColumn("weather", explode(col("weather"))))
df = (weather_exploded_df.withColumn("description", col("weather.description"))
     .withColumn("icon", col("weather.icon"))
     .withColumn("id", col("weather.id"))
     .withColumn("main", col("weather.main")))
df = df.drop("weather")
display(df)

# COMMAND ----------

bronze_weather_path = f"dbfs:/FileStore/tables/G11/bronze/weather"
bronze_station_info_query = (df.write
                            .format("delta")
                            .mode("overwrite")
                            .save(bronze_weather_path))

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE 'dbfs:/FileStore/tables/G11/bronze/weather'

# COMMAND ----------

#Silver table for historic trip data
historic_trip_silver = (spark.read
    .format("delta")
    .load(historic_trip_output_path))

historic_trip_silver = historic_trip_silver.select(
    'started_at',
    'ended_at',
    'start_station_name',
    'end_station_name'
)
historic_trip_silver = historic_trip_silver.withColumn("started_at", date_format(col("started_at"), "yyyy-MM-dd HH:mm:ss"))
historic_trip_silver = historic_trip_silver.withColumn("ended_at", date_format(col("started_at"), "yyyy-MM-dd HH:mm:ss"))

# COMMAND ----------

historic_trip_silver.display()

# COMMAND ----------

silver_historic_trip_path = f"dbfs:/FileStore/tables/G11/silver/historic_trip_data/"
silver_historic_trip_query = (historic_trip_silver.write
    .format("delta")
    .mode("overwrite")
    .save(silver_historic_trip_path))

# COMMAND ----------

silver_historic_weather = (spark.read
    .format("delta")
    .load(historic_weather_data_path))

silver_historic_weather = (silver_historic_weather.withColumn("dt", date_format(from_unixtime(col("dt").cast("long")), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("temp", round((col("temp") - 273.15), 2))
    .withColumn("feels_like", round((col("feels_like") - 273.15), 2))
)

silver_historic_weather = silver_historic_weather.select(
    'dt',
    'temp',
    'feels_like',
    'snow_1h',
    'main',
    'rain_1h'
)

# COMMAND ----------

silver_historic_weather.display()

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/tables/G11/silver/historic_trip_data/")

# COMMAND ----------

#%fs rm -r dbfs:/FileStore/tables/G11/silver/historic_trip_data/

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/G11/silver/

# COMMAND ----------


