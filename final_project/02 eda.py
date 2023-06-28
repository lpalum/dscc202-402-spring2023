# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

# DBTITLE 1,Organization of the Notebook
# MAGIC %md
# MAGIC The first half of this notebook is mostly pandas-profiling results showing us information about the individual variables. The second half are graphs and relationships between hours, days, weeks, months, years, and weather and their relationship with the number of trips that occur. 

# COMMAND ----------

pip install -U pandas-profiling

# COMMAND ----------

pip install holidays

# COMMAND ----------

# DBTITLE 1,Imports
from pathlib import Path
from pyspark.sql.functions import *

import matplotlib.pyplot as plt
import seaborn as sns

import numpy as np
import requests

import pandas_profiling
import pandas as pd
from pandas_profiling.utils.cache import cache_file

# COMMAND ----------


historic_trip_data_df = (spark.read
     .format("delta")
     .load("dbfs:/FileStore/tables/G11/bronze/historic_trip_data/"))
historic_trip_data_df.display()
historic_trip_data_df.printSchema()
historic_trip_data_df.count()


# COMMAND ----------


bronze_station_status_df = (spark.read
                           .format("delta")
                           .load("dbfs:/FileStore/tables/G11/bronze/station_status"))
bronze_station_status_df.display()


# COMMAND ----------

from pyspark.sql.functions import *
df2 = (historic_trip_data_df.withColumn("day", dayofyear("started_at")))
df3 = (df2.select("day", "rideable_type"))
df3.display()

# COMMAND ----------

from pyspark.sql.functions import *
import pandas as pd
import matplotlib.pyplot as plt

df2 = (historic_trip_data_df.withColumn("day", dayofyear("started_at")))
df3 = (df2.select("day", "rideable_type"))
df4 = df2.groupBy(df3.day).count().orderBy(df2.day)
df4.show(31)
#df4.select('count').hist(by=df4.select('day'))

# COMMAND ----------

import pandas_profiling
import pandas as pd
from pandas_profiling.utils.cache import cache_file
import numpy as np


historic_trip_data_df = historic_trip_data_df.select("*").toPandas()
historic_trip_data_df['started_at']= pd.to_datetime(historic_trip_data_df['started_at'])
historic_trip_data_df['ended_at']= pd.to_datetime(historic_trip_data_df['ended_at'])


historic_trip_data_profile = pandas_profiling.ProfileReport(historic_trip_data_df)
historic_trip_data_profile

# COMMAND ----------

bronze_station_info_df = (spark.read
                         .format("delta")
                         .load("dbfs:/FileStore/tables/G11/bronze/station_info"))
bronze_station_info_df.display()

# COMMAND ----------

#For historic_trip_data_df

import pandas_profiling
import pandas as pd
from pandas_profiling.utils.cache import cache_file

historic_trip_data_df = (spark.read.format("delta").load("dbfs:/FileStore/tables/G11/bronze/historic_trip_data"))

df = historic_trip_data_df.select("*").toPandas()


# COMMAND ----------

profile = pandas_profiling.ProfileReport(df)
profile

# COMMAND ----------

#For bronze_station_status_df

import pandas_profiling
import pandas as pd
from pandas_profiling.utils.cache import cache_file

bronze_station_status_df = (spark.read.format("delta").load("dbfs:/FileStore/tables/G11/bronze/station_status"))
df = bronze_station_status_df.select("*").toPandas()

# COMMAND ----------

profile = pandas_profiling.ProfileReport(df)
profile

# COMMAND ----------

# DBTITLE 1,bronze_station_status
import pandas_profiling
import pandas as pd
from pandas_profiling.utils.cache import cache_file

bronze_station_info_df = (spark.read.format("delta").load("dbfs:/FileStore/tables/G11/bronze/station_info"))

df = bronze_station_info_df.select("*").toPandas()


bronze_station_status = (spark.read.format("delta").load("dbfs:/FileStore/tables/G11/bronze_station_status"))
bronze_station_status.display()

# COMMAND ----------

bronze_station_status_df = bronze_station_status.select("*").toPandas()
bronze_station_status_profile = pandas_profiling.ProfileReport(bronze_station_status_df)
bronze_station_status_profile

# COMMAND ----------

# DBTITLE 1,bronze_station_info
bronze_station_info = (spark.read.format("delta").load("dbfs:/FileStore/tables/G11/bronze_station_info"))
bronze_station_info.display()


# COMMAND ----------

bronze_station_info_df = bronze_station_info.select("*").toPandas()
bronze_station_info_profile = pandas_profiling.ProfileReport(bronze_station_info_df)
bronze_station_info_profile

# COMMAND ----------


historic_weather_df = (spark.readStream.format("delta").load("dbfs:/FileStore/tables/G11/bronze/historic_weather_data"))
historic_weather_df.display()

historic_weather_df = (spark.read.format("delta").load("dbfs:/FileStore/tables/G11/bronze/historic_weather_data"))
historic_weather_df.display()
historic_weather_df.printSchema()


# COMMAND ----------

import pandas_profiling
import pandas as pd
from pandas_profiling.utils.cache import cache_file

historic_weather_data_df = (spark.read.format("delta").load("dbfs:/FileStore/tables/G11/bronze/historic_weather_data"))
df = historic_weather_data_df.select("*").toPandas()

# COMMAND ----------

#Reading stream for historic trip data bronze
historic_trip_data_df = (spark.read
     .format("delta")
     .load("dbfs:/FileStore/tables/G11/bronze/historic_trip_data"))
historic_trip_data_df.display()
historic_trip_data_df.printSchema()
#here we have a bar chart displaying the end spots of the bikes and which spots are most common
#It is also grouped by if its a member or a casual user

# COMMAND ----------

# DBTITLE 1,historic_weather
#historic_weather_df = (spark.read
#                      .option("header", True)
#                      .csv("dbfs:/FileStore/tables/G11/historic_weather_df"))
#historic_weather_df.display()

dbfs:"/FileStore/tables/G11/"

#historic_weather = (spark.read.format("delta").load("dbfs:/FileStore/tables/G11/historic_weather_df"))
#historic_weather.display()


# COMMAND ----------

#Counts how many disctinct descriptions of the weather
#print("Distinct Count: " + str(historic_weather.select("description").distinct().count()))
#print("Distinct Count: " + str(historic_weather.select("description").distinct().count()))

# COMMAND ----------

#historic_weather_df = historic_weather.select("*").toPandas()
#historic_weather_profile = pandas_profiling.ProfileReport(historic_weather_df)
#historic_weather_profile

# COMMAND ----------



# COMMAND ----------


day2 = (historic_trip_data_df.withColumn("day", dayofyear("started_at")))
day3 = (day2.select("day", "rideable_type"))

day4 = day2.groupBy(day3.day).count().orderBy(day2.day)
day4.show(365)



# COMMAND ----------

# DBTITLE 1,Daily Yearly Trends
import matplotlib.pyplot as plt

counts=day4.select('count').toPandas()
days=day4.select('day').toPandas()

plt.figure(figsize=(25, 10))
scatterplt = plt.scatter(days,counts,color='red')
plot = plt.plot(days,counts)
plt.show()

# COMMAND ----------

# DBTITLE 1,Daily Yearly Trends and Holidays Explanations
# MAGIC %md
# MAGIC Based on the graph above a we see that there is a lot of variations in the amount of trips taken throughout a year. It seems that there are four big spikes. 
# MAGIC
# MAGIC There are three spikes on August 6th, 13th and, 20th. These three spikes may be due to either very nice weather on those three days, or there could have been a lot of events on those three days. 
# MAGIC There is also a larger/longer spike from Novermber 1st to November 13. This may be due to people trying to buy gifts before the holiday season. 
# MAGIC
# MAGIC There are also a few dips in trips in April, May, and December. This makes sense since in the spring there tends to be a lot of rain and, in Decmeber it gets very cold, windy, and snowy neither of which is ideal for bike trips. 
# MAGIC
# MAGIC In terms of how holidays impact the number of trips there seems to have some impact. Specifically we see a big drop in trips on Christmas. There also a dip in rides on Memorial Day. Both these holidays causing a dip in rides makes sense because most people don't work on those days and aren't going to be traveling on bikes on holidays. 

# COMMAND ----------

day2 = (historic_trip_data_df.withColumn("day", hour("started_at")))
day3 = (day2.select("day", "rideable_type"))

day4 = day2.groupBy(day3.day).count().orderBy(day2.day)
day4.show(24)

# COMMAND ----------

# DBTITLE 1,Hourly Trends
import matplotlib.pyplot as plt

counts=day4.select('count').toPandas()
days=day4.select('day').toPandas()

plt.figure(figsize=(25, 10))
scatterplt = plt.scatter(days,counts,color='red')
plot = plt.plot(days,counts)

plt.show()

# COMMAND ----------

# DBTITLE 1,Hourly Trends
# MAGIC %md
# MAGIC The hourly trends tell us that most rides occur during the afternoon. There is a gradual increase in riders throughout the day with it peaking at 5pm. This makes sense since most people get out of work in the afternoon. 

# COMMAND ----------

week2 = (historic_trip_data_df.withColumn("week", weekofyear("started_at")))
week3 = (week2.select("week", "rideable_type"))
week4 = week2.groupBy(week3.week).count().orderBy(week2.week)
week4.show(52)

# COMMAND ----------

# DBTITLE 1,Yearly Weekly Trends
import matplotlib.pyplot as plt
counts=week4.select('count').toPandas()
weeks=week4.select('week').toPandas()
plt.figure(figsize=(25, 10))
scatterplt = plt.scatter(weeks,counts,color='red')
plot = plt.plot(weeks,counts)

plt.show()

# COMMAND ----------

# DBTITLE 1,Yearly Weekly Trends Explanation
# MAGIC %md
# MAGIC The yearly weekly trends follows the same pattern as the daily yearly trends, but the graph is smoother. There is a two week peak in early Novmber and the lows in April, May, and December. We also see a strong deep in late September. 

# COMMAND ----------

week5 = (historic_trip_data_df.withColumn("week", dayofweek("started_at")))
week6 = (week5.select("week", "rideable_type"))
week7 = week5.groupBy(week6.week).count().orderBy(week5.week)
week7.show()

# COMMAND ----------

# DBTITLE 1,Weekly Trends
import matplotlib.pyplot as plt
counts=week7.select('count').toPandas()
weeks=week7.select('week').toPandas()
plt.figure(figsize=(25, 10))
scatterplt = plt.scatter(weeks,counts,color='red')
plot = plt.plot(weeks,counts)

plt.show()

# COMMAND ----------

# DBTITLE 1,Weekly Trends Explanation
# MAGIC %md
# MAGIC Based on the weekly trends that the most common days to ride bikes are Fridays, Saturdays, and Sundays. This makes sense because people tend to have the most time off on those days. Mondays likely have the lowest number of trips because people tend to be pretty low energy on Mondays. 

# COMMAND ----------

month2 = (historic_trip_data_df.withColumn("month", month("started_at")))
month3 = (month2.select("month", "rideable_type"))
month4 = month2.groupBy(month3.month).count().orderBy(month2.month)
month4.show()


# COMMAND ----------

import matplotlib.pyplot as plt
counts=month4.select('count').toPandas()
months=month4.select('month').toPandas()
plt.figure(figsize=(25, 10))
scatterplt = plt.scatter(months,counts,color='red')
plot = plt.plot(months,counts)

plt.show()

# COMMAND ----------

# DBTITLE 1,Monthly Yearly Trends Explanations
# MAGIC %md
# MAGIC The monthly trends to follow the same pattern as the daily yearly trends and weekly yearly trends but is smoother. There seems to be a lot of bike trips in November and significant dip in trips in April. 

# COMMAND ----------

# DBTITLE 1,Monthly Trends
month2 = (historic_trip_data_df.withColumn("month", dayofmonth("started_at")))
month3 = (month2.select("month", "rideable_type"))
month4 = month2.groupBy(month3.month).count().orderBy(month2.month)
month4.show(31)


# COMMAND ----------

import matplotlib.pyplot as plt
counts=month4.select('count').toPandas()
months=month4.select('month').toPandas()
plt.figure(figsize=(25, 10))
scatterplt = plt.scatter(months,counts,color='red')
plot = plt.plot(months,counts)

plt.show()

# COMMAND ----------

# DBTITLE 1,Monthly Trends
# MAGIC %md
# MAGIC It seems that the most ridden days tend to be at the start-middle of the month, and then drops off at the end of the month. This could be attributed to motivation. Sometimes people want to get very fit at the start of a month and then lose motivation towards the end of the month. Also not every month has 31 days, which explains the very sudden drop at the end. 

# COMMAND ----------

year2 = (historic_trip_data_df.withColumn("year", year("started_at")))
year3 = (year2.select("year", "rideable_type"))
year4 = year2.groupBy(year3.year).count().orderBy(year2.year)
year4.show()

# COMMAND ----------

import matplotlib.pyplot as plt
counts=year4.select('count').toPandas()
years=year4.select('year').toPandas()
plt.figure(figsize=(25, 10))
scatterplt = plt.scatter(years,counts,color='red')
plot = plt.plot(years,counts)

plt.show()

# COMMAND ----------

# DBTITLE 1,Yearly Trends Explanations
# MAGIC %md
# MAGIC The year trends tell us that there were a lot more trips in 2022 than in 2021 or 2023. 
# MAGIC
# MAGIC 2021 was still pretty heavily impacted by covid regualtions so people were probably less willing to go on bikes that are shared by a lot of people. 
# MAGIC
# MAGIC 2022 probably has the most trips in it, because in 2022 pretty much all covid restrictions were lifted and people were more motivated to go outside more and ride more bikes. 
# MAGIC
# MAGIC 2023 has less trips in it, because the year is still not done. 

# COMMAND ----------

# DBTITLE 1,Weather Trends
weather_trips = (spark.read.format("delta").load("dbfs:/FileStore/tables/G11/silver/inventory/"))
weather_trips.display()

# COMMAND ----------

from pyspark.sql.functions import *

df4 = (weather_trips.withColumn("hour_change", abs("net_hour_change")))
df5 = (df4.select("main", "hour_change"))

# COMMAND ----------

import matplotlib.pyplot as plt
df6 = df4.groupBy(df5.main).count().orderBy(df5.main)
df = df6.select("*").toPandas()
ax = df.plot.bar(x='main', y='count', rot=30)

# COMMAND ----------

# DBTITLE 1,Weather Trends Explanations
# MAGIC %md
# MAGIC Our findings here are pretty interesting. There are pretty much no rides being done under more extreme conditions such as thunderstorms, snow, and smoke. It seems as though most of the activity is being done during times where it is either cloudy or clear skies. Although there is a decrease in activity while it is raining, there is still a fair amount of activity. As you can see on the histogram there are basically no rides during drizzle, fog, or haze. This is most likely due to lack of reporting those weather conditions, it likely happens more often than displayed here.

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))

# COMMAND ----------


