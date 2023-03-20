# Databricks notebook source
import pandas as pd
import requests
import json
import datetime

# COMMAND ----------

GROUPS = [['G01', 'tcharle3@ur.rochester.edu', 'jbrehm@ur.rochester.edu', 'ccolli29@ur.rochester.edu', 'varvind@ur.rochester.edu', 'jwang247@ur.rochester.edu'],
['G02', 'gdumpa@ur.rochester.edu', 'vkakunur@ur.rochester.edu', 'nmalkann@ur.rochester.edu', 'hkotavet@ur.rochester.edu', 'sroy22@ur.rochester.edu'],
['G03', 'lnguy32@ur.rochester.edu', 'hvu7@ur.rochester.edu', 'btran10@u.rochester.edu', 'jperovi3@ur.rochester.edu', None],
['G04', 'tshroff@ur.rochester.edu', 'asingla3@ur.rochester.edu', 'prao13@ur.rochester.edu', 'htalele@ur.rochester.edu', 'hloya@ur.rochester.edu'],
['G05', 'rkandoi@ur.rochester.edu', 'amath12@ur.rochester.edu', 'ryadav3@ur.rochester.edu', None, None],
['G06', 'achilla@ur.rochester.edu', 'nkorrapo@ur.rochester.edu', 'dmunot@ur.rochester.edu', 'apalit@ur.rochester.edu', 'sgarg11@ur.rochester.edu'],
['G07', 'skishor2@ur.rochester.edu', 'ssingh71@ur.rochester.edu', 'sswar@ur.rochester.edu', 'dgarg@ur.rochester.edu', 'stamhane@ur.rochester.edu'],
['G08', 'ikaplan4@u.rochester.edu', 'mkingsl6@u.rochester.edu', 'agolli@u.rochester.edu', 'nboonin@u.rochester.edu ', None],
['G09', 'jchen134@u.rochester.edu', 'qtang5@u.rochester.edu', 'jyon@u.rochester.edu', 'cmiao3@u.rochester.edu', 'zwang154@u.rochester.edu'],
['G10', 'zgu12@ur.rochester.edu', 'tluo9@ur.rochester.edu', 'dyu18@ur.rochester.edu', 'jcrosset@ur.rochester.edu', 'klee109@u.rochester.edu'],
['G11', 'vchistay@u.rochester.edu', 'fcolombo@u.rochester.edu', 'agirsky@u.rochester.edu', 'ajaweed@ur.rochester.edu', 'jtschopp@u.rochester.edu'],
['G12', 'nwang28@ur.rochester.edu', 'mwu52@ur.rochester.edu', 'clu36@ur.rochester.edu', 'aabbaraj@ur.rochester.edu', None],
['G13', 'akarunan@ur.rochester.edu', 'eklutse@ur.rochester.edu', 'snuamaha@ur.rochester.edu', 'tmurali@ur.rochester.edu', 'stodi@ur.rochester.edu'],
['GXX', 'lpalum@gmail.com', None, None, None, None]
]


# COMMAND ----------

# DBTITLE 1,Define Project Global Variables
NYC_WEATHER_FILE_PATH = 'dbfs:/FileStore/tables/raw/weather/NYC_Weather_Data.csv'
BIKE_TRIP_DATA_PATH = 'dbfs:/FileStore/tables/raw/bike_trips/'

BIKE_STATION_JSON = "https://gbfs.citibikenyc.com/gbfs/es/station_information.json"
BIKE_STATION_STATUS_JSON = "https://gbfs.citibikenyc.com/gbfs/es/station_status.json"

# Some configuration of the cluster
spark.conf.set("spark.sql.shuffle.partitions", "40")  # Configure the size of shuffles the same as core count on your cluster
spark.conf.set("spark.sql.adaptive.enabled", "true")  # Spark 3.0 AQE - coalescing post-shuffle partitions, converting sort-merge join to broadcast join, and skew join optimization


USER_NAME = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())['tags']['user']
GROUP_NAME = ""

for i in range(len(GROUPS)):
    if USER_NAME in GROUPS[i]:
        GROUP_NAME = GROUPS[i][0]

if GROUP_NAME == "":
    dbutils.notebook.exit(f"{USER_NAME} is  not assigned to a final project group")
    

GROUP_MODEL_NAME = f"{GROUP_NAME}_model"

GROUP_DB_NAME = f"{GROUP_NAME}_db"

GROUP_DATA_PATH = f"dbfs:/FileStore/tables/{GROUP_NAME}/"


# Setup the hive meta store if it does not exist and select database as the focus of future sql commands in this notebook
spark.sql(f"CREATE DATABASE IF NOT EXISTS {GROUP_DB_NAME}")
spark.sql(f"USE {GROUP_DB_NAME}")

# COMMAND ----------

# DBTITLE 1,Utility Functions to Read Station Status and Information
def get_bike_station_status():
  df = pd.read_json(BIKE_STATION_STATUS_JSON)
  if len(df)==0:
    print(f"Unable to retreive station status")
    df=pd.DataFrame([])
  return df['last_updated'].values[0], pd.json_normalize(df['data']['stations'])

def get_bike_stations():
  df = pd.read_json(BIKE_STATION_JSON)
  if len(df)==0:
    print(f"Unable to retreive stations")
    df=pd.DataFrame([])
  return df['last_updated'].values[0], pd.json_normalize(df['data']['stations'])



# COMMAND ----------

# DBTITLE 1,Display the Project Global Variables
displayHTML(f"""
<table border=1>
<tr><td><b>Variable Name</b></td><td><b>Value</b></td></tr>
<tr><td>NYC_WEATHER_FILE_PATH</td><td>{NYC_WEATHER_FILE_PATH}</td></tr>
<tr><td>BIKE_TRIP_DATA_PATH</td><td>{BIKE_TRIP_DATA_PATH}</td></tr>
<tr><td>BIKE_STATION_JSON</td><td>{BIKE_STATION_JSON}</td></tr>
<tr><td>BIKE_STATION_STATUS_JSON</td><td>{BIKE_STATION_STATUS_JSON}</td></tr>
<tr><td>USER_NAME</td><td>{USER_NAME}</td></tr>
<tr><td>GROUP_NAME</td><td>{GROUP_NAME}</td></tr>
<tr><td>GROUP_DATA_PATH</td><td>{GROUP_DATA_PATH}</td></tr>
<tr><td>GROUP_MODEL_NAME</td><td>{GROUP_MODEL_NAME}</td></tr>
<tr><td>GROUP_DB_NAME</td><td>{GROUP_DB_NAME}</td></tr>
</table>
""")
