# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8871bb89-78f7-468a-ba37-05ee576b8bdb",
# META       "default_lakehouse_name": "earthquake_lakehouse",
# META       "default_lakehouse_workspace_id": "7a34bd9a-aea5-469d-8449-99e0cc9e83ab",
# META       "known_lakehouses": [
# META         {
# META           "id": "8871bb89-78f7-468a-ba37-05ee576b8bdb"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# from datetime import date, timedelta

# start_date = date.today() - timedelta(7)
# end_date = date.today() - timedelta(1)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import json 

url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_date}&endtime={end_date}"
response = requests.get(url)
# print(response.status_code)
if (response.status_code == 200):
   rs = response.json()
   data = rs['features']

   file_path = f"/lakehouse/default/Files/{start_date}_earthquake_date.json"

   with open(file_path, 'w') as file: 
    json.dump(data, file, indent=4)
else: 
    print(f"Failed to load data from API: {response.status_code}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
