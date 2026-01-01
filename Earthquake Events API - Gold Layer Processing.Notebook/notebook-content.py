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
# META     },
# META     "environment": {
# META       "environmentId": "6d63afe7-5c7e-a4c0-4cba-f813e70ce7e1",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import col, when, udf
from pyspark.sql.types import StringType
import reverse_geocoder as rg

df = spark.read.table("earthquake_event_silver").filter(col("time") > start_date)
#display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_country_code(lat, lon):
    coordinates = (float(lat), float(lon))
    return rg.search(coordinates)[0].get('cc')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

get_country_code_udf = udf(get_country_code, StringType()) # registering the udfs so they can be used in spark dataframes

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_selected = df.withColumn("country_code", get_country_code_udf(col("latitude"), col("longitude")))\
                .withColumn("sig_class", when(col("sig")<100, "Low")\
                                        .when((col("sig") >= 100) & (col("sig") < 500), "Moderate").\
                                        otherwise("High"))
                                        
df_selected.write.mode("append").saveAsTable("earthquake_events_gold")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
