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

# PARAMETERS CELL ********************

start_date = '2025-12-25'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType

df = spark.read.option("multiline", "true").json(f"Files/{start_date}_earthquake_date.json")

df_selected = df.select(
                "id", 
                col("geometry.coordinates").getItem(0).alias("longitude"),
                col("geometry.coordinates").getItem(1).alias("latitude"),
                col("geometry.coordinates").getItem(2).alias("elevation"),
                col("properties.title"),
                col("properties.place"),
                col("properties.sig"),
                col("properties.mag"),
                col("properties.magType"),
                col("properties.time"), # seconds from Jan 1 1970
                col("properties.updated"),
            )
df_selected = df_selected.withColumn("time", (col('time')/1000).cast(TimestampType()))\
                        .withColumn("updated", (col('updated')/1000).cast(TimestampType()))

#display(df_selected)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import * 

deltaTable = DeltaTable.forName(spark,"earthquake_events_silver")
dfUpdated = df_selected

deltaTable.alias('silver')\
.merge(
    dfUpdated.alias('updates'),
    'silver.id == updates.id'
).whenNotMatchedInsertAll().whenMatchedUpdateAll().execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#df_selected.write.mode("append").saveAsTable("earthquake_events_silver")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
