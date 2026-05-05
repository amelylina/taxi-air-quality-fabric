# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c4c9cf64-1667-4152-bd26-12854382cfbe",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "2e0a9a0f-a9ac-4770-9137-10b52d0b6df6",
# META       "known_lakehouses": [
# META         {
# META           "id": "c4c9cf64-1667-4152-bd26-12854382cfbe"
# META         }
# META       ]
# META     },
# META     "environment": {}
# META   }
# META }

# CELL ********************

import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants
from pyspark.sql import functions as F, DataFrame

BRONZE_PATH = notebookutils.variableLibrary.getLibrary('storage_lib').bronze_path
WAREHOUSE_NAME = "wh_gold"

ZONE_TABLE = WAREHOUSE_NAME + ".dbo.dm_zone"
ZONE_FILE = BRONZE_PATH + "/Files/reference/taxi_zone_lookup.csv"

col_map = {
    "LocationID" : "location_id",
    "Borough" : "borough",
    "Zone" : "zone",
    "Service_zone" : "service_zone"
}
target_types = {
    "location_id" : "integer",
    "borough" : "string",
    "zone" : "string",
    "service_zone" : "string"
}

df = spark.read.option("header", True).csv(path=ZONE_FILE)
df = df.withColumnsRenamed(col_map)
for col_name, ttype in target_types.items():
    df = df.withColumn(col_name, F.col(col_name).cast(ttype))

df.write.mode("overwrite").synapsesql(f"{ZONE_TABLE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd

DATE_TABLE = WAREHOUSE_NAME + ".dbo.dm_date"

dates = pd.date_range('2023-01-01', '2024-12-31')
dim_date = pd.DataFrame({
    'date_key': dates.strftime('%Y%m%d').astype(int),
    'date': dates,
    'year': dates.year, 'quarter': dates.quarter, 'month': dates.month,
    'day': dates.day, 'day_of_week': dates.dayofweek,
    'day_name': dates.day_name(), 'month_name': dates.month_name(),
    'is_weekend': dates.dayofweek >= 5,
})

spark_df = spark.createDataFrame(dim_date)
spark_df.write.mode("overwrite").synapsesql(f"{DATE_TABLE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
