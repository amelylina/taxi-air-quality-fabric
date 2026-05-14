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
    "LocationID" : "zone_id",
    "Borough" : "borough",
    "Zone" : "zone_name",
    "Service_zone" : "service_zone"
}
target_types = {
    "zone_id" : "integer",
    "borough" : "string",
    "zone_name" : "string",
    "service_zone" : "string"
}

df = spark.read.option("header", True).csv(path=ZONE_FILE)
df = df.withColumnsRenamed(col_map)
for col_name, ttype in target_types.items():
    df = df.withColumn(col_name, F.col(col_name).cast(ttype))

df.write.mode("overwrite").synapsesql(ZONE_TABLE)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DATE_TABLE = WAREHOUSE_NAME + ".dbo.dm_date"

date_dim = (spark.range(0, 4018)
    .select((F.lit("2020-01-01").cast("date") + F.col("id").cast("int")).alias("date"))
    .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("date"))
    .withColumn("month", F.month("date"))
    .withColumn("day", F.dayofmonth("date"))
    .withColumn("day_of_week", F.dayofweek("date"))
    .withColumn("day_name", F.date_format("date", "EEEE"))
    .withColumn("month_name", F.date_format("date", "MMMM"))
    .withColumn("quarter", F.quarter("date"))
    .withColumn("is_weekend", F.dayofweek("date").isin(1, 7))
    .withColumn("year_month", F.date_format("date", "yyyy-MM"))
)
date_dim.write.mode("overwrite").synapsesql(DATE_TABLE)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

VENDOR_TABLE = WAREHOUSE_NAME + ".dbo.dm_vendor"

vendors = [
    (1, 'Creative Mobile Technologies, LLC'),
    (2, 'VeriFone Inc.')
]

vendor_df = spark.createDataFrame(vendors,schema='id int, vendor_name string')
vendor_df.write.mode("overwrite").synapsesql(VENDOR_TABLE)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

PAYMENT_TABLE = WAREHOUSE_NAME + ".dbo.dm_payment"

payments = [
    (1, 'Credit card'),
    (2, 'Cash'),
    (3, 'No charge'),
    (4, 'Dispute'),
    (5, 'Unknown'),
    (6, 'Voided trip')
]
payment_df = spark.createDataFrame(payments,schema='id int, payment_type string')
payment_df.write.mode("overwrite").synapsesql(PAYMENT_TABLE)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

PARAMS_FILE = BRONZE_PATH + '/Files/reference/sensor_parameters.csv'
PARAMS_TABLE = WAREHOUSE_NAME + '.dbo.dm_air_measurement'

df = spark.read.option("header", True).csv(path=PARAMS_FILE, schema="p_id int, p_name string, p_units string, p_display_name string")
df.write.mode("overwrite").synapsesql(PARAMS_TABLE)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
