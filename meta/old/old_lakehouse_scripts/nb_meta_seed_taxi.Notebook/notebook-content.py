# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "warehouse": {
# META       "default_warehouse": "e4ef323f-de18-8ccd-43b7-b933c540ca12",
# META       "known_warehouses": [
# META         {
# META           "id": "e4ef323f-de18-8ccd-43b7-b933c540ca12",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from datetime import date
from dateutil.relativedelta import relativedelta
from pyspark.sql import Row

SOURCE_NAME    = "TLC_yellow_taxi"
START_YEAR_MONTH = "2023-01"
END_YEAR_MONTH   = "2024-12"
URL_TEMPLATE   = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{ym}.parquet"

def month_range(start_ym, end_ym):
    cur  = date.fromisoformat(start_ym + "-01")
    stop = date.fromisoformat(end_ym + "-01")
    while cur <= stop:
        yield cur.strftime("%Y-%m")
        cur += relativedelta(months=1)

candidates = [
    Row(source_name=SOURCE_NAME,
        partition_key=ym,
        source_url=URL_TEMPLATE.format(ym=ym))
    for ym in month_range(START_YEAR_MONTH, END_YEAR_MONTH)
]

df = spark.createDataFrame(candidates)
df.createOrReplaceTempView("candidates")

spark.sql("""
MERGE INTO meta.ingestion_control AS target
USING candidates AS source
  ON target.source_name = source.source_name
 AND target.partition_key = source.partition_key
WHEN NOT MATCHED THEN INSERT (
    source_name, partition_key, source_url, status, created_at
) VALUES (
    source.source_name, source.partition_key, source.source_url, 'pending', current_timestamp()
)
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
