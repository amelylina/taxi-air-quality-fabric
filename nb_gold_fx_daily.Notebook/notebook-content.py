# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "41c4f02d-1dba-4787-a05c-4fb6306e3f15",
# META       "default_lakehouse_name": "lh_silver",
# META       "default_lakehouse_workspace_id": "2e0a9a0f-a9ac-4770-9137-10b52d0b6df6",
# META       "known_lakehouses": [
# META         {
# META           "id": "41c4f02d-1dba-4787-a05c-4fb6306e3f15"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "bc3150e9-84db-a7dc-45b0-8091bba045ea",
# META       "known_warehouses": [
# META         {
# META           "id": "bc3150e9-84db-a7dc-45b0-8091bba045ea",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE SCHEMA IF NOT EXISTS stg;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

last_processed_ts="1990-01-01"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

new_watermark = spark.sql("SELECT MAX(loaded_at) FROM lh_silver.dbo.fx_daily").collect()[0][0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants  
from pyspark.sql import functions as F, DataFrame
from datetime import datetime

STG_TABLE = "lh_silver.stg.fct_fx_daily"
CURR_TABLE = "wh_gold.dbo.dm_currency"

currency = spark.read.synapsesql(CURR_TABLE)

new_data = (
    spark.table("lh_silver.dbo.fx_daily")
    .filter(
        (F.col("loaded_at") > F.lit(last_processed_ts)) &
        (F.col("loaded_at") <= F.lit(new_watermark))
    )
)

cur_from = currency.alias("cur_from")
cur_to = currency.alias("cur_to")

joined = (new_data.alias("n")
    .join(
        cur_from,
        F.col("n.from_currency") == F.col("cur_from.currency_name"),
        "left"
    )
    .join(
        cur_to,
        F.col("n.to_currency") == F.col("cur_to.currency_name"),
        "left"
    )
    .select(
        "n.*",
        F.col("cur_from.id").alias("id_cur_from"),
        F.col("cur_to.id").alias("id_cur_to")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold = (joined
    .withColumn(
        "date_key",
        F.date_format("rate_date", "yyyyMMdd").cast("int")
    )
    .select("date_key","id_cur_from", "id_cur_to", "rate")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(gold.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(STG_TABLE))

mssparkutils.notebook.exit(new_watermark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
