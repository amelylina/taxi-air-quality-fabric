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
# META     "warehouse": {
# META       "default_warehouse": "461ede96-fee3-4cb5-a540-1f0ecc40fb6c",
# META       "known_warehouses": [
# META         {
# META           "id": "461ede96-fee3-4cb5-a540-1f0ecc40fb6c",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

watermark_ts = "1900-01-01"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime
import json

BRONZE_TABLE = "lh_bronze.dbo.ecb_fx_daily"
SILVER_TABLE = "lh_silver.dbo.fx_daily"

if watermark_ts is None:
    watermark_ts = datetime(1900, 1, 1)

new_bronze = (spark.read.table(BRONZE_TABLE)
    .filter(F.col("loaded_at") > F.lit(watermark_ts))
)

new_ts = new_bronze.agg(F.max("loaded_at")).collect()[0][0]

if new_ts is None:
    mssparkutils.notebook.exit({"status": "succeeded", "rows": 0})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

clean = (new_bronze
    .select(
        F.col("TIME_PERIOD").alias("rate_date"),
        F.col("CURRENCY").alias("from_currency"),
        F.col("CURRENCY_DENOM").alias("to_currency"),
        F.col("OBS_VALUE").alias("rate"),
        F.col("OBS_STATUS").alias("obs_status"),
        F.col("FREQ").alias("frequency"),
        F.col("loaded_at"),
    )
    .filter(F.col("obs_status")=='A')
    .filter(F.col("rate").isNotNull())
    .filter(F.col("rate")>0)
    .withColumn("rate", F.col("rate").cast(DecimalType(18,6)))
    .withColumn("rate_date", F.to_date("rate_date"))
    .withColumn("row_num", F.row_number().over(
        Window.partitionBy("rate_date", "from_currency", "to_currency")
              .orderBy(F.col("loaded_at").desc())
    ))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

date_range = (
    clean.select(
        F.min("rate_date").alias("min_date"),
        F.max("rate_date").alias("max_date")
    ).select(
        F.explode(F.sequence("min_date","max_date"))
        .alias("rate_date")
    )
)

pairs = clean.select("from_currency","to_currency").distinct()
spine = date_range.crossJoin(pairs)

filled = (
    spine.join(clean.select("rate_date", "from_currency", "to_currency", "rate"),
    ["rate_date", "from_currency", "to_currency"],"left")
)

w = (Window.partitionBy("from_currency","to_currency")
    .orderBy("rate_date")
    .rowsBetween(Window.unboundedPreceding, 0)
)

clean_filled = (filled
    .withColumn("rate", F.last("rate", ignorenulls=True).over(w))
    .filter(F.col("rate").isNotNull())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if spark.catalog.tableExists(SILVER_TABLE):
    silver_table = DeltaTable.forName(spark, SILVER_TABLE)
    (silver_table.alias("t")
        .merge(
            clean.alias("s"),
            """
            t.rate_date = s.rate_date
            AND t.from_currency = s.from_currency
            AND t.to_currency = s.to_currency
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    clean.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLE)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

exit_payload = {
    "row_count" : clean.count(),
    "new_ts" : new_ts.isoformat(),
    "source_table" : BRONZE_TABLE,
    "target_table" : SILVER_TABLE,
}

mssparkutils.notebook.exit(json.dumps(exit_payload))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
