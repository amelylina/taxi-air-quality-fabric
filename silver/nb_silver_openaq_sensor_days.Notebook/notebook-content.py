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

from pyspark.sql.window import Window
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime
import json

BRONZE_TABLE = "lh_bronze.dbo.openaq_measurements_daily"
SILVER_TABLE = "lh_silver.dbo.openaq_measurements"

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
    .filter(F.col("value").isNotNull())
    .filter(F.col("value") >= 0)
    .filter(F.col("coverage_pct") >= 75)
    .filter(F.col("date_utc").isNotNull())
    .withColumn("row_num", F.row_number().over(
        Window.partitionBy("sensor_id", "date_utc", "parameter")
              .orderBy(F.col("loaded_at").desc())
    ))
    .filter(F.col("row_num") == 1)
    .drop("row_num")

    .withColumn("year",  F.year("date_utc"))
    .withColumn("month", F.month("date_utc"))
    .select(
        "sensor_id", "location_id", "parameter", "units",
        F.col("date_utc").alias("measurement_date"),
        "value", "min_val", "max_val", "median_val", "coverage_pct",
        "year", "month", "loaded_at"
    )
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
            t.sensor_id = s.sensor_id
            AND t.measurement_date = s.measurement_date
            AND t.parameter = s.parameter
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
