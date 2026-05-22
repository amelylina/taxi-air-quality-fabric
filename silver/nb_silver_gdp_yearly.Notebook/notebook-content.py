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
# META         },
# META         {
# META           "id": "41c4f02d-1dba-4787-a05c-4fb6306e3f15"
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

BRONZE_TABLE = "lh_bronze.dbo.worldbank_gdp"
SILVER_TABLE = "lh_silver.dbo.gdp_yearly"

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
        F.col("countryiso3code").alias("country_code"),
        F.col("country_name"),
        F.col("indicator_id"),
        F.col("indicator_name"),
        F.col("year"),
        F.col("value").cast(DecimalType(20, 2)).alias("gdp_usd"),
        F.col("loaded_at"),
    )
    .filter(F.col("gdp_usd").isNotNull())
    .filter(F.col("gdp_usd") > 0)
    .filter(F.col("year").isNotNull())
    .withColumn("row_num", F.row_number().over(
        Window.partitionBy("country_code", "indicator_id", "year")
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

if spark.catalog.tableExists(SILVER_TABLE):
    silver_table = DeltaTable.forName(spark, SILVER_TABLE)
    (silver_table.alias("t")
        .merge(
            clean.alias("s"),
            """
            t.country_code = s.country_code
            AND t.indicator_id = s.indicator_id
            AND t.year = s.year
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
