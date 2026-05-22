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
# META     "environment": {
# META       "environmentId": "5785e204-d1c7-aa79-4777-44104137e0e8",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from shapely import wkt
from shapely.geometry import Point
import pandas as pd
import geopandas as gpd
import json

zone_table = spark.read.table("lh_bronze.dbo.taxi_zone_shapes")
if zone_table.isEmpty():
    raise RuntimeError(
        "taxi zones table is empty"
    )
else: 
    zones_pdf = zone_table.toPandas()
zones_pdf['geom'] = zones_pdf['geometry_wkt'].apply(wkt.loads)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

zones_gdf = gpd.GeoDataFrame(zones_pdf[['zone_id', 'zone_name', 'borough']], geometry=zones_pdf['geom'], crs=4326)

sensors_pdf = (spark.read.table("lh_bronze.dbo.openaq_locations")
    .select("location_id", "sensor_id", "p_name", "latitude", "longitude","loaded_at")
    .withColumn("row_num", F.row_number().over(
        Window.partitionBy("sensor_id")
              .orderBy(F.col("loaded_at").desc())
    ))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
    .toPandas())

sensors_gdf = gpd.GeoDataFrame(
    sensors_pdf,
    geometry=gpd.points_from_xy(sensors_pdf.longitude, sensors_pdf.latitude),
    crs=4326,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

joined = gpd.sjoin(sensors_gdf, zones_gdf, how="left", predicate="within")

result = joined[['sensor_id', 'location_id', 'p_name', 
                 'latitude', 'longitude',
                 'zone_id', 'zone_name', 'borough','loaded_at']].copy()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

SCHEMA = "sensor_id long, location_id long, p_name string, latitude double, longitude double, zone_id int, zone_name string, borough string, loaded_at timestamp"

sdf = spark.createDataFrame(result, SCHEMA)
sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("lh_silver.dbo.openaq_sensor_zones")

n_unmapped = result['zone_id'].isna().sum()
sensor_count = len(result)
n_mapped = sensor_count-n_unmapped

mssparkutils.notebook.exit(json.dumps({
    "mapped" : n_mapped,
    "unmapped" : n_unmapped,
    "total_sensors" : sensor_count,
}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
