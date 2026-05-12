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

from shapely import wkt
from shapely.geometry import Point
import pandas as pd
import geopandas as gpd

zones_pdf = spark.read.table("lh_bronze.dbo.taxi_zone_shapes").toPandas()
zones_pdf['geom'] = zones_pdf['geometry_wkt'].apply(wkt.loads)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

zones_gdf = gpd.GeoDataFrame(zones_pdf[['zone_id', 'zone_name', 'borough']], geometry=zones_pdf['geom'], crs=4326)

sensors_pdf = (spark.read.table("lh_bronze.dbo.openaq_locations")
    .select("location_id", "sensor_id", "p_name", "latitude", "longitude")
    .dropDuplicates(["sensor_id"])
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
                 'zone_id', 'zone_name', 'borough']].copy()
result['mapped_at'] = pd.Timestamp.utcnow()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sdf = spark.createDataFrame(result)
sdf.write.format("delta").mode("overwrite").saveAsTable("lh_silver.dbo.openaq_sensor_zones")

n_unmapped = result['zone_id'].isna().sum()
print(f"sensors not mapped to any zone: {n_unmapped} / {len(result)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
