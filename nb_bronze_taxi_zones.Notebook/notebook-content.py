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

import geopandas as gpd
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# BRONZE_PATH = notebookutils.variableLibrary.getLibrary('storage_lib').bronze_path
# SHAPE_FILE = BRONZE_PATH + '/Files/reference/taxi_zones/taxi_zones.shp'
SHAPE_FILE = '/lakehouse/default/Files/reference/taxi_zones/taxi_zones.shp'

gdf = gpd.read_file(SHAPE_FILE)
gdf = gdf.to_crs(4326)
gdf['geometry_wkt'] = gdf.geometry.to_wkt()

pdf = gdf[['LocationID', 'zone', 'borough', 'geometry_wkt']].rename(columns={
    'LocationID': 'zone_id', 'zone': 'zone_name'
}).copy()
pdf['zone_id'] = pdf['zone_id'].astype('int32')

schema = StructType([
    StructField("zone_id", IntegerType(), False),
    StructField("zone_name", StringType(), True),
    StructField("borough", StringType(), True),
    StructField("geometry_wkt", StringType(), False),
])
sdf = spark.createDataFrame(pdf, schema=schema).withColumn(
    "loaded_at", F.to_date(F.current_timestamp())
)

(sdf.write.format("delta").mode("overwrite")
    .saveAsTable("lh_bronze.dbo.taxi_zone_shapes"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
