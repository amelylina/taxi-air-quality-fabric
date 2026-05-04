# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.12"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "816baf1d-ce43-49b9-b16a-07c9169f7772",
# META       "default_lakehouse_name": "lh_meta",
# META       "default_lakehouse_workspace_id": "2e0a9a0f-a9ac-4770-9137-10b52d0b6df6",
# META       "known_lakehouses": [
# META         {
# META           "id": "816baf1d-ce43-49b9-b16a-07c9169f7772"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "009ed940-53ea-4243-8ee9-733f3448c3ae",
# META       "known_warehouses": [
# META         {
# META           "id": "009ed940-53ea-4243-8ee9-733f3448c3ae",
# META           "type": "MountedWarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

!pip install mssql-python

from mssql_python import connect
import struct
import pandas as pd

token_bytes = notebookutils.credentials.getToken("https://database.windows.net").encode("UTF-16-LE")

# Connection String for your warehouse 
server_name = "ousxut53kpee7fiotk4npe34qq-b6naulvmvfyepejxcc2s2c3n6y.datawarehouse.fabric.microsoft.com"

# Warehouse Name 
database_name = "wh_meta"

connection_string = (
    f"Server=tcp:{server_name},1433;"
    f"Database={database_name};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
)

# Convert token to ODBC compatible token format
token_struct = struct.pack(
    f"<I{len(token_bytes)}s", 
    len(token_bytes), 
    token_bytes
)

# Create the Driver Connection 
conn = connect(connection_string, attrs_before={1256: token_struct})

# # Pandas example
# df_pandas = pd.read_sql("SELECT * FROM meta.ingestion_control", conn)
# display(df_pandas)

# Native Cursor Example
cursor = conn.cursor()
cursor.execute("""
INSERT INTO meta.ingestion_control
(source_name, partition_key, source_url, status, created_at)
VALUES ('test', '2025-01', 'yellow_tripdata_2025-01.parquet', 'lmao', CURRENT_TIMESTAMP)
""")
# Pandas example
df_pandas = pd.read_sql("SELECT * FROM meta.ingestion_control", conn)
display(df_pandas)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
