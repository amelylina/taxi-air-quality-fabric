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
# META     "environment": {
# META       "environmentId": "465e8bc1-939a-9227-4dc4-5b5c6bda6737",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

from datetime import datetime, timezone
import requests
import json
from wh_conn import get_con, check_con

SERVER_NAME = notebookutils.variableLibrary.getLibrary('storage_lib').server_url
WAREHOUSE_NAME = 'wh_gold'
REFRESH_TOKEN = mssparkutils.credentials.getSecret(
    "https://fabric-project.vault.azure.net/",
    "onedrive-upload-refresh-token",
)
parsed_ts = datetime.now().strftime('%Y%m%d_%H%M')
file_name = "taxi_report_" + parsed_ts

GRAPH_URL = f"https://graph.microsoft.com/v1.0/me/drive/root:/FabricExport/{file_name}.json:/content"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

conn = get_con(SERVER_NAME, WAREHOUSE_NAME)
try:
    with conn.cursor() as cur:
        cur.execute ("""
            SELECT TOP 50 
                date_key, pickup_zone_id, trip_count, total_revenue_usd
            FROM wh_gold.dbo.fct_taxi_daily
            WHERE date_key >= 20241201 AND date_key <= 20241231
            ORDER BY date_key, pickup_zone_id
        """)
        columns = [col[0] for col in cur.description]
        rows = [
            dict(zip(columns, row))
            for row in cur.fetchall()
        ]
except Exception:
    raise
finally:
    conn.close()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

payload = {
    "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    "source": "wh_gold.dbo.fct_taxi_daily",
    "period_description": "December 2024, top 50 (date, zone) rows",
    "row_count": len(rows),
    "rows": rows,
}

response = requests.put(
    GRAPH_URL,
    headers={
        "Authorization": f"Bearer {REFRESH_TOKEN}",
        "Content-Type": "application/json"
    },
    json=payload
)
response.raise_for_status()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
