# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "465e8bc1-939a-9227-4dc4-5b5c6bda6737",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     },
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

# PARAMETERS CELL ********************

source_name = "openaq_nyc_daily"
date_from = "2023-01-01"
date_to = "2024-12-01" #inclusive of this month
url_template = ""
table_name = "meta.ingestion_control"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import date
from datetime import datetime
from dateutil.relativedelta import relativedelta

from wh_conn import get_con, check_con

SERVER_NAME = notebookutils.variableLibrary.getLibrary('storage_lib').server_url
META_WAREHOUSE = "wh_meta"

def month_range(date_from: date, date_to: date):
    cur = date(date_from.year, date_from.month, 1)
    end = date(date_to.year, date_to.month, 1)
    while cur <= end:
        yield cur
        cur += relativedelta(months=1)

def build_url(template: str | None, month_start: date) -> str | None:
    if template is None:
        return None
    yyyy_mm = month_start.strftime("%Y-%m")
    yyyy = month_start.strftime("%Y")
    mm = month_start.strftime("%m")
    return (template.replace("{yyyy-mm}", yyyy_mm).replace("{yyyy}", yyyy).replace("{mm}", mm))

def build_monthly_rows(source_name: str, date_from: date, date_to: date, url_template: str | None):
    return [
        (source_name, m.strftime("%Y-%m"), build_url(url_template, m))
        for m in month_range(date_from, date_to)
    ]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

INSERT_SQL = """
INSERT INTO meta.ingestion_control
    (source_name, partition_key, source_url, status, created_at)
SELECT ?, ?, ?, 'pending', CURRENT_TIMESTAMP
WHERE NOT EXISTS (
    SELECT 1 FROM meta.ingestion_control
    WHERE source_name = ? AND partition_key = ?
);
"""

def seed_partitions(rows: list[tuple]) -> int:
    if not rows:
        return 0

    payload = [
        (source, part, url, source, part)
        for (source, part, url) in rows
    ]

    conn = get_con(SERVER_NAME, META_WAREHOUSE)
    try:
        with conn.cursor() as cur :
            cur.executemany(INSERT_SQL, payload)
        conn.commit()
    except:
        conn.rollback()
    
    conn.close()

    return

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

date_from = datetime.strptime(date_from, '%Y-%m-%d').date()
date_to = datetime.strptime(date_to, '%Y-%m-%d').date()
all_rows = build_monthly_rows(source_name, date_from, date_to, None)
print(all_rows)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

seed_partitions(all_rows)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
