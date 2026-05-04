# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.12"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from datetime import datetime, timezone
from delta.tables import DeltaTable
from pyspark.sql import functions as F, Row

META_LAKEHOUSE = "lh_meta"
CONTROL_TABLE = f"{META_LAKEHOUSE}.meta.ingestion_control"
RUN_LOG_TABLE = f"{META_LAKEHOUSE}.meta.pipeline_run_log"

def _utcnow():
    return datetime.now(timezone.utc)

def log_pipeline_start(run_id: str, pipeline_name: str, layer: str):
    started_at = _utcnow()
    row = Row(
        run_id=run_id, pipeline_name=pipeline_name, layer=layer,
        status="running", started_at=started_at, ended_at=None,
        message=None, logged_at=started_at,
    )
    (spark.createDataFrame([row])
        .write.format("delta").mode("append").saveAsTable(RUN_LOG_TABLE))
    return started_at

def log_pipeline_end(run_id: str, status: str, message: str = None):
    ended_at = _utcnow()
    safe_msg = (message or "")[:4000]
    log_table = DeltaTable.forName(spark, RUN_LOG_TABLE)
    update_df = spark.createDataFrame(
        [(run_id, status, safe_msg, ended_at)],
        ["run_id", "new_status", "new_message", "new_ended_at"]
    )
    (log_table.alias("t")
        .merge(update_df.alias("s"), "t.run_id = s.run_id")
        .whenMatchedUpdate(set={
            "status": "s.new_status",
            "message": "s.new_message",
            "ended_at": "s.new_ended_at",
        })
        .execute())

def get_pending_partitions(source_name: str, layer: str = "silver"):
    if layer == "bronze":
        condition = "status = 'pending'"
    elif layer == "silver":
        condition = "status = 'succeeded' AND silver_status = 'pending'"
    else:
        raise ValueError(f"Unknown layer: {layer}")
    
    df = (spark.read.table(CONTROL_TABLE)
        .filter(f"source_name = '{source_name}' AND {condition}")
        .select("partition_key", "source_url")
        .orderBy("partition_key"))
    return [{"partition_key": r.partition_key, "source_url": r.source_url} 
            for r in df.collect()]

def claim_partitions(source_name: str, partition_keys: list, layer: str):
    now = _utcnow()
    if layer == "bronze":
        status_col, started_col = "status", "started_at"
    elif layer == "silver":
        status_col, started_col = "silver_status", "silver_started_at"
    else:
        raise ValueError(f"Unknown layer: {layer}")
    
    control = DeltaTable.forName(spark, CONTROL_TABLE)
    update_df = spark.createDataFrame(
        [(source_name, pk, "running", now) for pk in partition_keys],
        ["source_name", "partition_key", "new_status", "new_started"]
    )
    (control.alias("t")
        .merge(update_df.alias("s"),
               "t.source_name = s.source_name AND t.partition_key = s.partition_key")
        .whenMatchedUpdate(set={
            status_col: "s.new_status",
            started_col: "s.new_started",
        })
        .execute())


def update_partition_status(
    source_name: str, 
    partition_key: str, 
    layer: str,
    status: str, 
    rows_written: int = None,
    error_message: str = None
):
    update_partition_status_batch(
        source_name,
        [{"partition_key": partition_key, "status": status,
          "rows_written": rows_written, "error_message": error_message}],
        layer
    )

def update_partition_status_batch(source_name: str, updates: list[dict], layer: str):
    """updates is a list of dicts:
    {partition_key, status, rows_written, error_message}"""
    if not updates:
        return
    
    now = _utcnow()
    if layer == "bronze":
        status_col, ended_col, rows_col, err_col = "status", "ended_at", None, "error_message"
    elif layer == "silver":
        status_col, ended_col, rows_col, err_col = (
                    "silver_status", "silver_ended_at", "silver_rows_written", "silver_error_message"
                )
    else:
        raise ValueError(f"Unknown layer: {layer}")
    
    rows = []
    for u in updates:
        rows.append((
            source_name,
            u["partition_key"],
            u["status"],
            now,
            u.get("rows_written"),
            (u.get("error_message") or "")[:1000] if u.get("error_message") else None,
        ))
    
    update_df = spark.createDataFrame(
        rows,
        ["source_name", "partition_key", "new_status", "new_ended", "new_rows", "new_err"]
    )
    
    set_clause = {
        status_col: "s.new_status",
        ended_col: "s.new_ended",
        err_col: "s.new_err",
    }
    if rows_col:
        set_clause[rows_col] = "s.new_rows"
    
    control = DeltaTable.forName(spark, CONTROL_TABLE)
    (control.alias("t")
        .merge(update_df.alias("s"),
               "t.source_name = s.source_name AND t.partition_key = s.partition_key")
        .whenMatchedUpdate(set=set_clause)
        .execute())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
