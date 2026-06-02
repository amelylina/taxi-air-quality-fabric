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
# META       "environmentId": "4bea4ce7-d8b3-a131-4714-4320560007cd",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

import great_expectations as gx
from datetime import datetime, timezone
import requests

BOT_TOKEN = mssparkutils.credentials.getSecret(
    "https://fabric-project.vault.azure.net/",
    "telegram-bot-token",
)
CHAT_ID = mssparkutils.credentials.getSecret(
    "https://fabric-project.vault.azure.net/",
    "telegram-chat-id",
)

CHECKS = {
    "lh_silver.dbo.taxi_trips": [
        ("passenger_count between 1 and 6",
         gx.expectations.ExpectColumnValuesToBeBetween(column="passenger_count", min_value=1, max_value=6)),
        ("fare_amount >= 0",
         gx.expectations.ExpectColumnValuesToBeBetween(column="fare_amount", min_value=0, max_value=None)),
        ("total_amount >= 0",
         gx.expectations.ExpectColumnValuesToBeBetween(column="total_amount", min_value=0, max_value=None)),
        ("trip_distance between 0.01 and 100",
         gx.expectations.ExpectColumnValuesToBeBetween(column="trip_distance", min_value=0.01, max_value=100)),
        ("pulocationid not null",
         gx.expectations.ExpectColumnValuesToNotBeNull(column="pulocationid")),
        ("dolocationid not null",
         gx.expectations.ExpectColumnValuesToNotBeNull(column="dolocationid")),
        ("trip_duration_min between 1 and 720",
         gx.expectations.ExpectColumnValuesToBeBetween(column="trip_duration_min", min_value=1, max_value=720)),
        ("payment_type in known set",
         gx.expectations.ExpectColumnValuesToBeInSet(column="payment_type", value_set=[1,2,3,4,5,6])),
    ],
    "lh_silver.dbo.openaq_measurements": [
        ("value >= 0",
         gx.expectations.ExpectColumnValuesToBeBetween(column="value", min_value=0, max_value=None)),
        ("coverage_pct >= 75",
         gx.expectations.ExpectColumnValuesToBeBetween(column="coverage_pct", min_value=75, max_value=100)),
        ("sensor_id not null",
         gx.expectations.ExpectColumnValuesToNotBeNull(column="sensor_id")),
        ("measurement_date not null",
         gx.expectations.ExpectColumnValuesToNotBeNull(column="measurement_date")),
        ("parameter in known set",
         gx.expectations.ExpectColumnValuesToBeInSet(
             column="parameter",
             value_set=["pm25","no2","o3","co","pm10","pm1","so2","no","nox","temperature","relativehumidity","um003"])),
    ],
    "lh_silver.dbo.openaq_measurements_hourly": [
        ("value >= 0",
         gx.expectations.ExpectColumnValuesToBeBetween(column="value", min_value=0, max_value=None)),
        ("hour_utc between 0 and 23",
         gx.expectations.ExpectColumnValuesToBeBetween(column="hour_utc", min_value=0, max_value=23)),
        ("sensor_id not null",
         gx.expectations.ExpectColumnValuesToNotBeNull(column="sensor_id")),
        ("parameter in known set",
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="parameter",
            value_set=["pm25","no2","o3","co","pm10","pm1","so2","no","nox","temperature","relativehumidity","um003"]))
    ],
    "lh_silver.dbo.fx_daily": [
        ("rate > 0",
         gx.expectations.ExpectColumnValuesToBeBetween(column="rate", min_value=0, max_value=None, strict_min=True)),
        ("rate_date not null",
         gx.expectations.ExpectColumnValuesToNotBeNull(column="rate_date")),
        ("from_currency in set",
         gx.expectations.ExpectColumnValuesToBeInSet(column="from_currency", value_set=["USD"])),
        ("to_currency in set",
         gx.expectations.ExpectColumnValuesToBeInSet(column="to_currency", value_set=["EUR"])),
    ],
    "lh_silver.dbo.gdp_yearly": [
        ("gdp_usd > 0",
         gx.expectations.ExpectColumnValuesToBeBetween(column="gdp_usd", min_value=0, max_value=None, strict_min=True)),
        ("year not null",
         gx.expectations.ExpectColumnValuesToNotBeNull(column="year")),
        ("country_code not null",
         gx.expectations.ExpectColumnValuesToNotBeNull(column="country_code")),
    ],
    "lh_silver.dbo.openaq_sensor_zones": [
        ("sensor_id not null",
         gx.expectations.ExpectColumnValuesToNotBeNull(column="sensor_id")),
    ],
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def run_checks(checks: dict)-> list[dict]:
    context = gx.get_context()

    source = context.data_sources.add_spark(name="fabric_spark")
    out = []

    for table, expectations in checks.items():
        df = spark.read.table(table)
        row_count = df.count()

        asset = source.add_dataframe_asset(name=table.replace(".", "_"))
        batch_def = asset.add_batch_definition_whole_dataframe("batch")
        batch = batch_def.get_batch(batch_parameters={"dataframe": df})

        suite = gx.ExpectationSuite(name=f"{table}_suite")
        for _, exp in expectations:
            suite.add_expectation(exp)

        result = batch.validate(suite)

        check_results = []
        for label,r in zip([lbl for lbl, _ in expectations], result.results):
            check_results.append({
                "label" : label,
                "success" : r.success,
                "unexpected_count": r.result.get("unexpected_count"),
                "unexpected_percent": r.result.get("unexpected_percent"),
            })

        out.append({
            "table": table,
            "row_count": row_count,
            "overall_success": result.success,
            "checks": check_results,
        })

    return out

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def format_header() -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return f"DATA QUALITY REPORT - {now}"

def format_table_block(r: dict) -> str:
    passed = sum(1 for c in r["checks"] if c["success"])
    n = len(r["checks"])
    icon = "✅" if r["overall_success"] else "⚠️"
    short = r["table"].split(".")[-1]

    lines = [f"{icon} {short.upper()} ({passed}/{n} checks, {r['row_count']:,} rows)"]
    for c in r["checks"]:
        mark = "✓" if c["success"] else "✗"
        detail = ""
        if not c["success"] and c.get("unexpected_count") is not None:
            detail = f" - {c['unexpected_count']} bad ({c['unexpected_percent']:.2f}%)"
        lines.append(f"  {mark} {c['label']}{detail}")
    return "\n".join(lines)

def format_footer(results: list[dict]) -> str:
    total_checks = sum(len(r["checks"]) for r in results)
    total_passed = sum(sum(1 for c in r["checks"] if c["success"]) for r in results)
    status = "ALL PASSED" if total_passed == total_checks else "FAILURES DETECTED"
    return f"OVERALL: {total_passed}/{total_checks} - {status}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def notify_telegram(text: str, parse_mode: str = "Markdown") -> dict:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    resp = requests.post(
        url,
        json={
            "chat_id": CHAT_ID,
            "text": text,
            "disable_web_page_preview": True,
        },
        timeout=30,
    )
    if not resp.ok:
        print(f"Telegram error {resp.status_code}: {resp.text}")
    return resp.json()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

results = run_checks(CHECKS)

notify_telegram(format_header())
for r in results:
    notify_telegram(format_table_block(r))
notify_telegram(format_footer(results))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
