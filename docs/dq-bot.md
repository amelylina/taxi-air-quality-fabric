# Data Quality Monitoring

On-demand data quality checks for the silver layer, triggered from Telegram and reported back to the same chat.

## What it does

This gives the project a quick, manual way to validate the silver-layer lakehouse tables without opening Fabric. From a Telegram chat you send a command, the checks run in Fabric against the current data, and a readable report comes back as chat messages - pass/fail counts per table, row counts, and details on anything that failed.

It's meant as a spot-check tool: run it after a pipeline finishes, before relying on the data downstream, or whenever you want confidence the silver tables look sane.

## How it works

There are two pieces:

1. **The bot** - a small Telegram bot (separate repo, [dq-bot](https://github.com/amelylina/fabric-dq-bot)) that listens for commands. On `/runchecks` it calls the Fabric REST API to start the data quality notebook as a job, and exposes `/status` to poll how that run is going. It's the control surface; it doesn't do any validation itself.

2. **The notebook** - a Fabric PySpark notebook (in `integrations/`) that does the actual work. It runs a set of [Great Expectations](https://greatexpectations.io/) validations over the silver tables, formats the results, and sends them straight to the Telegram chat using the same bot token.

So the flow is: you send `/runchecks` → the bot triggers the notebook in Fabric → the notebook validates the tables and messages the report back → you read it in Telegram. The bot triggers and tracks; the notebook validates and reports.

## What gets checked

The notebook validates the silver tables (taxi trips, OpenAQ measurements, FX rates, GDP, sensor zones) against expectations like value ranges, not-null constraints, and allowed value sets. Each report message marks a table ✅ or ⚠️ and lists which individual checks passed or failed, with bad-row counts for failures, plus an overall summary at the end.

The exact tables and rules live in the notebook itself - that's the place to look or edit when checks need to change.

## Setup notes

- The bot needs Fabric service-principal credentials and a Telegram bot token / authorized chat ID - see the [dq-bot](https://github.com/amelylina/fabric-dq-bot) repo for its `.env`.
- The notebook reads its Telegram token and chat ID from Azure Key Vault at runtime, so both sides post to the same chat.
- Access is locked to a single authorized chat, so only that chat can trigger runs or receive reports.
