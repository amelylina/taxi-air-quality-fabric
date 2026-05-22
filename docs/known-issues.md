# Known Issues and Future Work

## Known limitations of the current submission

### Data engineering scope

- **Per-partition silver status only for taxi.** GDP, FX, and OpenAQ silver use the watermark mechanism, which is simpler and idempotent via MERGE but doesn't expose per-month failure detail. Acceptable for small/slowly-changing or merge-keyed sources; a future iteration could unify on partition-status everywhere.

- **`stg.*` schema lives in `lh_silver`, not `wh_gold`.** Deliberate (see [docs/architecture.md](docs/architecture.md)), but it means the staging area for gold is technically in the silver lakehouse.

- **No quarantine table for rejected rows.** Silver DQ filters drop bad rows silently. Counts are logged via pipeline message but the rejected rows themselves are not persisted. Adding a `lh_silver.dq.rejected_*` table per silver source is a straightforward extension.

### Source-specific limitations

- **OpenAQ ingestion uses two complementary paths.** Daily measurements come from the rate-limited API (50 req/min free tier); hourly measurements come from the unauthenticated S3 archive. This is by design — see [docs/architecture.md](docs/architecture.md).

- **OpenAQ NYC sensor coverage is sparse.** After spatial-joining sensors to TLC zones, only a handful of Manhattan-core zones have any sensor across the study window. **NO2 has zero in-zone sensors**; PM2.5 has the best coverage. The pipeline correctly drops out-of-zone sensors via the `openaq_sensor_zones` join. This source-side coverage limitation is surfaced in the report overview.

- **Air quality `avg_value` is a coverage-weighted average across sensor-days/hours rather than a strict time-weighted average.** 

- **24-month study window limits Q4 long-term trend claims.** Multi-year comparisons in the report are directional, not statistically robust.

- **GDP has a 1-2 year publication lag.**

- **Hourly bronze drops `min_val`/`max_val`/`median_val`/`coverage_pct`** because at hourly grain from the S3 archive these are degenerate (~99.9% of cells have a single reading).

- **Latent S3 missing-partition bug was found and fixed during hourly development.** Both daily and hourly S3 notebooks now pre-check path existence via `FileSystem.exists()` .

- **Currency dimension is scope-limited.** `dm_currency` is manually seeded with USD and EUR only, matching the project's analytical scope. To support more currencies, derive from `lh_silver.dbo.fx_daily` distinct values.

### Orchestration

- **OpenAQ API key in Power Query dataflow.** Could be solved via Key Vault → Web Activity → Dataflow parameter pattern in a pipeline, unfortunately the limitations of DataFlow web connector made it impossible to successfully do this in my test runs.

- **`pl_seed` Spark concurrency.** Ideally I would have a single pipeline that would run the seedings parallel to each other for speed, but the Fabric Trial F4 SKU has it's limitations. It is still included in the commited project.

### BI

- **Power BI report lacks certain analytical measures.** For statistical correlation analysis between mobility and air quality, additional DAX measures (e.g., Pearson correlation across the trips × pollutant series) would be needed. The current report shows visual correlation only.

## Out-of-scope (explicitly skipped, marked optional in brief)

- Purview lineage (the in-warehouse lineage documented here is the substitute).
- Row-Level Security in Power BI.
- Production refresh schedule (the platform is schedule-ready but unscheduled for evaluation determinism).

## Future work

- Materialise rejected silver rows to a `dq.rejected_*` quarantine table with reason codes.
- Replace manually-seeded dims with data-driven seeds where appropriate (e.g. `dm_currency` from `fx_daily` distinct values).
- Add Great Expectations or Deequ-style assertions to silver notebooks.