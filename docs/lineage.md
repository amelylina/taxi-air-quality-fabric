# Lineage

Two diagrams: **data lineage** showing source → bronze → silver → gold → BI flow per table, and **pipeline lineage** showing how the orchestration pipelines invoke each other. The data lineage is the "what flows where"; the pipeline lineage is the "what runs what".

## Data lineage

The platform processes four source families. Each is shown separately for clarity; the BI fan-in at the end shows how the gold layer feeds the semantic model.

### Taxi (mobility)
```mermaid
flowchart LR
    classDef src     fill:#e0f2fe,stroke:#0369a1,color:#0c4a6e
    classDef bronze  fill:#fef3c7,stroke:#a16207,color:#713f12
    classDef silver  fill:#e5e7eb,stroke:#4b5563,color:#1f2937
    classDef gold    fill:#fde68a,stroke:#a16207,color:#713f12
    SRC_TLC[NYC TLC<br/>Parquet HTTP]:::src
    SRC_SHP[NYC Zone<br/>Shapefile HTTP]:::src
    B_TAXI[Files/yellow_taxi/*]:::bronze
    B_SHP[taxi_zone_shapes]:::bronze
    S_TAXI[taxi_trips]:::silver
    STG_TD[stg.fct_taxi_daily]:::silver
    STG_TH[stg.fct_taxi_hourly]:::silver
    G_TD[fct_taxi_daily]:::gold
    G_TH[fct_taxi_hourly]:::gold
    D_ZONE[dm_zone]:::gold
    SRC_TLC --> B_TAXI --> S_TAXI
    S_TAXI --> STG_TD --> G_TD
    S_TAXI --> STG_TH --> G_TH
    SRC_SHP --> B_SHP --> D_ZONE
```
Hourly taxi is a re-aggregation of the same silver — no new ingestion. Zone shapefile feeds the gold dimension directly via `nb_gold_seed_dimensions` (the same shapefile separately also feeds the OpenAQ sensor↔zone join — see the air quality diagram).

### OpenAQ (air quality) — dual-path

```mermaid
flowchart LR
  classDef src     fill:#e0f2fe,stroke:#0369a1,color:#0c4a6e
  classDef bronze  fill:#fef3c7,stroke:#a16207,color:#713f12
  classDef silver  fill:#e5e7eb,stroke:#4b5563,color:#1f2937
  classDef gold    fill:#fde68a,stroke:#a16207,color:#713f12

  SRC_API[OpenAQ /sensors/daysAPI]:::src
  SRC_S3[OpenAQ S3 ArchiveCSV.GZ]:::src
  SRC_LOC[OpenAQ /locationsAPI]:::src
  SRC_SHP_R[NYC Zone Shapefile]:::src

  B_M[openaq_measurements_daily]:::bronze
  B_MH[openaq_measurements_hourly]:::bronze
  B_L[openaq_locations]:::bronze
  B_SH_R[taxi_zone_shapes]:::bronze

  S_M[openaq_measurements]:::silver
  S_MH[openaq_measurements_hourly]:::silver
  S_Z[openaq_sensor_zones]:::silver
  STG_AQ[stg.fct_air_quality]:::silver
  STG_AQH[stg.fct_air_quality_hourly]:::silver

  G_AQ[fct_air_quality]:::gold
  G_AQH[fct_air_quality_hourly]:::gold
  D_AIR[dm_air_measurement]:::gold

  SRC_API --> B_M
  SRC_S3 --> B_M
  SRC_S3 --> B_MH
  SRC_SHP_R --> B_SH_R

  B_M --> S_M --> STG_AQ --> G_AQ
  B_MH --> S_MH --> STG_AQH --> G_AQH

  SRC_LOC --> B_L
  B_L --> S_Z
  B_SH_R -.-> S_Z
  S_Z -.->|sensor-zonelookup| STG_AQ
  S_Z -.->|sensor-zonelookup| STG_AQH

  D_AIR -.->|parameterlookup| G_AQ
  D_AIR -.->|parameterlookup| G_AQH
```

Solid arrows are data flow; dotted arrows are join-key lookups. The S3 archive serves both daily and hourly bronze (daily for parameter backfill, hourly for time-of-day analysis). The API path feeds only daily bronze (rate limits make hourly impractical at scale). `openaq_sensor_zones` is built from two inputs and used as a lookup, not aggregated into a fact.

### Economy

```mermaid
flowchart LR
  classDef src     fill:#e0f2fe,stroke:#0369a1,color:#0c4a6e
  classDef bronze  fill:#fef3c7,stroke:#a16207,color:#713f12
  classDef silver  fill:#e5e7eb,stroke:#4b5563,color:#1f2937
  classDef gold    fill:#fde68a,stroke:#a16207,color:#713f12

  SRC_WB[World Bank GDPJSON]:::src
  SRC_ECB[ECB FXCSV]:::src

  B_GDP[worldbank_gdp]:::bronze
  B_FX[ecb_fx_daily]:::bronze

  S_GDP[gdp_yearly]:::silver
  S_FX[fx_daily]:::silver
  STG_GDP[stg.fct_gdp_yearly]:::silver
  STG_FX[stg.fct_fx_daily]:::silver

  G_GDP[fct_gdp_yearly]:::gold
  G_FX[fct_fx_daily]:::gold
  D_CUR[dm_currency]:::gold

  SRC_WB --> B_GDP --> S_GDP --> STG_GDP --> G_GDP
  SRC_ECB --> B_FX --> S_FX --> STG_FX --> G_FX

  D_CUR -.->|currencylookup| G_FX
```

GDP runs as part of `pl_seed`, not the recurring masters — the World Bank endpoint returns full history per call, so refreshing daily/weekly is wasteful. FX silver applies a date-spine forward-fill so weekend/holiday gaps don't break downstream joins on `date_key`.

### BI fan-in

```mermaid
flowchart LR
  classDef gold    fill:#fde68a,stroke:#a16207,color:#713f12
  classDef model   fill:#ddd6fe,stroke:#5b21b6,color:#4c1d95

  G_TD[wh_gold.dbo]:::gold

  SM[Semantic Model]:::model
  RPT[Power BI Report]:::model

  G_TD --> SM

  SM --> RPT
```
Right now only 1 dimension: `dm_vendor` is not included into semantic model, as it doesn't provide any analytical value to the current project.

## Pipeline lineage

```mermaid
flowchart LR
  classDef pipe    fill:#e0e7ff,stroke:#3730a3,color:#312e81
  classDef seed    fill:#fee2e2,stroke:#991b1b,color:#7f1d1d
  classDef master  fill:#dcfce7,stroke:#15803d,color:#14532d

  PSEED[pl_seed]:::seed

  MTAXI[pl_master_taxi]:::master
  MOAD[pl_master_openaq_daily]:::master
  MOAH[pl_master_openaq_hourly]:::master
  MFX[pl_master_fx]:::master

  PBT[pl_bronze_taxi_trips]:::pipe
  PBOD[pl_bronze_openaq_sensor_days]:::pipe
  PBOH[pl_bronze_openaq_sensor_hours]:::pipe
  DFFX[df_bronze_fx_daily]:::pipe
  DFGDP[df_bronze_gdp_US]:::pipe
  DFLOC[df_bronze_openaq_locations]:::pipe

  PST[pl_silver_taxi_trips]:::pipe
  PSO[pl_silver_openaq_sensor_days]:::pipe
  PSOH[pl_silver_openaq_sensor_hours]:::pipe
  PSFX[pl_silver_fx_daily]:::pipe
  PSGDP[pl_silver_gdp_yearly]:::pipe

  PGT[pl_gold_taxi_trips]:::pipe
  PGTH[pl_gold_taxi_hourly]:::pipe
  PGO[pl_gold_openaq_sensor_days]:::pipe
  PGOH[pl_gold_openaq_sensor_hours]:::pipe
  PGFX[pl_gold_fx_daily]:::pipe
  PGGDP[pl_gold_gdp_yearly]:::pipe

  PSEED --> DFGDP --> PSGDP --> PGGDP
  PSEED --> DFLOC

  MTAXI --> PBT --> PST --> PGT
  PST --> PGTH

  MOAD --> PBOD --> PSO --> PGO
  MOAH --> PBOH --> PSOH --> PGOH
  MFX --> DFFX --> PSFX --> PGFX
```