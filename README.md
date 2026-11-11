# Air Quality Data Pipeline

A production-grade Medallion Architecture data pipeline built with Apache Spark and Delta Lake on Databricks, ingesting manually landed batch files of OpenAQ air quality sensor data to analyze pollution trends across Berlin (DE), Toronto (CA) and New York (US).

## Architecture Overview

The pipeline implements a Lakehouse Medallion architecture (Landing → Bronze → Silver → Gold, with an audit log):

```
┌────────────────────────────────────────────────────────────────────────┐
│                        1. MANUAL LANDING ZONE                          │
│   • Manual batch files copied in by 00_setup_landing_data.py           │
│   • Landing Volume: /Volumes/workspace/air_quality/landing/raw/        │
│   • Batch partitioned: full_load/batch_xxxx, incremental/batch_yyyymmdd │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                           2. BRONZE LAYER                              │
│   • Notebook: 01_raw_to_bronze.py                                      │
│   • Table: workspace.air_quality.bronze_readings                       │
│   • Strict Schema-on-Read (StructType, NO inferSchema)                 │
│   • Audit tracking: source_file, batch_id, load_type, load_timestamp   │
│   • Schema drift auto-evolution (mergeSchema = true)                   │
│   • Corrupt record capture (_corrupt_record)                           │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                           3. SILVER LAYER                              │
│   • Notebook: 02_bronze_to_silver.py                                   │
│   • Readings: workspace.air_quality.silver_readings                    │
│   • Stations: workspace.air_quality.silver_stations                    │
│   • Quarantine: workspace.air_quality.silver_quarantine                │
│   • Flattening, strict type casting (try_cast), null/range validation  │
│   • Standardized pollutant units (ppm/ppb converted to µg/m³)          │
│   • In-batch deduplication on (sensor_id, datetime_from_utc)           │
│   • Idempotent upsert via Delta Lake MERGE INTO                        │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                            4. GOLD LAYER                               │
│   • 03_silver_to_gold.py: star schema (dim_date, dim_location,         │
│     dim_parameter, fact_measurements) via deterministic xxhash64 keys  │
│   • 04_gold_aggregates_and_validation.py: daily KPI aggregate, WHO     │
│     exceedance view, gap-free calendar, validation checks              │
│   • gold_to_powerbi.py: CSV export -> Power BI semantic model          │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                       5. AUDIT & EXECUTION LOGS                        │
│   • Table: workspace.air_quality.pipeline_execution_logs               │
│   • Tracks layer, batch_id, start/end time, status, rows read/inserted │
└────────────────────────────────────────────────────────────────────────┘
```

---

## Directory Structure

```
├── data/
│   ├── raw/
│   │   ├── full_load/            # 10 baseline batches (batch_0001 to batch_0010, 13,000 records)
│   │   └── incremental/          # 3 daily batches (batch_20261001 to batch_20261003, 378 records)
│   ├── full_load/                # Baseline payload sample
│   └── incremental_load/         # Incremental payload sample
├── notebooks/
│   ├── 00_config_helpers.py      # Shared config, explicit StructType schema, audit logging, batch helpers
│   ├── 00_setup_landing_data.py  # Copies repo batch files into the landing Volume
│   ├── 01_raw_to_bronze.py       # Ingests landing batch folders into Bronze Delta table with drift handling
│   ├── 02_bronze_to_silver.py    # Flattens, validates, quarantines, converts units, and MERGEs into Silver
│   ├── 03_silver_to_gold.py      # Star schema (3 dims + fact), idempotent MERGE, referential-integrity check
│   ├── 04_gold_aggregates_and_validation.py  # Gap-free calendar, daily KPI aggregate, WHO view, validation checks
│   └── gold_to_powerbi.py        # Exports Gold tables as CSV for Power BI
├── docs/GOLD_AND_POWERBI_GUIDE.md  # Gold model, DAX measures, dashboard spec, evaluation checklist
└── README.md                     # Data models, architecture documentation, and run guide
```

---

## Data Models & Data Dictionary

### 1. `workspace.air_quality.bronze_readings`
- **Description**: Raw append-only Delta table preserving the source schema as strings, tracking file origin and load timestamps.
- **Primary / Natural Key**: `(_sensor_id, period.datetimeFrom.utc)`
- **Partition Key**: `ingestion_date`

| Column Name | Data Type | Nullable | Description |
|---|---|---|---|
| `value` | `STRING` | Yes | Raw pollutant measurement value as reported by the sensor |
| `flagInfo` | `STRUCT<hasFlags: STRING>` | Yes | Flag indicator for invalid or disputed sensor readings |
| `parameter` | `STRUCT<id: STRING, name: STRING, units: STRING, displayName: STRING>` | Yes | Pollutant identifier (`pm25`, `o3`, `no2`, `co`, `so2`, `pm10`), units, and display name |
| `period` | `STRUCT<label: STRING, interval: STRING, datetimeFrom: STRUCT<utc: STRING, local: STRING>, datetimeTo: STRUCT<utc: STRING, local: STRING>>` | Yes | Aggregation window (e.g. 1-hour interval) with UTC and local timestamps |
| `coordinates` | `STRUCT<latitude: STRING, longitude: STRING>` | Yes | Geographic coordinates of the sensor station |
| `summary` | `STRUCT<min: STRING, q02: STRING, q25: STRING, median: STRING, q75: STRING, q98: STRING, max: STRING, avg: STRING, sd: STRING>` | Yes | Statistical summary values for the measurement window |
| `coverage` | `STRUCT<expectedCount: STRING, expectedInterval: STRING, observedCount: STRING, observedInterval: STRING, percentComplete: STRING, percentCoverage: STRING, datetimeFrom: STRUCT<...>, datetimeTo: STRUCT<...>>` | Yes | Sampling coverage percentages and observation completeness |
| `_sensor_id` | `STRING` | Yes | Unique identifier of the sensor device |
| `_location_id` | `STRING` | Yes | Unique identifier of the monitoring station |
| `_location_name` | `STRING` | Yes | Human-readable name of the monitoring station |
| `_city` | `STRING` | Yes | City or locality of the station |
| `_country` | `STRING` | Yes | Two-letter ISO country code (`DE`, `CA`, `US` in the supplied data) |
| `_parameter` | `STRING` | Yes | Redundant top-level parameter string |
| `_unit` | `STRING` | Yes | Redundant top-level unit string |
| `_corrupt_record` | `STRING` | Yes | Unparseable raw JSON text isolated by `PERMISSIVE` mode |
| `source_file` | `STRING` | No | Originating file URI in the landing volume (`_metadata.file_path`) |
| `batch_id` | `STRING` | No | Folder name of the batch processed (e.g. `batch_0001`, `batch_20261001`) |
| `load_type` | `STRING` | No | Ingestion category (`full_load` or `incremental`) |
| `ingestion_date` | `DATE` | No | Partition date on ingestion (`current_date()`) |
| `load_timestamp` | `TIMESTAMP` | No | Exact timestamp when the record was written into Bronze |

---

### 2. `workspace.air_quality.silver_readings`
- **Description**: Cleaned, strongly typed, deduplicated, and unit-standardized air quality observations.
- **Primary Key**: `(sensor_id, datetime_from_utc)`

| Column Name | Data Type | Nullable | Description |
|---|---|---|---|
| `sensor_id` | `BIGINT` | No | Primary identifier for the sensor device |
| `location_id` | `BIGINT` | No | Identifier for the station location |
| `location_name` | `STRING` | Yes | Name of the air quality station |
| `city` | `STRING` | Yes | City or district of the station |
| `country` | `STRING` | Yes | Country ISO code |
| `parameter_id` | `BIGINT` | Yes | OpenAQ parameter ID |
| `parameter_name` | `STRING` | Yes | Standard pollutant name (`pm25`, `pm10`, `o3`, `no2`, `co`, `so2`) |
| `parameter_display_name` | `STRING` | Yes | Human-friendly parameter label |
| `datetime_from_utc` | `TIMESTAMP` | No | Window start timestamp in UTC (Primary Key component) |
| `datetime_from_local` | `STRING` | Yes | Window start timestamp in station local time |
| `datetime_to_utc` | `TIMESTAMP` | Yes | Window end timestamp in UTC |
| `datetime_to_local` | `STRING` | Yes | Window end timestamp in station local time |
| `value_raw` | `DOUBLE` | Yes | Measurement value as reported originally |
| `unit_raw` | `STRING` | Yes | Original measurement unit (`ppm`, `ppb`, `µg/m³`) |
| `value_ugm3` | `DOUBLE` | Yes | Standardized pollutant concentration converted to `µg/m³` |
| `unit_std` | `STRING` | Yes | Standardized unit label (`µg/m³`) |
| `has_flags` | `BOOLEAN` | Yes | Boolean flag indicating whether the sensor reading was flagged |
| `period_label` | `STRING` | Yes | Measurement window interval label (`1hour`) |
| `period_interval` | `STRING` | Yes | Window duration string (`01:00:00`) |
| `latitude` | `DOUBLE` | Yes | Station geographic latitude |
| `longitude` | `DOUBLE` | Yes | Station geographic longitude |
| `summary_min` | `DOUBLE` | Yes | Period minimum concentration |
| `summary_max` | `DOUBLE` | Yes | Period maximum concentration |
| `summary_avg` | `DOUBLE` | Yes | Period average concentration |
| `summary_median` | `DOUBLE` | Yes | Period median concentration |
| `coverage_pct` | `DOUBLE` | Yes | Percentage of valid observation coverage (0.0 to 100.0) |
| `batch_id` | `STRING` | No | Batch folder identifier from which the row originated |
| `load_type` | `STRING` | No | `full_load` or `incremental` |
| `load_timestamp` | `TIMESTAMP` | No | Timestamp when the row was merged into Silver |

---

### 3. `workspace.air_quality.silver_stations`
- **Description**: Station dimension table capturing physical coordinates and station attributes.
- **Primary Key**: `(sensor_id, location_id)`

| Column Name | Data Type | Nullable | Description |
|---|---|---|---|
| `sensor_id` | `BIGINT` | No | Sensor identifier |
| `location_id` | `BIGINT` | No | Station location identifier |
| `location_name` | `STRING` | Yes | Name of the station |
| `city` | `STRING` | Yes | City location |
| `country` | `STRING` | Yes | Country code |
| `latitude` | `DOUBLE` | Yes | Geographic latitude |
| `longitude` | `DOUBLE` | Yes | Geographic longitude |
| `batch_id` | `STRING` | No | Ingested batch identifier |
| `load_type` | `STRING` | No | Load category |
| `load_timestamp` | `TIMESTAMP` | No | Timestamp of the station dimension upsert |

---

### 4. `workspace.air_quality.silver_quarantine`
- **Description**: Isolated non-conforming, corrupt, or logically invalid records rejected from Silver.
- **Primary Key**: Surrogate / Composite `(batch_id, _sensor_id, datetime_from_utc, load_timestamp)`

| Column Name | Data Type | Description |
|---|---|---|
| `batch_id` | `STRING` | Batch where the rejected record appeared |
| `load_type` | `STRING` | `full_load` or `incremental` |
| `_sensor_id` | `STRING` | Sensor ID as string (may be null) |
| `_location_id` | `STRING` | Location ID as string |
| `_parameter` | `STRING` | Parameter name |
| `datetime_from_utc` | `STRING` | UTC timestamp string |
| `value_raw` | `STRING` | Raw uncastable value |
| `quarantine_reason` | `STRING` | Detailed failure cause (e.g. `_corrupt_record is not null`, `null key field`, `negative value`) |
| `raw_json` | `STRING` | Full unparseable JSON payload |
| `load_timestamp` | `TIMESTAMP` | Quarantine insertion timestamp |

---

### 5. `workspace.air_quality.pipeline_execution_logs`
- **Description**: Dedicated audit logging table recording operational metrics for every execution step across all layers.
- **Primary Key**: `(run_id, batch_id, layer)`

| Column Name | Data Type | Description |
|---|---|---|
| `run_id` | `STRING` | Unique execution run ID (`YYYYMMDDHHMMSS_uuid`) |
| `batch_id` | `STRING` | Name of the batch processed |
| `layer` | `STRING` | Pipeline stage (`Raw-to-Bronze`, `Bronze-to-Silver`, `Silver-to-Gold`) |
| `load_type` | `STRING` | `full_load` or `incremental` |
| `run_mode` | `STRING` | `pending` or `backfill` |
| `parameters` | `STRING` | JSON string of execution widgets/parameters |
| `source_path` | `STRING` | Source URI or table predicate processed |
| `start_time` | `TIMESTAMP` | UTC start timestamp of the step |
| `end_time` | `TIMESTAMP` | UTC end timestamp of the step |
| `status` | `STRING` | `SUCCESS` or `FAILURE` |
| `rows_read` | `BIGINT` | Total input rows read |
| `rows_inserted` | `BIGINT` | New rows written to the target table |
| `rows_updated` | `BIGINT` | Existing rows updated via MERGE or replace |
| `rows_quarantined` | `BIGINT` | Bad rows routed to quarantine |
| `notes` | `STRING` | Descriptive audit messages (e.g., schema drift discovered, idempotent re-run) |
| `error_message` | `STRING` | Captured error message on failure |
| `load_timestamp` | `TIMESTAMP` | Log record write timestamp |

---

## Gold Layer Data Model (Star Schema)

```
                 gold_dim_date                gold_dim_parameter
                 (date_key PK)                (parameter_key PK)
                       \                          /
                        \                        /
                      gold_fact_measurements  (measurement_key PK)
                        /                        \
                       /                          \
              gold_dim_location          gold_agg_daily_city_parameter
              (location_key PK)          (date_key, city, parameter_key)
```

| Table | Grain | Key | Rows come from |
|---|---|---|---|
| `gold_dim_date` | one row per calendar day (gap-free) | `date_key` = `yyyyMMdd` | `03` + `04` |
| `gold_dim_location` | one row per `(sensor_id, location_id)` | `location_key` = `xxhash64(sensor_id, location_id)` | `03` |
| `gold_dim_parameter` | one row per pollutant | `parameter_key` = `xxhash64(parameter_id, lower(name))` | `03` |
| `gold_fact_measurements` | one row per Silver reading (`sensor_id`, `datetime_from_utc`) | `measurement_key` = `xxhash64(sensor_id, datetime, parameter)` | `03` |
| `gold_agg_daily_city_parameter` | `(date, city, country, pollutant)` | composite | `04` |
| `gold_vw_who_exceedance` | view over the aggregate, daily vs WHO 2021 guideline | - | `04` |

**Design choices:** deterministic hash surrogate keys mean dimensions and facts agree without lookups and reruns never
create duplicates; dimensions are merged before the fact so foreign keys always resolve; flagged readings are
excluded from KPI averages but counted in `flagged_readings`.

**Run order:** `03_silver_to_gold` (full_load, then incremental) -> `04_gold_aggregates_and_validation` -> `gold_to_powerbi`.

See [`docs/GOLD_AND_POWERBI_GUIDE.md`](docs/GOLD_AND_POWERBI_GUIDE.md) for the Power BI model, DAX measures and evaluation checklist.

---

## Environment Setup & Execution Guide

### 1. Workspace Configuration & Git Sync
1. Open Databricks and navigate to **Workspace**.
2. Select **Create** > **Git folder**.
3. Repository URL: `https://github.com/aliyahrasheed/air-quality-pipeline-`
4. Confirm that the folder contains notebooks `00_config_helpers`, `00_setup_landing_data`, `01_raw_to_bronze`, `02_bronze_to_silver`, `03_silver_to_gold`, and `04_gold_aggregates_and_validation`.

### 2. Unity Catalog Schema & Volume Setup
Run once in a Databricks SQL query or cluster:
```sql
USE CATALOG workspace;
CREATE SCHEMA IF NOT EXISTS air_quality;
CREATE VOLUME IF NOT EXISTS workspace.air_quality.landing;
```
The root landing volume will be: `/Volumes/workspace/air_quality/landing/raw`

### 3. Landing the Raw Data
The landing directory is populated manually from the batch files in `data/raw/`:

#### Manual Raw Upload (Full-Load and Incremental Batches)
1. Compress your `data/raw/` folder into `raw.zip`.
2. Upload `raw.zip` to `/Volumes/workspace/air_quality/landing/`.
3. In a notebook cell, extract the archive:
   ```python
   import zipfile
   base = "/Volumes/workspace/air_quality/landing"
   with zipfile.ZipFile(f"{base}/raw.zip") as z:
       z.extractall(base)
   ```
4. Verify files exist under `/Volumes/workspace/air_quality/landing/raw/full_load/` and `.../raw/incremental/`.


---

### 4. Running Bronze Ingestion (`01_raw_to_bronze.py`)

#### A. Standard Full Load (Baseline)
- Set widgets:
  - `load_type`: `full_load`
  - `run_mode`: `pending`
  - `start_batch`: `""`
  - `end_batch`: `""`
- Execute **Run All**: Ingests all 10 full-load batches (13,000 records).

#### B. Standard Incremental Load
- Set widgets:
  - `load_type`: `incremental`
  - `run_mode`: `pending`
- Execute **Run All**: Ingests pending incremental batches (378 records).

#### C. Backfill an Older Period
- Set widgets:
  - `load_type`: `full_load`
  - `run_mode`: `backfill`
  - `start_batch`: `batch_0003`
  - `end_batch`: `batch_0005`
- Execute **Run All**: Re-processes only `batch_0003`, `batch_0004`, and `batch_0005` idempotently via `replaceWhere`, recording `rows_updated = N, rows_inserted = 0`.

---

### 5. Running Silver Transformations (`02_bronze_to_silver.py`)

#### A. Full Load Promotion
- Set `load_type` = `full_load`, `run_mode` = `pending`.
- Execute **Run All**: Flattens, cleans, standardizes units, and MERGEs into `silver_readings` and `silver_stations`.

#### B. Incremental Promotion
- Set `load_type` = `incremental`, `run_mode` = `pending`.
- Execute **Run All**: Promotes only newly arrived bronze batches.

#### C. Idempotency Test (Duplicate Prevention)
- Set `run_mode` = `backfill`, `start_batch` = `batch_0001`, `end_batch` = `batch_0001`.
- Execute **Run All**: The MERGE statement finds all keys match:
  - Total row count remains **13,378**.
  - Log records `rows_inserted = 0`, `rows_updated = 169`.
  - Zero duplicate records are generated.

---

## Technical Features & Rubric Compliance

### 1. Strict Schema-on-Read (No `inferSchema`)
The schema is defined explicitly in [`00_config_helpers.py`](notebooks/00_config_helpers.py) using PySpark `StructType` and `StructField`. All leaf fields are ingested as strings in Bronze to guarantee that unexpected format shifts never break the raw ingestion layer.

### 2. Schema Drift Handling
- **Discovery**: In `01_raw_to_bronze.py`, the function `discover_new_keys()` inspects incoming JSON keys without triggering schema inference.
- **Auto-evolution**: Any newly detected columns are appended dynamically to the read schema, Delta-sanitized with `safe_col_name()`, and absorbed using `.option("mergeSchema", "true")`.

### 3. Data Cleansing, Unit Standardization & Quarantine
- **Type Casting**: In `02_bronze_to_silver.py`, fields are safely cast using `try_cast(...)`.
- **Unit Standardization**: The UDF `to_ugm3()` converts gas pollutants reported in `ppm` or `ppb` (`no2`, `o3`, `so2`, `co`, `no`, `nox`) to standard micrograms per cubic meter (`µg/m³`) using molecular weight gas constants at standard temperature and pressure.
- **Quarantine Routing**: Records with `_corrupt_record IS NOT NULL`, null primary keys, or negative values are routed to `silver_quarantine` along with a documented `quarantine_reason`.

### 4. Idempotency via Delta `MERGE INTO`
Silver updates use Delta Lake's native `merge()` API:
```python
dt.alias("tgt").merge(
    silver_df.alias("src"),
    "tgt.sensor_id = src.sensor_id AND tgt.datetime_from_utc = src.datetime_from_utc"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```
Repeated executions update existing records rather than duplicating them.

### 5. Execution Logs Verification
Every execution across all layers writes a structured audit log. Inspect the log table with:
```sql
SELECT 
    run_id, batch_id, layer, load_type, run_mode, status, 
    rows_read, rows_inserted, rows_updated, rows_quarantined, 
    start_time, end_time, notes
FROM workspace.air_quality.pipeline_execution_logs
ORDER BY start_time DESC;
```
