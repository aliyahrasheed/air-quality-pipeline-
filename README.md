# Air Quality Data Pipeline

A Medallion Architecture data pipeline built with Apache Spark, PySpark, and Delta Lake on Databricks. The project processes air-quality sensor measurements provided in OpenAQ-compatible JSONL format through Landing, Bronze, Silver, and Gold layers.

The pipeline supports batch ingestion, data validation, unit standardization, Delta Lake merge operations, and execution logging.

## Architecture Overview

The pipeline follows a four-layer data architecture:

```text
┌───────────────────────────────────────────────────────────────┐
│                     1. LANDING LAYER                          │
│  Source JSONL files organized into full-load and incremental  │
│  batches in a Databricks Unity Catalog Volume                 │
└──────────────────────────────┬────────────────────────────────┘
                               │
                               ▼
┌───────────────────────────────────────────────────────────────┐
│                     2. BRONZE LAYER                           │
│  Notebook: 01_raw_to_bronze                                   │
│  Table: workspace.air_quality.bronze_readings                │
│  Explicit source schema, raw fields, batch metadata,          │
│  schema-drift handling, and corrupt-record capture            │
└──────────────────────────────┬────────────────────────────────┘
                               │
                               ▼
┌───────────────────────────────────────────────────────────────┐
│                     3. SILVER LAYER                           │
│  Notebook: 02_bronze_to_silver                                │
│  Tables: silver_readings, silver_stations,                   │
│          silver_quarantine                                    │
│  Type casting, validation, unit standardization,              │
│  deduplication, and Delta Lake MERGE operations               │
└──────────────────────────────┬────────────────────────────────┘
                               │
                               ▼
┌───────────────────────────────────────────────────────────────┐
│                     4. GOLD LAYER                             │
│  Notebook: 03_silver_to_gold.py                               │
│  Tables: gold_dim_date, gold_dim_location,                    │
│          gold_dim_parameter, gold_fact_measurements            │
│  Dimension and fact tables for analytical queries             │
└───────────────────────────────────────────────────────────────┘

              Execution and batch logging
          workspace.air_quality.pipeline_execution_logs
```

## Directory Structure

```text
air-quality-pipeline-/
├── data/
│   ├── raw/
│   │   ├── full_load/
│   │   └── incremental/
│   ├── full_load/
│   └── incremental_load/
├── notebooks/
│   ├── 00_config_helpers.py
│   ├── 00_api_to_landing.py
│   ├── 01_raw_to_bronze.py
│   ├── 02_bronze_to_silver.py
│   └── 03_silver_to_gold.py
└── README.md
```

*The directory structure above describes the intended organization; the exact locations of files may differ in the Databricks workspace.*

## Technology Stack

- **Databricks:** Notebook execution and data engineering environment
- **Apache Spark / PySpark:** Distributed data processing
- **Delta Lake:** Table storage and merge operations
- **Unity Catalog:** Catalog, schema, and Volume organization
- **OpenAQ data:** Air-quality measurement source and data format
- **GitHub:** Source control and collaboration

## Data Models

All tables are managed under the `workspace.air_quality` catalog and schema.

### 1. Bronze — `bronze_readings`

**Purpose:** Preserve source measurements in a raw, structured representation for downstream processing.

The Bronze layer uses an explicit PySpark `StructType` schema. Source leaf fields are initially represented as strings so that type conversion can take place in Silver.

| Field | Description |
|---|---|
| `value` | Original measurement value |
| `flagInfo` | Measurement flag information |
| `parameter` | Parameter ID, name, units, and display name |
| `period` | Measurement interval and UTC/local timestamps |
| `coordinates` | Latitude and longitude |
| `summary` | Available measurement summary statistics |
| `coverage` | Sampling coverage and observation completeness information |
| `_sensor_id` | Source sensor identifier |
| `_location_id` | Source location identifier |
| `_location_name` | Source location name |
| `_city` | City or locality |
| `_country` | Country code |
| `_parameter` | Top-level parameter field |
| `_unit` | Top-level unit field |
| `_corrupt_record` | Field used to capture malformed input records |
| `batch_id` | Batch folder identifier |
| `load_type` | `full_load` or `incremental` |
| `load_timestamp` | Timestamp associated with the load |

The Bronze ingestion process also discovers unexpected top-level fields and prepares the read schema to accommodate them.

**Table:** `workspace.air_quality.bronze_readings`

### 2. Silver — `silver_readings`

**Purpose:** Store validated and strongly typed measurement records with standardized units.

| Column | Data type | Description |
|---|---|---|
| `sensor_id` | LONG | Sensor identifier |
| `location_id` | LONG | Location identifier |
| `location_name` | STRING | Station name |
| `city` | STRING | City or locality |
| `country` | STRING | Country code |
| `parameter_id` | LONG | Parameter identifier |
| `parameter_name` | STRING | Standard parameter name |
| `parameter_display_name` | STRING | Human-readable parameter name |
| `datetime_from_utc` | TIMESTAMP | Measurement start in UTC |
| `datetime_from_local` | STRING | Measurement start in local time |
| `datetime_to_utc` | TIMESTAMP | Measurement end in UTC |
| `datetime_to_local` | STRING | Measurement end in local time |
| `value_raw` | DOUBLE | Original numeric measurement |
| `unit_raw` | STRING | Original measurement unit |
| `value_ugm3` | DOUBLE | Standardized measurement value, where conversion is possible |
| `unit_std` | STRING | Standard unit label |
| `has_flags` | BOOLEAN | Measurement flag indicator |
| `period_label` | STRING | Measurement period label |
| `period_interval` | STRING | Measurement interval |
| `latitude` | DOUBLE | Station latitude |
| `longitude` | DOUBLE | Station longitude |
| `summary_min` | DOUBLE | Minimum summary value |
| `summary_max` | DOUBLE | Maximum summary value |
| `summary_avg` | DOUBLE | Average summary value |
| `summary_median` | DOUBLE | Median summary value |
| `coverage_pct` | DOUBLE | Measurement coverage percentage |
| `batch_id` | STRING | Source batch identifier |
| `load_type` | STRING | Load category |
| `load_timestamp` | TIMESTAMP | Load timestamp |

The current Silver merge key is `(sensor_id, datetime_from_utc)`.

The transformation process flattens nested fields, casts values to appropriate types, validates records, deduplicates data within a batch, and uses Delta Lake `MERGE INTO` operations to update existing records or insert new ones.

The `to_ugm3()` function standardizes supported units to micrograms per cubic meter (`µg/m³`). Measurements with unsupported units may remain in Silver with a null standardized value.

**Table:** `workspace.air_quality.silver_readings`

### 3. Silver — `silver_stations`

**Purpose:** Store station and location attributes separately from measurement records.

| Column | Data type | Description |
|---|---|---|
| `sensor_id` | LONG | Sensor identifier |
| `location_id` | LONG | Location identifier |
| `location_name` | STRING | Station name |
| `city` | STRING | City or locality |
| `country` | STRING | Country code |
| `latitude` | DOUBLE | Station latitude |
| `longitude` | DOUBLE | Station longitude |
| `batch_id` | STRING | Source batch identifier |
| `load_type` | STRING | Load category |
| `load_timestamp` | TIMESTAMP | Load timestamp |

The current station merge key is `(sensor_id, location_id)`.

**Table:** `workspace.air_quality.silver_stations`

### 4. Silver — `silver_quarantine`

**Purpose:** Retain records that fail specified data-quality checks so that their issues can be investigated.

| Column | Data type | Description |
|---|---|---|
| `batch_id` | STRING | Source batch identifier |
| `load_type` | STRING | Load category |
| `_sensor_id` | STRING | Source sensor identifier |
| `_location_id` | STRING | Source location identifier |
| `_parameter` | STRING | Source parameter name |
| `datetime_from_utc` | STRING | Source measurement timestamp |
| `value_raw` | STRING | Original measurement value |
| `quarantine_reason` | STRING | Reason the record was quarantined |
| `raw_json` | STRING | Raw record content |
| `load_timestamp` | TIMESTAMP | Quarantine timestamp |

The transformation notebook routes records failing its configured checks, such as malformed records, missing key fields, or negative measurement values, to this table.

**Table:** `workspace.air_quality.silver_quarantine`

### 5. Execution Logs — `pipeline_execution_logs`

**Purpose:** Record batch processing activity and execution outcomes across the pipeline.

| Column | Data type | Description |
|---|---|---|
| `run_id` | STRING | Identifier for an execution |
| `batch_id` | STRING | Batch being processed |
| `layer` | STRING | Pipeline layer |
| `load_type` | STRING | Full-load or incremental category |
| `run_mode` | STRING | Processing mode |
| `parameters` | STRING | JSON representation of run parameters |
| `source_path` | STRING | Source path or source-table description |
| `start_time` | TIMESTAMP | Execution start time |
| `end_time` | TIMESTAMP | Execution end time |
| `status` | STRING | Execution status |
| `rows_read` | BIGINT | Number of input rows |
| `rows_inserted` | BIGINT | Number of inserted rows |
| `rows_updated` | BIGINT | Number of updated rows |
| `rows_quarantined` | BIGINT | Number of quarantined rows |
| `notes` | STRING | Additional execution information |
| `error_message` | STRING | Error details, when applicable |
| `load_timestamp` | TIMESTAMP | Log write timestamp |

The shared helper notebook defines the execution-log schema and the `log_run()` function used by the ingestion and transformation notebooks.

**Table:** `workspace.air_quality.pipeline_execution_logs`

### 6. Gold — Dimension and Fact Tables

**Purpose:** Organize the processed Silver data into tables intended for analytical queries.

| Table | Purpose | Observed row count |
|---|---|---:|
| `gold_dim_date` | Date dimension | 54 |
| `gold_dim_location` | Location dimension | 13 |
| `gold_dim_parameter` | Parameter dimension | 9 |
| `gold_fact_measurements` | Air-quality measurement fact table | 13,000 |

These counts were observed after the successful Gold notebook run. They describe that execution and may change when additional data is processed.

## Environment Setup and Execution Guide

### 1. Databricks and Git Setup

1. Open Databricks and navigate to your Git folder.
2. Connect the repository:

   `https://github.com/aliyahrasheed/air-quality-pipeline-`

3. Select the branch containing the notebooks you intend to run.
4. Confirm that the required notebooks are available in the Git folder.

### 2. Catalog, Schema, and Volume

The project configuration uses:

- Catalog: `workspace`
- Schema: `air_quality`
- Landing root: `/Volumes/workspace/air_quality/landing/raw`

The shared helper notebook creates the schema if it does not exist.

The landing Volume must exist and be accessible before uploading or reading source files.

### 3. Landing Raw Data

The pipeline supports full-load and incremental batches organized into folders under the landing root.

Example layout:

```text
/Volumes/workspace/air_quality/landing/raw/
├── full_load/
│   ├── batch_0001/
│   ├── batch_0002/
│   └── ...
└── incremental/
    └── <incremental_batch_folder>/
```

Each batch folder should contain its source JSONL file or files.

The project also includes an API-to-Landing notebook for the API ingestion workflow. Manual file uploads can be used to populate the landing Volume instead.

### 4. Run Bronze Ingestion

Open `01_raw_to_bronze`.

Configure the widgets:

| Widget | Full-load run | Incremental run |
|---|---|---|
| `load_type` | `full_load` | `incremental` |
| `run_mode` | `pending` | `pending` |
| `start_batch` | Leave blank unless selecting a range | Leave blank unless selecting a range |
| `end_batch` | Leave blank unless selecting a range | Leave blank unless selecting a range |

Run the notebook and confirm that the batch-processing summary reports success.

The `pending` mode selects batches that do not already have a successful execution-log entry for the relevant layer and load type.

### 5. Run Silver Transformations

After Bronze succeeds, open `02_bronze_to_silver`.

Use the corresponding `load_type` and `run_mode` values. Run the notebook and review the execution summary.

The Silver process transforms Bronze records, validates data, standardizes supported measurement units, updates Silver tables, and records processing outcomes.

### 6. Run Gold Transformations

After Silver succeeds, open `03_silver_to_gold.py`.

Run the notebook and review its output. Confirm that the four Gold tables exist and contain data.

The successful run documented for this project returned:

- `gold_dim_date`: 54 rows
- `gold_dim_location`: 13 rows
- `gold_dim_parameter`: 9 rows
- `gold_fact_measurements`: 13,000 rows

## Technical Features

### 1. Explicit Schema

The shared configuration notebook defines the source schema using PySpark `StructType` and `StructField`. Leaf fields are initially represented as strings so that type casting can take place in Silver.

### 2. Schema-Drift Handling

The helper functions inspect top-level JSON keys and identify fields not included in the expected source schema. The Bronze notebook uses these helpers when preparing its input schema.

### 3. Validation and Quarantine

The Silver notebook applies type conversion and configured data-quality checks. Records that fail applicable checks can be routed to `silver_quarantine` with a reason for investigation.

### 4. Unit Standardization

The `to_ugm3()` function handles recognized units and supported pollutant conversion factors. Values already expressed in the standard unit are retained; supported `ppm` and `ppb` values are converted where the function has a corresponding factor.

### 5. Delta Lake Merge Operations

Silver processing uses Delta Lake merge operations to update matching records and insert new records. This supports repeatable batch processing according to the merge keys defined in the notebooks.

### 6. Execution Logging

The pipeline records execution metadata and row counts in `workspace.air_quality.pipeline_execution_logs`.

Use the following query to inspect recent executions:

```sql
SELECT
    run_id,
    batch_id,
    layer,
    load_type,
    run_mode,
    status,
    rows_read,
    rows_inserted,
    rows_updated,
    rows_quarantined,
    start_time,
    end_time,
    notes,
    error_message
FROM workspace.air_quality.pipeline_execution_logs
ORDER BY start_time DESC;
```

## Project Validation

The Bronze, Silver, and Gold notebooks were run successfully during the documented project workflow. The Gold output contained the four dimension/fact tables and the row counts listed above.

For further validation, inspect execution logs, review quarantined records, and verify that the keys and relationships between Gold dimensions and fact records behave as expected.

## Future Improvements

- Add automated assertions for data quality and dimension/fact relationships.
- Schedule the notebooks as a Databricks Workflow.
- Expand monitoring for incremental loads and schema changes.
- Develop analytical queries and visualizations using the Gold tables.
