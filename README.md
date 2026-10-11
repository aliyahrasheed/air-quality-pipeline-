# Air Quality Data Pipeline

A Medallion Architecture data pipeline built with Apache Spark and Delta Lake on Databricks, ingesting global air quality sensor data from the OpenAQ API to analyze pollution trends across Pakistan, India, and the United States.

## Architecture Overview

The pipeline implements a three-tier Medallion architecture:

1. Landing Zone: Raw JSON Lines files uploaded in batches (full load and incremental).
2. Bronze Layer: Raw append-only Delta table preserving source schema with audit tracking and schema drift handling.
3. Silver Layer: Cleaned, strongly typed, deduplicated, and unit-standardized Delta tables built via idempotent MERGE operations.

## Directory Structure

- data/raw/: Raw ingested data files split into full_load and incremental batches.
- notebooks/: Databricks notebooks executing pipeline stages:
  - 00_config_helpers.py: Shared configuration, explicit schemas, audit logging functions, and batch selection helpers.
  - 01_raw_to_bronze.py: Ingests raw batch folders into the Bronze Delta table with schema drift detection and corrupt record tracking.
  - 02_bronze_to_silver.py: Flattens structures, validates records, extracts clean station metadata, normalizes pollutant units, and merges into Silver tables.

## Environment Setup and Execution Guide

Follow these steps to set up and run the pipeline in Databricks:

### 1. Workspace Configuration and Repository Import
- Open Databricks and navigate to Workspace.
- Select Create, choose Git folder, and paste the repository URL: https://github.com/aliyahrasheed/air-quality-pipeline-
- Ensure the folder contains notebooks 00_config_helpers, 01_raw_to_bronze, and 02_bronze_to_silver.

### 2. Unity Catalog Schema and Volume Setup
- Navigate to Catalog, select workspace, and create a schema named air_quality.
- Inside the air_quality schema, create a managed Volume named landing.
- The volume target root will be: /Volumes/workspace/air_quality/landing/raw

### 3. Raw Data Landing
- Upload the raw archive (raw.zip) directly into the landing volume.
- Unpack the archive into the landing directory using Serverless compute or an active cluster:
  ```python
  import zipfile
  base = "/Volumes/workspace/air_quality/landing"
  dbutils.fs.rm(f"{base}/raw", recurse=True)
  with zipfile.ZipFile(f"{base}/raw.zip") as z:
      z.extractall(base)
  ```
- Confirm the contents exist under:
  - /Volumes/workspace/air_quality/landing/raw/full_load/
  - /Volumes/workspace/air_quality/landing/raw/incremental/

### 4. Running Bronze Ingestion (01_raw_to_bronze)
- Attach the notebook to Serverless compute.
- Run the full load ingestion:
  - Set parameter load_type to full_load
  - Set parameter run_mode to pending
  - Execute Run All (processes 10 batches, total 13,000 records).
- Run the incremental ingestion:
  - Set parameter load_type to incremental
  - Set parameter run_mode to pending
  - Execute Run All (processes 3 batches, total 378 records).
- Verify total row count:
  ```sql
  SELECT COUNT(*) FROM workspace.air_quality.bronze_readings;
  -- Expected count: 13378
  ```

### 5. Running Silver Transformations (02_bronze_to_silver)
- Run full load promotion:
  - Set parameter load_type to full_load
  - Set parameter run_mode to pending
  - Execute Run All (processes 10 batches into silver_readings and silver_stations).
- Run incremental load promotion:
  - Set parameter load_type to incremental
  - Set parameter run_mode to pending
  - Execute Run All (processes 3 batches).
- Verify Silver tables:
  ```sql
  SELECT COUNT(*) FROM workspace.air_quality.silver_readings;
  -- Expected count: 13378

  SELECT COUNT(*) FROM workspace.air_quality.silver_stations;
  -- Expected count: 13

  SELECT COUNT(*) FROM workspace.air_quality.silver_quarantine;
  -- Expected count: 0
  ```
- Idempotency test:
  - Set run_mode to backfill and re-run any batch.
  - Confirm row count remains 13,378 and inserted count is 0 in the execution log.

## Data Layers and Tables

### Catalog and Schema
- Catalog: workspace
- Schema: air_quality
- Volume: /Volumes/workspace/air_quality/landing

### Tables
- workspace.air_quality.bronze_readings: Bronze layer storing raw records with file and ingestion metadata.
- workspace.air_quality.silver_readings: Cleaned air quality readings keyed on sensor_id and datetime_from_utc.
- workspace.air_quality.silver_stations: Station dimension table keyed on sensor_id and location_id.
- workspace.air_quality.silver_quarantine: Invalid rows rejected due to corrupt records, null keys, or negative values.
- workspace.air_quality.pipeline_execution_logs: Audit table tracking each batch run across Bronze and Silver layers.

## Silver Layer Transformations

The Silver notebook applies the following rules:
- Schema flattening: Nested JSON structures for parameter, period, coordinates, coverage, and summary are extracted into explicit columns.
- Strict type casting: Numerical values, identifiers, and timestamps are cast using try_cast expressions to prevent runtime failures.
- Data validation and quarantine: Rows with unparseable JSON, null keys, or invalid measurement values are routed to the quarantine table with a documented reason.
- Unit standardization: Pollutant values reported in ppm or ppb are standardized to micrograms per cubic meter based on molecular weight, while retaining the raw value and unit.
- Deduplication: In-batch duplicate records on sensor_id and datetime_from_utc are resolved prior to merge.
- Idempotent upsert: Merged into silver_readings using Delta Lake MERGE ON (sensor_id, datetime_from_utc), ensuring repeat executions do not duplicate records.
