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
