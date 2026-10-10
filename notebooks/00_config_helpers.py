# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Config & Helpers
# MAGIC Shared configuration, explicit schemas, logging and helper functions.
# MAGIC Every other notebook starts with `%run ./00_config_helpers`.
# MAGIC
# MAGIC **Nothing here processes data** - it only defines things and creates the log table.

# COMMAND ----------

import json
import re
import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import (
    MapType, StringType, StructField, StructType, TimestampType, LongType
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration
# MAGIC Change `CATALOG` if your catalog is not called `workspace`.

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "air_quality"

# Root folder that contains  full_load/batch_xxxx  and  incremental/batch_yyyymmdd (JSON Lines files)
VOLUME_ROOT = "/Volumes/workspace/air_quality/landing/raw"

# Table names (schema-qualified; catalog is set with USE CATALOG below)
BRONZE_TABLE = f"{SCHEMA}.bronze_readings"
SILVER_TABLE = f"{SCHEMA}.silver_readings"
SILVER_STATIONS_TABLE = f"{SCHEMA}.silver_stations"
QUARANTINE_TABLE = f"{SCHEMA}.silver_quarantine"
LOG_TABLE = f"{SCHEMA}.pipeline_execution_logs"

LAYER_BRONZE = "Raw-to-Bronze"
LAYER_SILVER = "Bronze-to-Silver"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Explicit Bronze source schema (schema-on-read, NO inferSchema)
# MAGIC Every leaf field is read as `StringType` on purpose: Bronze is the raw copy, so a type change
# MAGIC at the source (e.g. `value` becoming `"N/A"`) can never break ingestion.
# MAGIC Strict typing/casting happens when data moves to Silver.

# COMMAND ----------

def _str(name):
    return StructField(name, StringType(), True)


_DT = StructType([_str("utc"), _str("local")])

BRONZE_SOURCE_SCHEMA = StructType([
    _str("value"),
    StructField("flagInfo", StructType([_str("hasFlags")]), True),
    StructField("parameter", StructType([_str("id"), _str("name"), _str("units"), _str("displayName")]), True),
    StructField("period", StructType([
        _str("label"), _str("interval"),
        StructField("datetimeFrom", _DT, True),
        StructField("datetimeTo", _DT, True),
    ]), True),
    StructField("coordinates", StructType([_str("latitude"), _str("longitude")]), True),
    StructField("summary", StructType([
        _str("min"), _str("q02"), _str("q25"), _str("median"),
        _str("q75"), _str("q98"), _str("max"), _str("avg"), _str("sd"),
    ]), True),
    StructField("coverage", StructType([
        _str("expectedCount"), _str("expectedInterval"),
        _str("observedCount"), _str("observedInterval"),
        _str("percentComplete"), _str("percentCoverage"),
        StructField("datetimeFrom", _DT, True),
        StructField("datetimeTo", _DT, True),
    ]), True),
    _str("_sensor_id"), _str("_location_id"), _str("_location_name"),
    _str("_city"), _str("_country"), _str("_parameter"), _str("_unit"),
])

EXPECTED_KEYS = [f.name for f in BRONZE_SOURCE_SCHEMA.fields]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Schema-drift helpers

# COMMAND ----------

def safe_col_name(name):
    """Delta does not allow spaces or ,;{}()= in column names."""
    return re.sub(r"[ ,;{}()\n\t=]", "_", name)


def discover_new_keys(files_glob):
    """
    Look at the TOP-LEVEL keys that really appear in the raw files (no schema inference:
    each line is read as text and we only list its keys) and return those that are not in
    our expected schema. These are 'drifted' columns.
    """
    keys = (
        spark.read.text(files_glob)
             .select(F.explode(F.map_keys(F.from_json("value", MapType(StringType(), StringType())))).alias("k"))
             .distinct()
    )
    found = {r["k"] for r in keys.collect()}
    return sorted(found - set(EXPECTED_KEYS))


def build_read_schema(new_keys=()):
    """Expected schema + any newly discovered columns (as strings) + corrupt-record column."""
    fields = list(BRONZE_SOURCE_SCHEMA.fields)
    fields += [StructField(k, StringType(), True) for k in new_keys]
    fields += [StructField("_corrupt_record", StringType(), True)]
    return StructType(fields)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Batch discovery (batches are folders; names sort in chronological order)

# COMMAND ----------

def list_batches(load_type, start_batch="", end_batch=""):
    """
    Return the batch folder names for a load type, SORTED (this is the processing order).
    start_batch / end_batch are inclusive and optional - this is how backfills pick a range.
    """
    root = f"{VOLUME_ROOT}/{load_type}"
    names = sorted(f.name.rstrip("/") for f in dbutils.fs.ls(root) if f.name.endswith("/"))
    if start_batch:
        names = [n for n in names if n >= start_batch]
    if end_batch:
        names = [n for n in names if n <= end_batch]
    return names


def get_successful_batches(layer, load_type):
    """Batches that already have a SUCCESS row in the log for this layer (used by 'pending' mode)."""
    if not spark.catalog.tableExists(LOG_TABLE):
        return set()
    rows = (
        spark.table(LOG_TABLE)
        .where((F.col("layer") == layer) & (F.col("load_type") == load_type) & (F.col("status") == "SUCCESS"))
        .select("batch_id").distinct().collect()
    )
    return {r["batch_id"] for r in rows}


def select_batches(layer, load_type, run_mode, start_batch="", end_batch=""):
    """
    run_mode = 'pending'  -> only batches without a SUCCESS log row yet (normal incremental run)
    run_mode = 'backfill' -> every batch in the range, re-processed even if done before
    """
    batches = list_batches(load_type, start_batch, end_batch)
    if run_mode == "pending":
        done = get_successful_batches(layer, load_type)
        batches = [b for b in batches if b not in done]
    return batches

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Audit logging

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
  run_id            STRING,
  batch_id          STRING,
  layer             STRING,
  load_type         STRING,
  run_mode          STRING,
  parameters        STRING,
  source_path       STRING,
  start_time        TIMESTAMP,
  end_time          TIMESTAMP,
  status            STRING,
  rows_read         BIGINT,
  rows_inserted     BIGINT,
  rows_updated      BIGINT,
  rows_quarantined  BIGINT,
  notes             STRING,
  error_message     STRING,
  load_timestamp    TIMESTAMP
) USING DELTA
""")

LOG_SCHEMA = StructType([
    StructField("run_id", StringType()), StructField("batch_id", StringType()),
    StructField("layer", StringType()), StructField("load_type", StringType()),
    StructField("run_mode", StringType()), StructField("parameters", StringType()),
    StructField("source_path", StringType()),
    StructField("start_time", TimestampType()), StructField("end_time", TimestampType()),
    StructField("status", StringType()),
    StructField("rows_read", LongType()), StructField("rows_inserted", LongType()),
    StructField("rows_updated", LongType()), StructField("rows_quarantined", LongType()),
    StructField("notes", StringType()), StructField("error_message", StringType()),
    StructField("load_timestamp", TimestampType()),
])


def new_run_id():
    return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S") + "_" + uuid.uuid4().hex[:6]


def log_run(run_id, batch_id, layer, load_type, run_mode, parameters, source_path,
            start_time, status, rows_read=0, rows_inserted=0, rows_updated=0,
            rows_quarantined=0, notes=None, error_message=None):
    """Write ONE audit row. Call it for every batch, on success AND on failure."""
    end_time = datetime.now(timezone.utc)
    row = (
        run_id, batch_id, layer, load_type, run_mode, json.dumps(parameters), source_path,
        start_time, end_time, status,
        int(rows_read), int(rows_inserted), int(rows_updated), int(rows_quarantined),
        notes, (error_message[:2000] if error_message else None), end_time,
    )
    spark.createDataFrame([row], schema=LOG_SCHEMA).write.mode("append").saveAsTable(LOG_TABLE)

# COMMAND ----------

print(f"Config loaded. Catalog={CATALOG}, schema={SCHEMA}")
print(f"Raw root: {VOLUME_ROOT}")