# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02 - Bronze to Silver
# MAGIC Reads from `bronze_readings` batch by batch (in sorted order), flattens nested columns,
# MAGIC casts to strong types, quarantines bad rows, standardises units, deduplicates within the
# MAGIC batch, and **MERGE INTO** `silver_readings` and `silver_stations`.
# MAGIC
# MAGIC | Parameter | Meaning |
# MAGIC |---|---|
# MAGIC | `load_type` | `full_load` or `incremental` (matches the Bronze batch partition) |
# MAGIC | `run_mode` | `pending` = only batches not yet logged as Silver SUCCESS; `backfill` = re-process every batch in range |
# MAGIC | `start_batch` / `end_batch` | Optional inclusive folder-name range for backfills |
# MAGIC
# MAGIC **Idempotent:** MERGE ON (sensor_id, datetime_from_utc) — re-running the same batch
# MAGIC produces 0 inserted / 0 updated rows and the row count never changes.

# COMMAND ----------

# MAGIC %run ./00_config_helpers

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets

# COMMAND ----------

dbutils.widgets.dropdown("load_type", "full_load", ["full_load", "incremental"])
dbutils.widgets.dropdown("run_mode",  "pending",   ["pending", "backfill"])
dbutils.widgets.text("start_batch", "")
dbutils.widgets.text("end_batch",   "")

load_type   = dbutils.widgets.get("load_type")
run_mode    = dbutils.widgets.get("run_mode")
start_batch = dbutils.widgets.get("start_batch").strip()
end_batch   = dbutils.widgets.get("end_batch").strip()

params = {
    "load_type":   load_type,
    "run_mode":    run_mode,
    "start_batch": start_batch,
    "end_batch":   end_batch,
}
print(params)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional imports for Silver

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, LongType, StringType, StructField, StructType, TimestampType
)
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## DDL — create Silver tables if they don't exist yet

# COMMAND ----------

# silver_readings — one row per (sensor_id, datetime_from_utc)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
  sensor_id              LONG,
  location_id            LONG,
  location_name          STRING,
  city                   STRING,
  country                STRING,
  parameter_id           LONG,
  parameter_name         STRING,
  parameter_display_name STRING,
  datetime_from_utc      TIMESTAMP,
  datetime_from_local    STRING,
  datetime_to_utc        TIMESTAMP,
  datetime_to_local      STRING,
  value_raw              DOUBLE,
  unit_raw               STRING,
  value_ugm3             DOUBLE,
  unit_std               STRING,
  has_flags              BOOLEAN,
  period_label           STRING,
  period_interval        STRING,
  latitude               DOUBLE,
  longitude              DOUBLE,
  summary_min            DOUBLE,
  summary_max            DOUBLE,
  summary_avg            DOUBLE,
  summary_median         DOUBLE,
  coverage_pct           DOUBLE,
  batch_id               STRING,
  load_type              STRING,
  load_timestamp         TIMESTAMP
) USING DELTA
""")

# silver_stations — one row per (sensor_id, location_id) — slowly-changing snapshot
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_STATIONS_TABLE} (
  sensor_id      LONG,
  location_id    LONG,
  location_name  STRING,
  city           STRING,
  country        STRING,
  latitude       DOUBLE,
  longitude      DOUBLE,
  batch_id       STRING,
  load_type      STRING,
  load_timestamp TIMESTAMP
) USING DELTA
""")

# silver_quarantine — bad rows with a reason column
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {QUARANTINE_TABLE} (
  batch_id          STRING,
  load_type         STRING,
  _sensor_id        STRING,
  _location_id      STRING,
  _parameter        STRING,
  datetime_from_utc STRING,
  value_raw         STRING,
  quarantine_reason STRING,
  raw_json          STRING,
  load_timestamp    TIMESTAMP
) USING DELTA
""")

print("DDL done — all Silver tables exist.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unit-conversion UDF
# MAGIC Standardise every measurement to µg/m³.
# MAGIC Rows that are already µg/m³ pass through. Unknown units get `value_ugm3 = NULL`
# MAGIC (they are still kept in Silver; only NULL key/corrupt/negative rows go to quarantine).

# COMMAND ----------

# Molecular weights → ppm-to-µg/m³ factor at 20 °C, 1 atm  (factor = MW * 1000 / 24.04)
_PPM_TO_UGM3 = {
    "no2":  1912.9,   # MW 46.01
    "o3":   1996.0,   # MW 48.00
    "so2":  2659.0,   # MW 64.06
    "co":   1165.0,   # MW 28.01
    "no":   1247.0,   # MW 30.01
    "nox":  1912.9,   # treated as NO2-equivalent
    "pm25": 1.0,      # ppm not standard for PM; 1:1 fallback
    "pm10": 1.0,
}

@F.udf(DoubleType())

def to_ugm3(value, unit, parameter):
    """Return value in µg/m³, or None if conversion is impossible."""
    if value is None or unit is None:
        return None

    try:
        v = float(value)
    except (TypeError, ValueError):
        return None

    # Normalize micro symbols, spaces, and superscript 3.
    u = (
        unit.lower()
        .replace(" ", "")
        .replace("\u03bc", "u")
        .replace("\u00b5", "u")
        .replace("³", "3")
    )

    # These units already represent µg/m³.
    if u in ("ug/m3", "ugm3"):
        return v

    p = (parameter or "").lower().strip()
    factor = _PPM_TO_UGM3.get(p)

    if u == "ppm" and factor:
        return v * factor

    if u == "ppb" and factor:
        return v / 1000.0 * factor

    return None


# COMMAND ----------

# MAGIC %md
# MAGIC ## Core batch processor

# COMMAND ----------

def process_silver_batch(run_id, batch_name):
    """
    Promote ONE batch from bronze_readings to Silver tables.
    Returns True on success, False on any exception.
    Mirrors process_bronze_batch in 01_raw_to_bronze exactly.
    """
    import datetime as _dt
    start_time = _dt.datetime.now(_dt.timezone.utc)
    source_path = f"bronze_readings WHERE batch_id='{batch_name}' AND load_type='{load_type}'"

    try:
        # ── 1. Read from Bronze ──────────────────────────────────────────────────────
        bronze = (
            spark.table(BRONZE_TABLE)
            .where(
                (F.col("batch_id") == batch_name) &
                (F.col("load_type") == load_type)
            )
        )
        rows_read = bronze.count()
        if rows_read == 0:
            log_run(run_id, batch_name, LAYER_SILVER, load_type, run_mode, params,
                    source_path, start_time, "SUCCESS",
                    rows_read=0, notes="0 rows in Bronze for this batch — skipped")
            print(f"SKIP {batch_name}: 0 rows in Bronze")
            return True

        # ── 2. Flatten & cast (all fields; use try_cast for non-nullable targets) ───
        flat = bronze.select(
            # Keys / identifiers
            F.expr("try_cast(_sensor_id AS bigint)").alias("sensor_id"),
            F.expr("try_cast(_location_id AS bigint)").alias("location_id"),
            F.col("_location_name").alias("location_name"),
            F.col("_city").alias("city"),
            F.col("_country").alias("country"),

            # Parameter block
            F.expr("try_cast(parameter.id AS bigint)").alias("parameter_id"),
            F.col("parameter.name").alias("parameter_name"),
            F.col("parameter.displayName").alias("parameter_display_name"),
            F.col("parameter.units").alias("unit_raw"),

            # Period timestamps
            F.expr("try_cast(period.datetimeFrom.utc AS timestamp)").alias("datetime_from_utc"),
            F.col("period.datetimeFrom.local").alias("datetime_from_local"),
            F.expr("try_cast(period.datetimeTo.utc AS timestamp)").alias("datetime_to_utc"),
            F.col("period.datetimeTo.local").alias("datetime_to_local"),
            F.col("period.label").alias("period_label"),
            F.col("period.interval").alias("period_interval"),

            # Measurement value
            F.expr("try_cast(value AS double)").alias("value_raw"),

            # Flags
            F.expr("try_cast(flagInfo.hasFlags AS boolean)").alias("has_flags"),

            # Coordinates
            F.expr("try_cast(coordinates.latitude AS double)").alias("latitude"),
            F.expr("try_cast(coordinates.longitude AS double)").alias("longitude"),

            # Summary statistics
            F.expr("try_cast(summary.min AS double)").alias("summary_min"),
            F.expr("try_cast(summary.max AS double)").alias("summary_max"),
            F.expr("try_cast(summary.avg AS double)").alias("summary_avg"),
            F.expr("try_cast(summary.median AS double)").alias("summary_median"),

            # Coverage
            F.expr("try_cast(coverage.percentCoverage AS double)").alias("coverage_pct"),

            # Passthrough for quarantine routing
            F.col("_corrupt_record"),
            F.col("_parameter").alias("_parameter_raw"),
        )

        # ── 3. Classify rows ─────────────────────────────────────────────────────────
        corrupt_cond   = F.col("_corrupt_record").isNotNull()
        null_key_cond  = (
            F.col("sensor_id").isNull() |
            F.col("location_id").isNull() |
            F.col("datetime_from_utc").isNull()
        )
        bad_value_cond = F.col("value_raw").isNull() | (F.col("value_raw") < 0)

        quarantine_reason = (
            F.when(corrupt_cond,   F.lit("_corrupt_record is not null"))
             .when(null_key_cond,  F.lit("null key field (sensor_id / location_id / datetime_from_utc)"))
             .when(bad_value_cond, F.lit("non-castable or negative value"))
        )

        bad  = flat.where(corrupt_cond | null_key_cond | bad_value_cond)
        good = flat.where(~(corrupt_cond | null_key_cond | bad_value_cond))

        
        # ── 4. Write quarantine (idempotent per batch) ──
        rows_quarantined = bad.count()

        if rows_quarantined > 0:
            # Remove previous quarantine records for this batch before rewriting them.
            if spark.catalog.tableExists(QUARANTINE_TABLE):
                spark.sql(f"""
                    DELETE FROM {QUARANTINE_TABLE}
                    WHERE batch_id = '{batch_name}'
                      AND load_type = '{load_type}'
                """)

            (
                bad.select(
                    F.lit(batch_name).alias("batch_id"),
                    F.lit(load_type).alias("load_type"),
                    F.col("sensor_id").cast(StringType()).alias("_sensor_id"),
                    F.col("location_id").cast(StringType()).alias("_location_id"),
                    F.col("_parameter_raw").alias("_parameter"),
                    F.col("datetime_from_utc").cast(StringType()).alias("datetime_from_utc"),
                    F.col("value_raw").cast(StringType()).alias("value_raw"),
                    quarantine_reason.alias("quarantine_reason"),
                    F.col("_corrupt_record").alias("raw_json"),
                    F.current_timestamp().alias("load_timestamp"),
                )
                .write.format("delta").mode("append").saveAsTable(QUARANTINE_TABLE)
            )



        # ── 5. Unit standardisation ──────────────────────────────────────────────────
        good = (
            good
            .withColumn("value_ugm3", to_ugm3(F.col("value_raw"), F.col("unit_raw"), F.col("parameter_name")))
            .withColumn("unit_std",
                F.when(
                    to_ugm3(F.col("value_raw"), F.col("unit_raw"), F.col("parameter_name")).isNotNull(),
                    F.lit("\u00b5g/m\u00b3")
                ).otherwise(F.lit(None).cast(StringType()))
            )
        )

        # ── 6. Deduplicate within the batch on (sensor_id, datetime_from_utc) ───────
        dedup_window = (
            Window.partitionBy("sensor_id", "parameter_id", "datetime_from_utc")
                  .orderBy(F.col("parameter_name").asc_nulls_last())   # deterministic tiebreak
        )
        good = (
            good
            .withColumn("_rn", F.row_number().over(dedup_window))
            .where(F.col("_rn") == 1)
            .drop("_rn")
        )

        # ── 7. Final Silver projection ────────────────────────────────────────────────
        now_ts = F.current_timestamp()
        silver_df = good.select(
            "sensor_id", "location_id", "location_name", "city", "country",
            "parameter_id", "parameter_name", "parameter_display_name",
            "datetime_from_utc", "datetime_from_local",
            "datetime_to_utc",   "datetime_to_local",
            "value_raw", "unit_raw", "value_ugm3", "unit_std",
            "has_flags", "period_label", "period_interval",
            "latitude", "longitude",
            "summary_min", "summary_max", "summary_avg", "summary_median",
            "coverage_pct",
            F.lit(batch_name).alias("batch_id"),
            F.lit(load_type).alias("load_type"),
            now_ts.alias("load_timestamp"),
        )

        # ── 8. MERGE INTO silver_readings ─────────────────────────────────────────────
        if spark.catalog.tableExists(SILVER_TABLE):
            dt = DeltaTable.forName(spark, SILVER_TABLE)
            # Do not rewrite an identical row just to refresh load_timestamp:
            # this keeps exact reruns idempotent while still updating changed source data.
            reading_compare_cols = [c for c in silver_df.columns if c != "load_timestamp"]
            reading_changed = "NOT (" + " AND ".join(
                [f"tgt.`{c}` <=> src.`{c}`" for c in reading_compare_cols]
            ) + ")"
            (
                dt.alias("tgt")
                  .merge(
                      silver_df.alias("src"),
                      "tgt.sensor_id = src.sensor_id AND tgt.parameter_id = src.parameter_id AND tgt.datetime_from_utc = src.datetime_from_utc"
                  )
                  .whenMatchedUpdateAll(condition=reading_changed)
                  .whenNotMatchedInsertAll()
                  .execute()
            )
            hist = dt.history(1).select("operationMetrics").collect()[0][0]
            rows_inserted = int(hist.get("numTargetRowsInserted", 0))
            rows_updated  = int(hist.get("numTargetRowsUpdated",  0))
        else:
            silver_df.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLE)
            rows_inserted = silver_df.count()
            rows_updated  = 0

        # ── 9. MERGE INTO silver_stations ─────────────────────────────────────────────
        stations_df = (
            good
            .select("sensor_id", "location_id", "location_name", "city", "country",
                    "latitude", "longitude")
            .dropDuplicates(["sensor_id", "location_id"])
            .withColumn("batch_id",       F.lit(batch_name))
            .withColumn("load_type",      F.lit(load_type))
            .withColumn("load_timestamp", now_ts)
        )

        if spark.catalog.tableExists(SILVER_STATIONS_TABLE):
            st = DeltaTable.forName(spark, SILVER_STATIONS_TABLE)
            station_compare_cols = [c for c in stations_df.columns if c != "load_timestamp"]
            station_changed = "NOT (" + " AND ".join(
                [f"tgt.`{c}` <=> src.`{c}`" for c in station_compare_cols]
            ) + ")"
            (
                st.alias("tgt")
                  .merge(
                      stations_df.alias("src"),
                      "tgt.sensor_id = src.sensor_id AND tgt.location_id = src.location_id"
                  )
                  .whenMatchedUpdateAll(condition=station_changed)
                  .whenNotMatchedInsertAll()
                  .execute()
            )
        else:
            stations_df.write.format("delta").mode("overwrite").saveAsTable(SILVER_STATIONS_TABLE)

        # ── 10. Audit log ──────────────────────────────────────────────────────────────
        notes_parts = []
        if rows_quarantined:
            notes_parts.append(f"{rows_quarantined} rows quarantined")
        if rows_inserted == 0 and rows_updated > 0:
            notes_parts.append(f"{rows_updated} rows updated (idempotent re-run, 0 new rows)")
        elif rows_updated:
            notes_parts.append(f"{rows_updated} rows updated")

        log_run(
            run_id, batch_name, LAYER_SILVER, load_type, run_mode, params, source_path,
            start_time, "SUCCESS",
            rows_read=rows_read,
            rows_inserted=rows_inserted,
            rows_updated=rows_updated,
            rows_quarantined=rows_quarantined,
            notes="; ".join(notes_parts) or None,
        )
        print(
            f"OK   {batch_name}: "
            f"read={rows_read}  ins={rows_inserted}  upd={rows_updated}  quar={rows_quarantined}"
        )
        return True

    except Exception as exc:
        log_run(
            run_id, batch_name, LAYER_SILVER, load_type, run_mode, params, source_path,
            start_time, "FAILURE", error_message=str(exc),
        )
        print(f"FAIL {batch_name}: {str(exc)[:300]}")
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

run_id  = new_run_id()
batches = select_batches(LAYER_SILVER, load_type, run_mode, start_batch, end_batch)
print(f"run_id={run_id} | {len(batches)} batch(es) to process: {batches}")

results = {}
for b in batches:       # sorted order — stop at first failure to keep upserts consistent
    ok = process_silver_batch(run_id, b)
    results[b] = ok
    if not ok:
        print(f"Stopping after failed batch '{b}' to preserve upsert file order.")
        break

failed = [b for b, ok in results.items() if not ok]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run summary

# COMMAND ----------

display(
    spark.table(LOG_TABLE)
    .where(F.col("run_id") == run_id)
    .select("batch_id", "layer", "status", "rows_read",
            "rows_inserted", "rows_updated", "rows_quarantined",
            "start_time", "end_time", "notes", "error_message")
    .orderBy("batch_id")
)

# COMMAND ----------

display(
    spark.table(QUARANTINE_TABLE)
    .filter(
        (F.col("batch_id") == "batch_20190616_180536") &
        (F.col("load_type") == "incremental")
    )
    .groupBy("batch_id", "quarantine_reason")
    .count()
)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     COUNT(DISTINCT CONCAT_WS(
# MAGIC         '|',
# MAGIC         COALESCE(sensor_id, ''),
# MAGIC         COALESCE(datetime_from_utc, ''),
# MAGIC         COALESCE(parameter_name, '')
# MAGIC     )) AS distinct_readings
# MAGIC FROM workspace.air_quality.silver_readings;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     batch_id,
# MAGIC     quarantine_reason,
# MAGIC     COUNT(*) AS rejected_rows
# MAGIC FROM workspace.air_quality.silver_quarantine
# MAGIC WHERE batch_id = 'batch_20190616_190612'
# MAGIC GROUP BY batch_id, quarantine_reason;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     batch_id,
# MAGIC     _sensor_id,
# MAGIC     _location_id,
# MAGIC     _parameter,
# MAGIC     datetime_from_utc,
# MAGIC     value_raw,
# MAGIC     quarantine_reason,
# MAGIC     raw_json
# MAGIC FROM workspace.air_quality.silver_quarantine
# MAGIC WHERE batch_id = 'batch_20190616_190612';

# COMMAND ----------

if failed:
    raise Exception(f"Silver finished with failures in batches: {failed}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verification queries

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   load_type,
# MAGIC   COUNT(*)                          AS total_rows,
# MAGIC   COUNT(DISTINCT sensor_id)         AS sensors,
# MAGIC   COUNT(DISTINCT parameter_name)    AS parameters,
# MAGIC   MIN(datetime_from_utc)            AS earliest,
# MAGIC   MAX(datetime_from_utc)            AS latest,
# MAGIC   SUM(CASE WHEN value_ugm3 IS NULL THEN 1 ELSE 0 END) AS unconverted_units
# MAGIC FROM workspace.air_quality.silver_readings
# MAGIC GROUP BY load_type
# MAGIC ORDER BY load_type

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS quarantined_rows FROM workspace.air_quality.silver_quarantine

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Bronze + Silver audit log side by side (most recent run first)
# MAGIC SELECT batch_id, layer, status, rows_read, rows_inserted, rows_updated,
# MAGIC        rows_quarantined, start_time, end_time, notes
# MAGIC FROM   workspace.air_quality.pipeline_execution_logs
# MAGIC ORDER  BY start_time DESC
# MAGIC LIMIT  40