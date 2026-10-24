# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03 - Silver to Gold
# MAGIC Builds a dimensional Gold model from `silver_readings` and refreshes it batch by batch.
# MAGIC
# MAGIC **Star schema**
# MAGIC - `gold_dim_date`: one row per calendar date; primary key `date_key` (YYYYMMDD).
# MAGIC - `gold_dim_location`: one row per sensor/location combination; primary key `location_key`.
# MAGIC - `gold_dim_parameter`: one row per pollutant parameter; primary key `parameter_key`.
# MAGIC - `gold_fact_measurements`: one row per Silver measurement grain; primary key `measurement_key`,
# MAGIC   with foreign keys to the three dimensions.
# MAGIC
# MAGIC Uses deterministic hash keys and Delta MERGE so reruns are idempotent.
# MAGIC Uses the shared batch-selection and audit logging helpers from `00_config_helpers`.

# COMMAND ----------

# MAGIC %run ./00_config_helpers

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Run parameters

# COMMAND ----------

dbutils.widgets.dropdown("load_type", "full_load", ["full_load", "incremental"])
dbutils.widgets.dropdown("run_mode", "pending", ["pending", "backfill"])
dbutils.widgets.text("start_batch", "")
dbutils.widgets.text("end_batch", "")

load_type = dbutils.widgets.get("load_type")
run_mode = dbutils.widgets.get("run_mode")
start_batch = dbutils.widgets.get("start_batch").strip()
end_batch = dbutils.widgets.get("end_batch").strip()

params = {
    "load_type": load_type,
    "run_mode": run_mode,
    "start_batch": start_batch,
    "end_batch": end_batch,
}
print(params)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Imports and Gold table names

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType, DoubleType, LongType, StringType, TimestampType, IntegerType
)
from datetime import datetime, timezone

GOLD_DATE_TABLE = f"{SCHEMA}.gold_dim_date"
GOLD_LOCATION_TABLE = f"{SCHEMA}.gold_dim_location"
GOLD_PARAMETER_TABLE = f"{SCHEMA}.gold_dim_parameter"
GOLD_FACT_TABLE = f"{SCHEMA}.gold_fact_measurements"
LAYER_GOLD = "Silver-to-Gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create empty Gold tables if they do not exist
# MAGIC
# MAGIC The primary and foreign keys are explicitly represented in the model. Delta Lake does not
# MAGIC enforce informational PK/FK constraints in the same way as a traditional relational database,
# MAGIC so the MERGE keys and verification queries below enforce/check the intended relationships.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_DATE_TABLE} (
    date_key INT,
    full_date DATE,
    day_of_month INT,
    day_of_week INT,
    day_name STRING,
    week_of_year INT,
    month_number INT,
    month_name STRING,
    quarter_number INT,
    year_number INT
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_LOCATION_TABLE} (
    location_key BIGINT,
    sensor_id BIGINT,
    location_id BIGINT,
    location_name STRING,
    city STRING,
    country STRING,
    latitude DOUBLE,
    longitude DOUBLE
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_PARAMETER_TABLE} (
    parameter_key BIGINT,
    parameter_id BIGINT,
    parameter_name STRING,
    parameter_display_name STRING,
    standard_unit STRING
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_FACT_TABLE} (
    measurement_key BIGINT,
    date_key INT,
    location_key BIGINT,
    parameter_key BIGINT,
    sensor_id BIGINT,
    location_id BIGINT,
    parameter_id BIGINT,
    datetime_from_utc TIMESTAMP,
    datetime_from_local STRING,
    datetime_to_utc TIMESTAMP,
    value_raw DOUBLE,
    value_ugm3 DOUBLE,
    unit_raw STRING,
    unit_std STRING,
    has_flags BOOLEAN,
    period_label STRING,
    period_interval STRING,
    summary_min DOUBLE,
    summary_max DOUBLE,
    summary_avg DOUBLE,
    summary_median DOUBLE,
    coverage_pct DOUBLE,
    batch_id STRING,
    load_type STRING,
    load_timestamp TIMESTAMP
) USING DELTA
""")

print("Gold tables are ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Helpers for Delta MERGE and row metrics

# COMMAND ----------

def _merge_upsert(table_name, source_df, merge_condition):
    """MERGE source_df into an existing Gold Delta table and return inserted/updated counts."""
    target = DeltaTable.forName(spark, table_name)
    (
        target.alias("tgt")
        .merge(source_df.alias("src"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    metrics = DeltaTable.forName(spark, table_name).history(1).select("operationMetrics").collect()[0][0]
    inserted = int(metrics.get("numTargetRowsInserted", 0))
    updated = int(metrics.get("numTargetRowsUpdated", 0))
    return inserted, updated


def _date_dimension(source_df):
    dates = (
        source_df
        .withColumn("_full_date", F.to_date("datetime_from_utc"))
        .where(F.col("_full_date").isNotNull())
        .select("_full_date").distinct()
    )
    return dates.select(
        (F.date_format("_full_date", "yyyyMMdd").cast("int")).alias("date_key"),
        F.col("_full_date").alias("full_date"),
        F.dayofmonth("_full_date").alias("day_of_month"),
        F.dayofweek("_full_date").alias("day_of_week"),
        F.date_format("_full_date", "EEEE").alias("day_name"),
        F.weekofyear("_full_date").alias("week_of_year"),
        F.month("_full_date").alias("month_number"),
        F.date_format("_full_date", "MMMM").alias("month_name"),
        F.quarter("_full_date").alias("quarter_number"),
        F.year("_full_date").alias("year_number"),
    )


def _location_dimension(source_df):
    base = (
        source_df
        .select(
            "sensor_id", "location_id", "location_name", "city", "country",
            "latitude", "longitude"
        )
        .dropDuplicates(["sensor_id", "location_id"])
    )
    return base.select(
        F.xxhash64("sensor_id", "location_id").alias("location_key"),
        "sensor_id", "location_id", "location_name", "city", "country",
        "latitude", "longitude"
    )


def _parameter_dimension(source_df):
    base = (
        source_df
        .select("parameter_id", "parameter_name", "parameter_display_name", "unit_std")
        .dropDuplicates(["parameter_id", "parameter_name"])
    )
    return base.select(
        F.xxhash64(
            F.coalesce(F.col("parameter_id"), F.lit(-1)),
            F.lower(F.coalesce(F.col("parameter_name"), F.lit("unknown")))
        ).alias("parameter_key"),
        "parameter_id", "parameter_name",
        "parameter_display_name",
        F.col("unit_std").alias("standard_unit")
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Process one Silver batch into dimensions and fact table

# COMMAND ----------

def process_gold_batch(run_id, batch_name):
    import datetime as _dt

    start_time = _dt.datetime.now(_dt.timezone.utc)
    source_path = (
        f"{SILVER_TABLE} WHERE batch_id='{batch_name}' AND load_type='{load_type}'"
    )

    try:
        silver = (
            spark.table(SILVER_TABLE)
            .where(
                (F.col("batch_id") == batch_name) &
                (F.col("load_type") == load_type)
            )
        )
        rows_read = silver.count()

        if rows_read == 0:
            log_run(
                run_id, batch_name, LAYER_GOLD, load_type, run_mode, params,
                source_path, start_time, "SUCCESS",
                rows_read=0, notes="0 Silver rows for this batch — skipped"
            )
            print(f"SKIP {batch_name}: no Silver rows")
            return True

        # Build and upsert dimensions first so fact foreign keys can resolve.
        date_df = _date_dimension(silver).dropDuplicates(["date_key"])
        location_df = _location_dimension(silver).dropDuplicates(["location_key"])
        parameter_df = _parameter_dimension(silver).dropDuplicates(["parameter_key"])

        d_ins, d_upd = _merge_upsert(
            GOLD_DATE_TABLE, date_df, "tgt.date_key = src.date_key"
        )
        l_ins, l_upd = _merge_upsert(
            GOLD_LOCATION_TABLE, location_df, "tgt.location_key = src.location_key"
        )
        p_ins, p_upd = _merge_upsert(
            GOLD_PARAMETER_TABLE, parameter_df, "tgt.parameter_key = src.parameter_key"
        )

        # Derive the same deterministic keys for the fact table's dimension references.
        fact = (
            silver
            .withColumn("_full_date", F.to_date("datetime_from_utc"))
            .withColumn("date_key", F.date_format("_full_date", "yyyyMMdd").cast("int"))
            .withColumn("location_key", F.xxhash64("sensor_id", "location_id"))
            .withColumn(
                "parameter_key",
                F.xxhash64(
                    F.coalesce(F.col("parameter_id"), F.lit(-1)),
                    F.lower(F.coalesce(F.col("parameter_name"), F.lit("unknown")))
                )
            )
            .withColumn(
                "measurement_key",
                F.xxhash64(
                    F.col("sensor_id"),
                    F.col("datetime_from_utc").cast("string"),
                    F.coalesce(F.col("parameter_id"), F.lit(-1)),
                    F.lower(F.coalesce(F.col("parameter_name"), F.lit("unknown")))
                )
            )
            .select(
                "measurement_key", "date_key", "location_key", "parameter_key",
                "sensor_id", "location_id", "parameter_id",
                "datetime_from_utc", "datetime_from_local", "datetime_to_utc",
                "value_raw", "value_ugm3", "unit_raw", "unit_std", "has_flags",
                "period_label", "period_interval",
                "summary_min", "summary_max", "summary_avg", "summary_median",
                "coverage_pct", "batch_id", "load_type", "load_timestamp"
            )
            .dropDuplicates(["measurement_key"])
        )

        rows_fact_source = fact.count()
        f_ins, f_upd = _merge_upsert(
            GOLD_FACT_TABLE, fact, "tgt.measurement_key = src.measurement_key"
        )

        notes = (
            f"Dimensions inserted/updated: date={d_ins}/{d_upd}, "
            f"location={l_ins}/{l_upd}, parameter={p_ins}/{p_upd}; "
            f"fact source rows={rows_fact_source}"
        )

        log_run(
            run_id, batch_name, LAYER_GOLD, load_type, run_mode, params,
            source_path, start_time, "SUCCESS",
            rows_read=rows_read,
            rows_inserted=f_ins,
            rows_updated=f_upd,
            notes=notes
        )
        print(
            f"OK {batch_name}: read={rows_read}, fact inserted={f_ins}, "
            f"fact updated={f_upd}; {notes}"
        )
        return True

    except Exception as exc:
        log_run(
            run_id, batch_name, LAYER_GOLD, load_type, run_mode, params,
            source_path, start_time, "FAILURE", error_message=str(exc)
        )
        print(f"FAIL {batch_name}: {str(exc)[:300]}")
        return False


# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Run pending batches or backfill a batch range

# COMMAND ----------

run_id = new_run_id()
batches = select_batches(LAYER_GOLD, load_type, run_mode, start_batch, end_batch)
print(f"run_id={run_id} | {len(batches)} batch(es) selected: {batches}")

results = {}
for batch in batches:
    ok = process_gold_batch(run_id, batch)
    results[batch] = ok
    if not ok:
        print(f"Stopping after failed batch '{batch}'.")
        break

failed = [batch for batch, ok in results.items() if not ok]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Run summary

# COMMAND ----------

display(
    spark.table(LOG_TABLE)
    .where((F.col("run_id") == run_id) & (F.col("layer") == LAYER_GOLD))
    .select(
        "batch_id", "layer", "status", "rows_read",
        "rows_inserted", "rows_updated", "start_time", "end_time",
        "notes", "error_message"
    )
    .orderBy("batch_id")
)

if failed:
    raise Exception(f"Gold finished with failures in batches: {failed}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Verification queries
# MAGIC Run these after the notebook finishes successfully.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Table sizes
# MAGIC SELECT 'gold_dim_date' AS table_name, COUNT(*) AS row_count FROM workspace.air_quality.gold_dim_date
# MAGIC UNION ALL
# MAGIC SELECT 'gold_dim_location', COUNT(*) FROM workspace.air_quality.gold_dim_location
# MAGIC UNION ALL
# MAGIC SELECT 'gold_dim_parameter', COUNT(*) FROM workspace.air_quality.gold_dim_parameter
# MAGIC UNION ALL
# MAGIC SELECT 'gold_fact_measurements', COUNT(*) FROM workspace.air_quality.gold_fact_measurements;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Fact-to-dimension referential-integrity checks: each result should be 0
# MAGIC SELECT
# MAGIC   SUM(CASE WHEN d.date_key IS NULL THEN 1 ELSE 0 END) AS missing_date_dimension,
# MAGIC   SUM(CASE WHEN l.location_key IS NULL THEN 1 ELSE 0 END) AS missing_location_dimension,
# MAGIC   SUM(CASE WHEN p.parameter_key IS NULL THEN 1 ELSE 0 END) AS missing_parameter_dimension
# MAGIC FROM workspace.air_quality.gold_fact_measurements f
# MAGIC LEFT JOIN workspace.air_quality.gold_dim_date d ON f.date_key = d.date_key
# MAGIC LEFT JOIN workspace.air_quality.gold_dim_location l ON f.location_key = l.location_key
# MAGIC LEFT JOIN workspace.air_quality.gold_dim_parameter p ON f.parameter_key = p.parameter_key;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Fact grain should not contain duplicate measurement keys
# MAGIC SELECT measurement_key, COUNT(*) AS copies
# MAGIC FROM workspace.air_quality.gold_fact_measurements
# MAGIC GROUP BY measurement_key
# MAGIC HAVING COUNT(*) > 1
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Gold data dictionary
# MAGIC
# MAGIC | Table | Key | Important columns |
# MAGIC |---|---|---|
# MAGIC | `gold_dim_date` | PK `date_key` | `full_date`, day/week/month/quarter/year attributes |
# MAGIC | `gold_dim_location` | PK `location_key` | `sensor_id`, `location_id`, location name, city, country, coordinates |
# MAGIC | `gold_dim_parameter` | PK `parameter_key` | `parameter_id`, parameter name/display name, standard unit |
# MAGIC | `gold_fact_measurements` | PK `measurement_key`; FKs `date_key`, `location_key`, `parameter_key` | timestamps, raw and standardized values, quality/summary fields, batch metadata |
# MAGIC
# MAGIC Note: Delta tables are created with key columns and the MERGE logic uses those keys, but Delta
# MAGIC does not automatically enforce primary/foreign key constraints. The verification query above
# MAGIC checks referential integrity.