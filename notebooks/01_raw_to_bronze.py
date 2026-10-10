# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM workspace.air_quality.bronze_readings

# COMMAND ----------

# MAGIC %md
# MAGIC # 01 - Raw to Bronze
# MAGIC Reads raw JSON **batch folders in sorted order**, one batch at a time, and writes them to the Bronze Delta table.
# MAGIC
# MAGIC | Parameter | Meaning |
# MAGIC |---|---|
# MAGIC | `load_type` | `full_load` or `incremental` (the sub-folder under the raw root) |
# MAGIC | `run_mode` | `pending` = only batches not yet logged as SUCCESS (normal run). `backfill` = re-process every batch in the range |
# MAGIC | `start_batch` / `end_batch` | Optional inclusive folder-name range, e.g. `batch_0003` to `batch_0005` |
# MAGIC
# MAGIC **Idempotent:** a batch is replaced (`replaceWhere`) instead of appended, so re-running never duplicates rows.

# COMMAND ----------

# MAGIC %run ./00_config_helpers

# COMMAND ----------

dbutils.widgets.dropdown("load_type", "full_load", ["full_load", "incremental"])
dbutils.widgets.dropdown("run_mode", "pending", ["pending", "backfill"])
dbutils.widgets.text("start_batch", "")
dbutils.widgets.text("end_batch", "")

load_type = dbutils.widgets.get("load_type")
run_mode = dbutils.widgets.get("run_mode")
start_batch = dbutils.widgets.get("start_batch").strip()
end_batch = dbutils.widgets.get("end_batch").strip()

params = {"load_type": load_type, "run_mode": run_mode, "start_batch": start_batch, "end_batch": end_batch}
print(params)

# COMMAND ----------

def process_bronze_batch(run_id, batch_name):
    """Ingest ONE batch folder into Bronze and write ONE audit row. Returns True on success."""
    start_time = datetime.now(timezone.utc)
    batch_path = f"{VOLUME_ROOT}/{load_type}/{batch_name}"
    files_glob = f"{batch_path}/*.jsonl"
    try:
        # 1) schema drift: which top-level keys are NOT in our expected schema?
        new_keys = discover_new_keys(files_glob)
        schema = build_read_schema(new_keys)          # explicit StructType, no inference

        # 2) read with the explicit schema (JSON Lines: one record per line, so a bad record
        #    is isolated in _corrupt_record and the rest of the batch loads normally)
        df = (
            spark.read.schema(schema)
            .option("mode", "PERMISSIVE")
            .json(files_glob)
        )
        for k in new_keys:                              # make drifted names Delta-safe
            if safe_col_name(k) != k:
                df = df.withColumnRenamed(k, safe_col_name(k))

        # 3) metadata columns
        df = (
            df.select("*", F.col("_metadata.file_path").alias("source_file"))
              .withColumn("batch_id", F.lit(batch_name))
              .withColumn("load_type", F.lit(load_type))
              .withColumn("ingestion_date", F.current_date())
              .withColumn("load_timestamp", F.current_timestamp())
        )

        # 4) idempotent write: replace this batch's rows (or create the table the first time)
        pred = f"batch_id = '{batch_name}' AND load_type = '{load_type}'"
        exists = spark.catalog.tableExists(BRONZE_TABLE)
        prev_rows = spark.table(BRONZE_TABLE).where(pred).count() if exists else 0

        writer = df.write.format("delta").option("mergeSchema", "true")   # allows new columns
        if exists:
            try:
                writer.mode("overwrite").option("replaceWhere", pred).saveAsTable(BRONZE_TABLE)
            except Exception as e:      # fallback: delete this batch, then append it
                print(f"replaceWhere failed ({str(e)[:120]}); using DELETE + append")
                spark.sql(f"DELETE FROM {BRONZE_TABLE} WHERE {pred}")
                writer.mode("append").saveAsTable(BRONZE_TABLE)
        else:
            writer.mode("overwrite").partitionBy("ingestion_date").saveAsTable(BRONZE_TABLE)

        # 5) counts come from the written table (no re-reading of the source)
        agg = (
            spark.table(BRONZE_TABLE).where(pred)
            .agg(F.count("*").alias("n"),
                 F.sum(F.when(F.col("_corrupt_record").isNotNull(), 1).otherwise(0)).alias("bad"))
            .collect()[0]
        )
        n, bad = int(agg["n"]), int(agg["bad"] or 0)

        notes = []
        if new_keys:
            notes.append(f"SCHEMA DRIFT - new columns added to Bronze: {new_keys}")
        if prev_rows:
            notes.append(f"batch re-processed: {prev_rows} existing rows replaced")
        if bad:
            notes.append(f"{bad} corrupt records flagged in _corrupt_record")

        log_run(run_id, batch_name, LAYER_BRONZE, load_type, run_mode, params, batch_path,
                start_time, "SUCCESS",
                rows_read=n,
                rows_inserted=0 if prev_rows else n,     # first time = inserted
                rows_updated=n if prev_rows else 0,      # re-run = replaced (counted as updated)
                rows_quarantined=bad,
                notes="; ".join(notes) or None)
        print(f"OK   {batch_name}: {n} rows" + (f" | new cols {new_keys}" if new_keys else ""))
        return True

    except Exception as e:
        log_run(run_id, batch_name, LAYER_BRONZE, load_type, run_mode, params, batch_path,
                start_time, "FAILURE", error_message=str(e))
        print(f"FAIL {batch_name}: {str(e)[:200]}")
        return False

# COMMAND ----------

run_id = new_run_id()
batches = select_batches(LAYER_BRONZE, load_type, run_mode, start_batch, end_batch)   # already sorted
print(f"run_id={run_id} | {len(batches)} batch(es) to process: {batches}")

results = {}
for b in batches:                       # sorted order = file order
    results[b] = process_bronze_batch(run_id, b)

failed = [b for b, ok in results.items() if not ok]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check the result

# COMMAND ----------

display(
    spark.table(LOG_TABLE)
    .where((F.col("run_id") == run_id))
    .select("batch_id", "layer", "status", "rows_read", "rows_inserted", "rows_updated",
            "rows_quarantined", "start_time", "end_time", "notes", "error_message")
    .orderBy("batch_id")
)

# COMMAND ----------

if failed:
    raise Exception(f"Bronze finished with failures in batches: {failed}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_rows FROM workspace.air_quality.bronze_readings

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT batch_id, COUNT(*) AS n FROM workspace.air_quality.bronze_readings GROUP BY batch_id ORDER BY batch_id