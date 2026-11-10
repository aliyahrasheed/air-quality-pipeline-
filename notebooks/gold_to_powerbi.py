# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Gold to Power BI Export
# MAGIC Exports Gold star-schema dimensional tables (`gold_dim_date`, `gold_dim_location`,
# MAGIC `gold_dim_parameter`, `gold_fact_measurements`) as CSV files into a dedicated Unity Catalog Volume
# MAGIC (`/Volumes/workspace/air_quality/landing/powerbi_export`) for direct ingestion into Power BI.

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "air_quality"

tables = ["gold_dim_date", "gold_dim_location", "gold_dim_parameter", "gold_fact_measurements"]
export_vol = f"/Volumes/{CATALOG}/{SCHEMA}/landing/powerbi_export"
dbutils.fs.mkdirs(export_vol)

for t in tables:
    full_table_name = f"{CATALOG}.{SCHEMA}.{t}"
    print(f"Exporting {full_table_name} to {export_vol}/{t} ...")
    spark.table(full_table_name) \
        .coalesce(1) \
        .write.mode("overwrite") \
        .option("header", True) \
        .csv(f"{export_vol}/{t}")
    print(f"✅ {t} exported successfully")
