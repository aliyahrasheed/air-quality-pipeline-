# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 00 - Setup Landing Data (Manual Data Ingestion Setup)
# MAGIC Prepares the Unity Catalog Volume directory structure and copies the extracted manual
# MAGIC batch datasets (`full_load/batch_0001` .. `batch_0010` and `incremental/batch_yyyymmdd`)
# MAGIC from the workspace repository into the Landing Volume (`/Volumes/workspace/air_quality/landing/raw/`).
# MAGIC
# MAGIC Run this notebook once to initialize the volume before executing `01_raw_to_bronze`.

# COMMAND ----------

# MAGIC %run ./00_config_helpers

# COMMAND ----------

import os

# Create landing volume folder hierarchy if not present
landing_full = f"{VOLUME_ROOT}/full_load"
landing_inc = f"{VOLUME_ROOT}/incremental"

dbutils.fs.mkdirs(landing_full)
dbutils.fs.mkdirs(landing_inc)

# Define source path for manual data inside the Databricks repo
# Adjust this path if your workspace repo clone location differs
repo_root = "/Workspace/Users/l242544@lhr.nu.edu.pk/air-quality-pipeline-/data/raw"

print(f"Destination Volume Root: {VOLUME_ROOT}")
print(f"Source Repo Data Root:   {repo_root}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Copy Full Load Batches to Volume

# COMMAND ----------

try:
    full_load_src = f"{repo_root}/full_load"
    full_batches = sorted([b for b in dbutils.fs.ls(full_load_src) if b.isDir()], key=lambda x: x.name)
    print(f"Found {len(full_batches)} full_load batches to copy.")
    
    for b in full_batches:
        dst = f"{landing_full}/{b.name.rstrip('/')}"
        dbutils.fs.cp(b.path, dst, recurse=True)
        print(f"  ✓ Copied full-load batch: {b.name}")
except Exception as e:
    print(f"Warning copying full_load: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Copy Incremental Batches to Volume

# COMMAND ----------

try:
    inc_src = f"{repo_root}/incremental"
    inc_batches = sorted([b for b in dbutils.fs.ls(inc_src) if b.isDir()], key=lambda x: x.name)
    print(f"Found {len(inc_batches)} incremental batches to copy.")
    
    for b in inc_batches:
        # Avoid copying temporary or manual duplicate folders
        if "manual_batch" in b.name:
            continue
        dst = f"{landing_inc}/{b.name.rstrip('/')}"
        dbutils.fs.cp(b.path, dst, recurse=True)
        print(f"  ✓ Copied incremental batch: {b.name}")
except Exception as e:
    print(f"Warning copying incremental: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verification

# COMMAND ----------

print("Volume Landing Zone Verification:")
print("Full Load Batches:", list_batches("full_load"))
print("Incremental Batches:", list_batches("incremental"))
