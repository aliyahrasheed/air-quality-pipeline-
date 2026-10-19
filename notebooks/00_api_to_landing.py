# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - API to Landing (OpenAQ Live Ingestion)
# MAGIC Extracts live air quality sensor data directly from the **OpenAQ API v3** and lands it as raw JSON Lines files
# MAGIC in the landing Volume (`/Volumes/workspace/air_quality/landing/raw/incremental/batch_<date>/`).
# MAGIC
# MAGIC This notebook connects your pipeline directly to the live OpenAQ data feed while maintaining
# MAGIC full Medallion Architecture separation (Raw Landing -> Bronze -> Silver).
# MAGIC
# MAGIC | Parameter | Default | Description |
# MAGIC |---|---|---|
# MAGIC | `country` | `PK` | Country code to fetch (e.g. `PK`, `IN`, `US`) |
# MAGIC | `batch_date` | `current_date` | Date identifier for the batch folder (e.g. `20261019`) |
# MAGIC | `limit` | `100` | Number of recent measurements to extract |
# MAGIC | `api_key` | `` | Optional OpenAQ API key (optional for standard rate limits) |

# COMMAND ----------

# MAGIC %run ./00_config_helpers

# COMMAND ----------

import json
import os
import urllib.request
import urllib.parse
from datetime import datetime, timezone

dbutils.widgets.text("country", "PK")
dbutils.widgets.text("batch_date", datetime.now(timezone.utc).strftime("%Y%m%d"))
dbutils.widgets.text("limit", "100")
dbutils.widgets.text("api_key", "")

country = dbutils.widgets.get("country").strip().upper() or "PK"
batch_date = dbutils.widgets.get("batch_date").strip() or datetime.now(timezone.utc).strftime("%Y%m%d")
limit = int(dbutils.widgets.get("limit").strip() or "100")
api_key = dbutils.widgets.get("api_key").strip()

batch_name = f"batch_{batch_date}"
target_dir = f"{VOLUME_ROOT}/incremental/{batch_name}"
target_file = f"{target_dir}/inc_{batch_name}.jsonl"

params = {"country": country, "batch_date": batch_date, "limit": limit}
print(f"API to Landing Config:")
print(f"  Country: {country}")
print(f"  Target batch folder: {target_dir}")
print(f"  Target file: {target_file}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Query OpenAQ API v3 & Format Records

# COMMAND ----------

start_time = datetime.now(timezone.utc)
run_id = new_run_id()

headers = {
    "User-Agent": "AirQualityDataPipeline/1.0",
    "Accept": "application/json"
}
if api_key:
    headers["X-API-Key"] = api_key

records = []
try:
    # 1. Fetch active locations for the specified country
    loc_url = f"https://api.openaq.org/v3/locations?countries_id={country}&limit=10"
    req = urllib.request.Request(loc_url, headers=headers)
    with urllib.request.urlopen(req, timeout=30) as resp:
        loc_data = json.loads(resp.read().decode("utf-8"))
        locations = loc_data.get("results", [])

    print(f"Discovered {len(locations)} locations for country {country}")

    # 2. For each location, fetch recent sensor measurements
    for loc in locations:
        if len(records) >= limit:
            break
        loc_id = loc.get("id")
        loc_name = loc.get("name")
        city = loc.get("locality") or (loc.get("timezone", "").split("/")[-1] if loc.get("timezone") else country)
        country_code = country
        coords = loc.get("coordinates") or {}
        lat = coords.get("latitude")
        lon = coords.get("longitude")

        sensors = loc.get("sensors", [])
        for sensor in sensors:
            if len(records) >= limit:
                break
            sensor_id = sensor.get("id")
            param_obj = sensor.get("parameter", {})
            param_id = param_obj.get("id")
            param_name = param_obj.get("name")
            param_units = param_obj.get("units")
            param_display = param_obj.get("displayName")

            # Fetch recent measurements for this sensor
            try:
                meas_url = f"https://api.openaq.org/v3/sensors/{sensor_id}/measurements?limit=10"
                m_req = urllib.request.Request(meas_url, headers=headers)
                with urllib.request.urlopen(m_req, timeout=15) as m_resp:
                    m_data = json.loads(m_resp.read().decode("utf-8"))
                    meas_list = m_data.get("results", [])

                for m in meas_list:
                    period_obj = m.get("period", {})
                    rec = {
                        "value": str(m.get("value")) if m.get("value") is not None else None,
                        "flagInfo": m.get("flagInfo", {"hasFlags": False}),
                        "parameter": {
                            "id": str(param_id) if param_id is not None else None,
                            "name": param_name,
                            "units": param_units,
                            "displayName": param_display
                        },
                        "period": {
                            "label": period_obj.get("label", "1hour"),
                            "interval": period_obj.get("interval", "01:00:00"),
                            "datetimeFrom": period_obj.get("datetimeFrom", {}),
                            "datetimeTo": period_obj.get("datetimeTo", {})
                        },
                        "coordinates": {"latitude": str(lat), "longitude": str(lon)} if lat and lon else None,
                        "summary": {k: str(v) if v is not None else None for k, v in m.get("summary", {}).items()} if m.get("summary") else None,
                        "coverage": {k: str(v) if v is not None else None for k, v in m.get("coverage", {}).items()} if m.get("coverage") else None,
                        "_sensor_id": str(sensor_id),
                        "_location_id": str(loc_id),
                        "_location_name": loc_name,
                        "_city": city,
                        "_country": country_code,
                        "_parameter": param_name,
                        "_unit": param_units
                    }
                    records.append(rec)
                    if len(records) >= limit:
                        break
            except Exception as sensor_err:
                # Individual sensor errors do not halt entire batch
                continue

    print(f"Total live records harvested: {len(records)}")

    # 3. Write records as JSON Lines directly into Landing Volume
    if records:
        content = "\n".join(json.dumps(r) for r in records) + "\n"
        os.makedirs(target_dir, exist_ok=True)
        with open(target_file, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"Successfully landed {len(records)} records to {target_file}")

        # 4. Write audit log
        log_run(run_id, batch_name, "API-to-Landing", "incremental", "live_api",
                params, target_file, start_time, "SUCCESS",
                rows_read=len(records), rows_inserted=len(records),
                notes=f"Extracted {len(records)} records from OpenAQ API v3 for {country}")
    else:
        print("No records retrieved. Writing audit log.")
        log_run(run_id, batch_name, "API-to-Landing", "incremental", "live_api",
                params, target_file, start_time, "SUCCESS",
                rows_read=0, rows_inserted=0,
                notes="0 records returned by OpenAQ API")

except Exception as e:
    err = str(e)
    print(f"API-to-Landing Failure: {err}")
    log_run(run_id, batch_name, "API-to-Landing", "incremental", "live_api",
            params, target_file, start_time, "FAILURE", error_message=err)
    raise
