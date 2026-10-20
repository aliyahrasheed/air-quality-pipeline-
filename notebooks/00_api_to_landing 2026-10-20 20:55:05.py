# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 00 - API to Landing (OpenAQ v3)
# MAGIC Pulls measurements from OpenAQ directly into ordered daily JSONL batches. No manual CSV upload is needed.
# MAGIC Configure an OpenAQ API key in the `api_key` widget. Full load uses `start_date` through `end_date`;
# MAGIC incremental load uses the requested date window (normally the last 1-2 days to capture late data).
# MAGIC
# MAGIC **Run this notebook first**, then run `01_raw_to_bronze`, then `02_bronze_to_silver`.

# COMMAND ----------

# MAGIC %run ./00_config_helpers

# COMMAND ----------

import json
import urllib.request
import urllib.parse
from datetime import datetime, timezone, date, timedelta

dbutils.widgets.dropdown("load_type", "incremental", ["full_load", "incremental"])
dbutils.widgets.text("country", "PK")
dbutils.widgets.text("start_date", (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat())
dbutils.widgets.text("end_date", datetime.now(timezone.utc).date().isoformat())
dbutils.widgets.text("limit_per_sensor_day", "100")
dbutils.widgets.text("max_locations", "10")
dbutils.widgets.text("api_key", "5d4df12bc166f56725b4d983d79d2d851f45eff960459060d1e94f649bb0b428")

load_type = dbutils.widgets.get("load_type").strip()
country = dbutils.widgets.get("country").strip().upper() or "PK"
start_date = date.fromisoformat(dbutils.widgets.get("start_date").strip())
end_date = date.fromisoformat(dbutils.widgets.get("end_date").strip())
limit = int(dbutils.widgets.get("limit_per_sensor_day").strip() or "100")
max_locations = int(dbutils.widgets.get("max_locations").strip() or "10")
api_key = dbutils.widgets.get("api_key").strip()

if end_date < start_date:
    raise ValueError("end_date must be on or after start_date")
if not api_key:
    raise ValueError("OpenAQ v3 requires an API key. Obtain one from https://openaq.org and enter it in the api_key widget.")

headers = {"User-Agent": "AirQualityDataPipeline/1.0", "Accept": "application/json", "X-API-Key": api_key}
base = f"{VOLUME_ROOT}/{load_type}"
params = {"load_type": load_type, "country": country, "start_date": str(start_date),
          "end_date": str(end_date), "limit_per_sensor_day": limit, "max_locations": max_locations}
print(f"API ingestion parameters: {params}")

# COMMAND ----------

def get_json(url, timeout=30):
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))

def iso_utc(d, end=False):
    # OpenAQ date filters are UTC ISO timestamps.
    return f"{d.isoformat()}T{'23:59:59' if end else '00:00:00'}Z"

run_id = new_run_id()
extract_suffix = datetime.now(timezone.utc).strftime("%H%M%S")
locations_url = "https://api.openaq.org/v3/locations?" + urllib.parse.urlencode(
    {"iso": country, "limit": max_locations, "page": 1}
)
all_locations = get_json(locations_url).get("results", [])
print(f"Locations returned by API: {len(all_locations)}")

# COMMAND ----------

print("Number of locations:", len(all_locations))

if all_locations:
    print("First location fields:", list(all_locations[0].keys()))
    print("First location name:", all_locations[0].get("name"))
    print("Sensors in first location:", len(all_locations[0].get("sensors") or []))

# COMMAND ----------

for loc in all_locations:
    print("Location:", loc.get("name"))
    for sensor in loc.get("sensors") or []:
        print("Sensor ID:", sensor.get("id"))
        print("Parameter:", sensor.get("parameter"))

# COMMAND ----------

test_url = (
    "https://api.openaq.org/v3/sensors/23455/measurements?"
    + urllib.parse.urlencode({
        "datetime_from": "2026-10-19T00:00:00Z",
        "datetime_to": "2026-10-19T23:59:59Z",
        "limit": 5,
        "page": 1
    })
)

try:
    test_result = get_json(test_url, timeout=20)
    print("Measurements returned:", len(test_result.get("results", [])))
    print("API metadata:", test_result.get("meta"))
except Exception as e:
    print("Request failed:", repr(e))

# COMMAND ----------

test_url = (
    "https://api.openaq.org/v3/sensors/23455/measurements?"
    + urllib.parse.urlencode({"limit": 5, "page": 1})
)

try:
    test_result = get_json(test_url, timeout=20)
    results = test_result.get("results", [])
    print("Measurements returned:", len(results))
    print("API metadata:", test_result.get("meta"))

    for item in results:
        print(item)
except Exception as e:
    print("Request failed:", repr(e))

# COMMAND ----------

test_url = (
    "https://api.openaq.org/v3/sensors/23455/measurements?"
    + urllib.parse.urlencode({
        "datetime_from": "2019-06-16T00:00:00Z",
        "datetime_to": "2019-06-17T00:00:00Z",
        "limit": 5,
        "page": 1
    })
)

test_result = get_json(test_url, timeout=20)
results = test_result.get("results", [])

print("Measurements returned:", len(results))
for item in results:
    print(item.get("value"), item.get("period", {}).get("datetimeFrom", {}).get("utc"))

# COMMAND ----------


# One folder per day per extraction.
# The date prefix preserves chronological ordering, while the extraction
# suffix prevents later pulls from overwriting earlier batches.
day = start_date

while day <= end_date:
    batch_name = f"batch_{day.strftime('%Y%m%d')}_{extract_suffix}"
    target_dir = f"{base}/{batch_name}"
    target_file = f"{target_dir}/openaq_{batch_name}.jsonl"
    batch_start = datetime.now(timezone.utc)

    records = []
    day_params = {**params, "batch_date": day.isoformat()}

    try:
        for loc in all_locations:
            loc_id = loc.get("id")
            loc_name = loc.get("name")

            city = (
                loc.get("locality")
                or (
                    loc.get("timezone", "").split("/")[-1]
                    if loc.get("timezone")
                    else country
                )
            )

            coords = loc.get("coordinates") or {}
            lat = coords.get("latitude")
            lon = coords.get("longitude")

            for sensor in (loc.get("sensors") or []):
                sensor_id = sensor.get("id")
                parameter = sensor.get("parameter") or {}

                query = {
                    "datetime_from": iso_utc(day),
                    "datetime_to": iso_utc(day, end=True),
                    "limit": limit,
                    "page": 1,
                }

                url = (
                    f"https://api.openaq.org/v3/sensors/{sensor_id}/measurements?"
                    f"{urllib.parse.urlencode(query)}"
                )

                try:
                    measurements = get_json(url, timeout=20).get("results", [])
                except Exception as sensor_error:
                    print(
                        f"Skipping sensor {sensor_id} for {day}: "
                        f"{sensor_error}"
                    )
                    continue

                for m in measurements:
                    period = m.get("period") or {}

                    rec = {
                        "value": (
                            str(m.get("value"))
                            if m.get("value") is not None
                            else None
                        ),
                        "flagInfo": m.get("flagInfo") or {"hasFlags": False},
                        "parameter": {
                            "id": (
                                str(parameter.get("id"))
                                if parameter.get("id") is not None
                                else None
                            ),
                            "name": parameter.get("name"),
                            "units": parameter.get("units"),
                            "displayName": parameter.get("displayName"),
                        },
                        "period": {
                            "label": period.get("label", "1hour"),
                            "interval": period.get("interval", "01:00:00"),
                            "datetimeFrom": period.get("datetimeFrom") or {},
                            "datetimeTo": period.get("datetimeTo") or {},
                        },
                        "coordinates": (
                            {
                                "latitude": str(lat),
                                "longitude": str(lon),
                            }
                            if lat is not None and lon is not None
                            else None
                        ),
                        "summary": {
                            k: str(v) if v is not None else None
                            for k, v in (m.get("summary") or {}).items()
                        } or None,
                        "coverage": {
                            k: (
                                v
                                if k in ("datetimeFrom", "datetimeTo")
                                else str(v) if v is not None else None
                            )
                            for k, v in (m.get("coverage") or {}).items()
                        } or None,
                        "_sensor_id": (
                            str(sensor_id) if sensor_id is not None else None
                        ),
                        "_location_id": (
                            str(loc_id) if loc_id is not None else None
                        ),
                        "_location_name": loc_name,
                        "_city": city,
                        "_country": country,
                        "_parameter": parameter.get("name"),
                        "_unit": parameter.get("units"),
                        "_api_extracted_at": datetime.now(
                            timezone.utc
                        ).isoformat(),
                        "_source_api": "OpenAQ v3",
                    }

                    records.append(rec)

        if records:
            content = "\n".join(
                json.dumps(r, ensure_ascii=False) for r in records
            ) + "\n"

            dbutils.fs.mkdirs(target_dir)
            dbutils.fs.put(target_file, content, overwrite=True)

        log_run(
            run_id,
            batch_name,
            "API-to-Landing",
            load_type,
            "date_range",
            day_params,
            target_file,
            batch_start,
            "SUCCESS",
            rows_read=len(records),
            rows_inserted=len(records),
            notes=(
                f"OpenAQ v3; {len(records)} records for {country} "
                f"on {day}; extracted_at_utc"
            ),
        )

        print(f"{batch_name}: landed {len(records)} records")

    except Exception as exc:
        log_run(
            run_id,
            batch_name,
            "API-to-Landing",
            load_type,
            "date_range",
            day_params,
            target_file,
            batch_start,
            "FAILURE",
            error_message=str(exc),
        )
        raise

    day += timedelta(days=1)

print(
    "API landing complete. Run Raw-to-Bronze and then Bronze-to-Silver "
    "with the same load_type and batch range."
)


# COMMAND ----------

display(dbutils.fs.ls("/Volumes/workspace/air_quality/landing/raw/incremental"))

# COMMAND ----------

display(dbutils.fs.ls(
    "/Volumes/workspace/air_quality/landing/raw/incremental/batch_20190616_175026"
))

# COMMAND ----------

file_path = "/Volumes/workspace/air_quality/landing/raw/incremental/batch_20190616_175026/openaq_batch_20190616_175026.jsonl"

lines = dbutils.fs.head(file_path, 10000)
print(lines[:1000])

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/air_quality/landing/raw")