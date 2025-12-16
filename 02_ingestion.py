# Databricks notebook source

# NOTEBOOK 02: BRONZE INGESTION
# IMPORTS


import requests
import time
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType,
    TimestampType, DateType
)
from pyspark.sql.functions import max as spark_max

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# CONFIG


API_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

MIN_MAGNITUDE = 1.0
BATCH_DAYS = 7                 # small batch = stable API
RATE_LIMIT_SEC = 1.2
MAX_RETRIES = 3

BRONZE_TABLE = "bronze_earthquakes"
WATERMARK_TABLE = "control_watermark"

print(f"Min Magnitude : {MIN_MAGNITUDE}")
print(f"Batch size    : {BATCH_DAYS} days")

# COMMAND ----------

# SCHEMA


bronze_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("time", TimestampType(), False),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("depth", DoubleType(), True),
    StructField("mag", DoubleType(), True),
    StructField("mag_type", StringType(), True),
    StructField("nst", IntegerType(), True),
    StructField("gap", DoubleType(), True),
    StructField("dmin", DoubleType(), True),
    StructField("rms", DoubleType(), True),
    StructField("net", StringType(), True),
    StructField("place", StringType(), True),
    StructField("type", StringType(), True),
    StructField("status", StringType(), True),
    StructField("horizontal_error", DoubleType(), True),
    StructField("depth_error", DoubleType(), True),
    StructField("mag_error", DoubleType(), True),
    StructField("ingestion_ts", TimestampType(), True),
    StructField("ingestion_date", DateType(), True),
    StructField("batch_id", StringType(), True)
])

# COMMAND ----------

# READ WATERMARK


print("\nReading watermark...")

wm_df = spark.sql(f"""
    SELECT watermark_value
    FROM {WATERMARK_TABLE}
    WHERE table_name = '{BRONZE_TABLE}'
""")

if wm_df.count() > 0:
    start_dt = wm_df.collect()[0]["watermark_value"]
else:
    start_dt = datetime(1990, 1, 1, tzinfo=timezone.utc)
    spark.sql(f"""
        INSERT INTO {WATERMARK_TABLE}
        VALUES (
            '{BRONZE_TABLE}',
            'time',
            TIMESTAMP '1990-01-01 00:00:00',
            current_timestamp(),
            0
        )
    """)

# COMMAND ----------

# normalize watermark to UTC
start_dt = start_dt.replace(tzinfo=timezone.utc)
end_dt = datetime.now(timezone.utc)

print(f"Loading from {start_dt} → {end_dt}")
print("⚠️ Job is RESUMABLE. Safe to re-run.\n")

# COMMAND ----------

# API FETCH FUNCTION


def fetch_batch(start_date, end_date):
    all_features = []
    offset = 1
    limit = 20000

    while True:
        params = {
            "format": "geojson",
            "starttime": start_date.strftime("%Y-%m-%d"),
            "endtime": end_date.strftime("%Y-%m-%d"),
            "minmagnitude": MIN_MAGNITUDE,
            "orderby": "time",
            "limit": limit,
            "offset": offset
        }

        for attempt in range(MAX_RETRIES):
            try:
                time.sleep(RATE_LIMIT_SEC)
                r = requests.get(API_URL, params=params, timeout=120)
                r.raise_for_status()

                feats = r.json().get("features", [])
                if not feats:
                    return all_features

                all_features.extend(feats)

                if len(feats) < limit:
                    return all_features

                offset += len(feats)
                break

            except Exception:
                if attempt == MAX_RETRIES - 1:
                    return all_features
                time.sleep(5 * (attempt + 1))


# COMMAND ----------

# INGESTION LOOP


total_written = 0
batch_no = 1

while start_dt < end_dt:
    batch_end = min(start_dt + timedelta(days=BATCH_DAYS), end_dt)

    print(f"[{batch_no}] Fetching {start_dt} → {batch_end}")

    features = fetch_batch(start_dt, batch_end)
    print(f"  Raw fetched: {len(features)}")

    rows = []
    ingestion_ts = datetime.now(timezone.utc)
    batch_id = ingestion_ts.strftime("%Y%m%d_%H%M%S")

    for f in features:
        try:
            props = f.get("properties", {})
            coords = f.get("geometry", {}).get("coordinates", [])

            t = props.get("time")
            if not t:
                continue

            event_time = datetime.fromtimestamp(t / 1000, tz=timezone.utc)

            # ---- PREVENT SAME-DAY DUPLICATES ----
            if event_time <= start_dt:
                continue

            rows.append({
                "event_id": f.get("id"),
                "time": event_time,
                "latitude": coords[1] if len(coords) > 1 else None,
                "longitude": coords[0] if len(coords) > 0 else None,
                "depth": coords[2] if len(coords) > 2 else None,
                "mag": props.get("mag"),
                "mag_type": props.get("magType"),
                "nst": props.get("nst"),
                "gap": props.get("gap"),
                "dmin": props.get("dmin"),
                "rms": props.get("rms"),
                "net": props.get("net"),
                "place": props.get("place"),
                "type": props.get("type"),
                "status": props.get("status"),
                "horizontal_error": props.get("horizontalError"),
                "depth_error": props.get("depthError"),
                "mag_error": props.get("magError"),
                "ingestion_ts": ingestion_ts,
                "ingestion_date": ingestion_ts.date(),
                "batch_id": batch_id
            })
        except:
            continue

    if rows:
        df = spark.createDataFrame(rows, bronze_schema)

        df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(BRONZE_TABLE)

        max_time = df.agg(spark_max("time")).collect()[0][0]

        spark.sql(f"""
            UPDATE {WATERMARK_TABLE}
            SET watermark_value = TIMESTAMP '{max_time}',
                last_updated = current_timestamp(),
                records_processed = records_processed + {len(rows)}
            WHERE table_name = '{BRONZE_TABLE}'
        """)

        total_written += len(rows)
        print(f"  Written: {len(rows):,} | Total: {total_written:,}")

        start_dt = max_time.replace(tzinfo=timezone.utc)
    else:
        start_dt = batch_end

    batch_no += 1


# COMMAND ----------

# SUMMARY


print("\nINGESTION COMPLETE")
spark.sql(f"SELECT COUNT(*) AS total_records FROM {BRONZE_TABLE}").show()
spark.sql(f"""
    SELECT * FROM {WATERMARK_TABLE}
    WHERE table_name = '{BRONZE_TABLE}'
""").show(truncate=False)