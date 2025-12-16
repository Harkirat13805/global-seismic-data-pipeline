# Databricks notebook source

# NOTEBOOK 3: BRONZE PROCESSING
# Purpose : Quality checks + deduplication


from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, row_number,
    min as spark_min, max as spark_max, avg
)
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()


# LOAD BRONZE DATA


bronze_df = spark.table("bronze_earthquakes")
initial_count = bronze_df.count()

print(f"Bronze table records: {initial_count:,}")


# DATA QUALITY REPORT



print("DATA QUALITY REPORT")


print("\n--- NULL ANALYSIS ---")

# split by type
non_numeric_cols = ["event_id", "time"]
numeric_cols = ["latitude", "longitude", "depth", "mag"]

# Non-numeric
for c in non_numeric_cols:
    null_cnt = bronze_df.filter(col(c).isNull()).count()
    pct = (null_cnt / initial_count * 100) if initial_count else 0
    status = "OK" if pct < 5 else "WARNING"
    print(f"  {c}: {null_cnt:,} nulls ({pct:.2f}%) - {status}")

# Numeric
for c in numeric_cols:
    null_cnt = bronze_df.filter(col(c).isNull()).count()
    pct = (null_cnt / initial_count * 100) if initial_count else 0
    status = "OK" if pct < 5 else "WARNING"
    print(f"  {c}: {null_cnt:,} nulls ({pct:.2f}%) - {status}")



# COMMAND ----------

# RANGE VALIDATION


print("\n--- RANGE VALIDATION ---")

stats = bronze_df.select(
    spark_min("mag").alias("min_mag"),
    spark_max("mag").alias("max_mag"),
    avg("mag").alias("avg_mag"),
    spark_min("depth").alias("min_depth"),
    spark_max("depth").alias("max_depth"),
    spark_min("latitude").alias("min_lat"),
    spark_max("latitude").alias("max_lat"),
    spark_min("time").alias("earliest"),
    spark_max("time").alias("latest")
).collect()[0]

print(f"  Magnitude : {stats['min_mag']} → {stats['max_mag']} (avg: {stats['avg_mag']:.2f})")
print(f"  Depth     : {stats['min_depth']} → {stats['max_depth']} km")
print(f"  Latitude  : {stats['min_lat']} → {stats['max_lat']}")
print(f"  Date range: {stats['earliest']} → {stats['latest']}")

# COMMAND ----------

# DUPLICATE CHECK


print("\n--- DUPLICATE CHECK ---")

distinct_events = bronze_df.select("event_id").distinct().count()
duplicate_count = initial_count - distinct_events

print(f"  Total records       : {initial_count:,}")
print(f"  Distinct event_ids  : {distinct_events:,}")
print(f"  Duplicate records   : {duplicate_count:,}")


# COMMAND ----------

# DEDUPLICATION (KEEP LATEST INGESTION)


if duplicate_count > 0:
    print("\nRemoving duplicates (keeping latest ingestion)...")

    window = Window.partitionBy("event_id").orderBy(col("ingestion_ts").desc())

    deduped_df = (
        bronze_df
        .withColumn("row_num", row_number().over(window))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    final_count = deduped_df.count()

    deduped_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "false") \
        .saveAsTable("bronze_earthquakes")

    print(f"  Removed duplicates : {duplicate_count:,}")
    print(f"  Final count        : {final_count:,}")
else:
    print("\nNo duplicates found — data is clean")
    final_count = initial_count


# COMMAND ----------

# EVENT TYPE DISTRIBUTION


print("\n--- EVENT TYPE DISTRIBUTION ---")

bronze_df.groupBy("type") \
    .count() \
    .orderBy(col("count").desc()) \
    .show(truncate=False)

# COMMAND ----------

# SUMMARY



print("BRONZE PROCESSING COMPLETE")


print(f"Initial record count : {initial_count:,}")
print(f"Final record count   : {final_count:,}")