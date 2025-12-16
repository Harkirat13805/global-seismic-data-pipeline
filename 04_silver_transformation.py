# Databricks notebook source

# NOTEBOOK 4: SILVER TRANSFORMATION
# Purpose : Clean, enrich, and UPSERT to Silver


from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, coalesce, lower, broadcast, row_number,
    year, month, dayofmonth, hour, dayofweek, quarter,
    current_timestamp, max as spark_max
)
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, DoubleType, BooleanType
from datetime import datetime
import math

spark = SparkSession.builder.getOrCreate()


# STEP 1: READ SILVER WATERMARK


wm_df = spark.sql("""
    SELECT watermark_value 
    FROM control_watermark 
    WHERE table_name = 'silver_earthquakes'
""")

if wm_df.count() > 0:
    silver_watermark = wm_df.collect()[0][0]
    print(f"Silver watermark: {silver_watermark}")
    bronze_df = spark.table("bronze_earthquakes").filter(col("time") > silver_watermark)
else:
    print("First run – processing all bronze data")
    bronze_df = spark.table("bronze_earthquakes")

    spark.sql("""
        INSERT INTO control_watermark VALUES (
            'silver_earthquakes', 'time',
            TIMESTAMP '2000-01-01 00:00:00',
            current_timestamp(), 0
        )
    """)

record_count = bronze_df.count()
print(f"Records to process: {record_count:,}")

if record_count == 0:
    print("No new records. Exiting Silver processing.")
    raise SystemExit(0)



# COMMAND ----------

# STEP 2: DATA VALIDATION & CLEANING


print("Validating data...")

valid_df = bronze_df.filter(
    col("event_id").isNotNull() &
    col("time").isNotNull() &
    col("latitude").between(-90, 90) &
    col("longitude").between(-180, 180)
)

valid_df = (
    valid_df
    .withColumn(
        "depth",
        when(col("depth").isNull(), 33.0)
        .when(col("depth") < 0, 0.0)
        .when(col("depth") > 700, 700.0)
        .otherwise(col("depth"))
    )
    .withColumn("mag", coalesce(col("mag"), lit(2.5)))
    .withColumn(
        "type",
        when(lower(col("type")).isin("earthquake", "eq"), "earthquake")
        .otherwise(coalesce(lower(col("type")), lit("earthquake")))
    )
)

print(f"Valid records after cleaning: {valid_df.count():,}")

# COMMAND ----------

# STEP 3: GEOSPATIAL ENRICHMENT


print("Assigning tectonic regions...")

regions_df = broadcast(
    spark.table("ref_tectonic_regions")
    .select("region_code", "region_name", "min_lon", "max_lon", "min_lat", "max_lat", "priority")
)

enriched_df = valid_df.crossJoin(regions_df).filter(
    (col("latitude") >= col("min_lat")) &
    (col("latitude") <= col("max_lat")) &
    (col("longitude") >= col("min_lon")) &
    (col("longitude") <= col("max_lon"))
)

window = Window.partitionBy("event_id").orderBy("priority")

enriched_df = (
    enriched_df
    .withColumn("rn", row_number().over(window))
    .filter(col("rn") == 1)
    .drop("rn", "min_lon", "max_lon", "min_lat", "max_lat", "priority")
    .withColumnRenamed("region_code", "tectonic_region")
)

# Handle unmatched
matched_ids = enriched_df.select("event_id")
unmatched = (
    valid_df.join(matched_ids, "event_id", "left_anti")
    .withColumn("tectonic_region", lit("OTHER"))
    .withColumn("region_name", lit("Other Regions"))
)

enriched_df = enriched_df.unionByName(unmatched, allowMissingColumns=True)

print(f"Records after geospatial enrichment: {enriched_df.count():,}")

# COMMAND ----------

# STEP 4: PHYSICS & RISK CALCULATIONS


def calc_energy(m):
    return math.pow(10, 1.5 * m + 4.8) if m is not None else None

def depth_cat(d):
    if d < 70: return "SHALLOW"
    if d < 300: return "INTERMEDIATE"
    return "DEEP"

def risk(m):
    if m >= 7: return "CRITICAL"
    if m >= 6: return "HIGH"
    if m >= 5: return "MODERATE"
    if m >= 4: return "LOW"
    return "MINIMAL"

from pyspark.sql.functions import udf
energy_udf = udf(calc_energy, DoubleType())
depth_udf = udf(depth_cat, StringType())
risk_udf = udf(risk, StringType())

physics_df = (
    enriched_df
    .withColumn("energy_joules", energy_udf(col("mag")))
    .withColumn("depth_category", depth_udf(col("depth")))
    .withColumn("risk_level", risk_udf(col("mag")))
    .withColumn(
        "tsunami_potential",
        (col("mag") >= 7.0) & (col("depth") < 70)
    )
)

# COMMAND ----------

# STEP 5: TEMPORAL FEATURES


silver_df = (
    physics_df
    .withColumn("event_time", col("time"))
    .withColumn("depth_km", col("depth"))
    .withColumn("magnitude", col("mag"))
    .withColumn("hour_of_day", hour(col("time")))
    .withColumn("day_of_week", dayofweek(col("time")))
    .withColumn("day_of_month", dayofmonth(col("time")))
    .withColumn("month", month(col("time")))
    .withColumn("quarter", quarter(col("time")))
    .withColumn("year", year(col("time")))
    .withColumn("processed_ts", current_timestamp())
)



# COMMAND ----------

# STEP 6: UPSERT TO SILVER


table_exists = spark.catalog.tableExists("silver_earthquakes")

if table_exists:
    silver_df.createOrReplaceTempView("silver_updates")

    spark.sql("""
        MERGE INTO silver_earthquakes t
        USING silver_updates s
        ON t.event_id = s.event_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("Silver MERGE completed")
else:
    silver_df.write.format("delta").mode("overwrite").saveAsTable("silver_earthquakes")
    print("Silver table created")

# COMMAND ----------

# STEP 7: UPDATE WATERMARK


new_watermark = silver_df.agg(spark_max("event_time")).collect()[0][0]
processed = silver_df.count()

spark.sql(f"""
    UPDATE control_watermark
    SET watermark_value = TIMESTAMP '{new_watermark}',
        last_updated = current_timestamp(),
        records_processed = records_processed + {processed}
    WHERE table_name = 'silver_earthquakes'
""")

# COMMAND ----------

print("SILVER TRANSFORMATION COMPLETE")


spark.sql("SELECT COUNT(*) AS total FROM silver_earthquakes").show()
spark.sql("SELECT risk_level, COUNT(*) FROM silver_earthquakes GROUP BY risk_level").show()
