# Databricks notebook source

# NOTEBOOK 5: GOLD AGGREGATION
# Purpose : Analytics-ready aggregations for dashboards


from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum, avg, min, max, stddev, countDistinct,
    when, round as rnd, current_timestamp, to_date, year, month,
    dense_rank, lit, coalesce
)
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()


# LOAD SILVER DATA


silver_df = spark.table("silver_earthquakes")
record_count = silver_df.count()

print(f"Silver records: {record_count:,}")



# COMMAND ----------

# GOLD TABLE 1: REGIONAL RISK AGGREGATION


print("Creating regional risk aggregation...")

regional_agg = (
    silver_df
    .groupBy("tectonic_region", "region_name", "year", "month")
    .agg(
        count("*").alias("total_events"),
        rnd(avg("magnitude"), 3).alias("avg_magnitude"),
        rnd(max("magnitude"), 2).alias("max_magnitude"),
        rnd(min("magnitude"), 2).alias("min_magnitude"),
        rnd(coalesce(stddev("magnitude"), lit(0.0)), 3).alias("stddev_magnitude"),
        rnd(avg("depth_km"), 2).alias("avg_depth_km"),
        sum(when(col("depth_category") == "SHALLOW", 1).otherwise(0)).alias("shallow_count"),
        sum(when(col("depth_category") == "INTERMEDIATE", 1).otherwise(0)).alias("intermediate_count"),
        sum(when(col("depth_category") == "DEEP", 1).otherwise(0)).alias("deep_count"),
        sum(when(col("risk_level") == "CRITICAL", 1).otherwise(0)).alias("critical_count"),
        sum(when(col("risk_level") == "HIGH", 1).otherwise(0)).alias("high_risk_count"),
        sum(when(col("risk_level") == "MODERATE", 1).otherwise(0)).alias("moderate_count"),
        sum(when(col("tsunami_potential"), 1).otherwise(0)).alias("tsunami_count"),
        rnd(sum("energy_joules"), 2).alias("total_energy_joules")
    )
)

regional_agg = (
    regional_agg
    .withColumn(
        "risk_score",
        rnd(
            col("critical_count") * 50 +
            col("high_risk_count") * 20 +
            col("moderate_count") * 5 +
            col("max_magnitude") * 10,
            2
        )
    )
    .withColumn(
        "risk_level",
        when(col("risk_score") >= 100, "CRITICAL")
        .when(col("risk_score") >= 50, "HIGH")
        .when(col("risk_score") >= 20, "MODERATE")
        .when(col("risk_score") >= 5, "LOW")
        .otherwise("MINIMAL")
    )
    .withColumn("calculated_ts", current_timestamp())
)

regional_agg.write.format("delta").mode("overwrite").saveAsTable("gold_regional_risk")
print(f"Regional risk rows: {regional_agg.count():,}")

# COMMAND ----------

# GOLD TABLE 2: DAILY TEMPORAL METRICS


print("Creating temporal metrics...")

daily_agg = (
    silver_df
    .withColumn("event_date", to_date("event_time"))
    .groupBy("event_date")
    .agg(
        count("*").alias("total_events"),
        rnd(avg("magnitude"), 3).alias("avg_magnitude"),
        rnd(max("magnitude"), 2).alias("max_magnitude"),
        countDistinct("tectonic_region").alias("active_regions"),
        sum(when(col("risk_level") == "CRITICAL", 1).otherwise(0)).alias("critical_events"),
        sum(when(col("risk_level") == "HIGH", 1).otherwise(0)).alias("high_risk_events"),
        sum(when(col("tsunami_potential"), 1).otherwise(0)).alias("tsunami_events"),
        rnd(sum("energy_joules"), 2).alias("total_energy")
    )
    .withColumn("year", year("event_date"))
    .withColumn("month", month("event_date"))
)

window_7d = Window.orderBy("event_date").rowsBetween(-6, 0)
window_30d = Window.orderBy("event_date").rowsBetween(-29, 0)

daily_agg = (
    daily_agg
    .withColumn("rolling_7d_count", sum("total_events").over(window_7d))
    .withColumn("rolling_30d_count", sum("total_events").over(window_30d))
    .withColumn(
        "is_anomaly",
        when(col("rolling_7d_count") > 0,
             col("total_events") > (col("rolling_7d_count") / 7 * 2))
        .otherwise(False)
    )
    .withColumn("calculated_ts", current_timestamp())
)

daily_agg.write.format("delta").mode("overwrite").saveAsTable("gold_temporal_metrics")
print(f"Temporal rows: {daily_agg.count():,}")


# COMMAND ----------

# GOLD TABLE 3:  SUMMARY


print("summary")

kpi_df = (
    silver_df
    .agg(
        count("*").alias("total_earthquakes"),
        rnd(avg("magnitude"), 2).alias("avg_magnitude"),
        max("magnitude").alias("max_magnitude"),
        min("magnitude").alias("min_magnitude"),
        countDistinct("tectonic_region").alias("active_regions"),
        sum(when(col("risk_level") == "CRITICAL", 1).otherwise(0)).alias("critical_events"),
        sum(when(col("risk_level") == "HIGH", 1).otherwise(0)).alias("high_risk_events"),
        sum(when(col("tsunami_potential"), 1).otherwise(0)).alias("tsunami_events"),
        rnd(sum("energy_joules"), 2).alias("total_energy_joules"),
        rnd(avg("depth_km"), 1).alias("avg_depth_km"),
        min("event_time").alias("data_start"),
        max("event_time").alias("data_end")
    )
    .withColumn("refresh_ts", current_timestamp())
)

kpi_df.write.format("delta").mode("overwrite").saveAsTable("gold_kpi_summary")
print("KPI summary created")


# COMMAND ----------

# GOLD TABLE 4: REGION SUMMARY


print("Creating region summary...")

region_summary = (
    silver_df
    .groupBy("tectonic_region", "region_name")
    .agg(
        count("*").alias("total_events"),
        rnd(avg("magnitude"), 2).alias("avg_magnitude"),
        max("magnitude").alias("max_magnitude"),
        sum(when(col("risk_level") == "CRITICAL", 1).otherwise(0)).alias("critical_events"),
        sum(when(col("risk_level") == "HIGH", 1).otherwise(0)).alias("high_risk_events"),
        sum(when(col("tsunami_potential"), 1).otherwise(0)).alias("tsunami_events"),
        rnd(avg("latitude"), 2).alias("center_lat"),
        rnd(avg("longitude"), 2).alias("center_lon")
    )
)

rank_window = Window.orderBy(col("critical_events").desc(), col("total_events").desc())

region_summary = (
    region_summary
    .withColumn("risk_rank", dense_rank().over(rank_window))
    .withColumn("calculated_ts", current_timestamp())
)

region_summary.write.format("delta").mode("overwrite").saveAsTable("gold_region_summary")
print(f"Region summary rows: {region_summary.count():,}")




print("GOLD AGGREGATION COMPLETE")


for t in [
    "gold_regional_risk",
    "gold_temporal_metrics",
    "gold_kpi_summary",
    "gold_region_summary"
]:
    print(f"{t}: {spark.table(t).count():,} rows")

print("\nTop Risk Regions:")
spark.sql("""
    SELECT tectonic_region, total_events, critical_events, max_magnitude, risk_rank
    FROM gold_region_summary
    ORDER BY risk_rank
    LIMIT 10
""").show()
