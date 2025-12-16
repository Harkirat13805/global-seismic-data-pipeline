# Databricks notebook source
# Databricks notebook source
# ============================================================
# NOTEBOOK 08: PHYSICS CALCULATIONS
# Purpose : Apply physics formulas to earthquake data
# Platform: Databricks Community Edition
# Note    : No ML - just physics-based enrichment
# ============================================================

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, log10, pow, lit, round as rnd, current_timestamp,
    avg, count, max as spark_max, min as spark_min, sum as spark_sum
)

spark = SparkSession.builder.getOrCreate()

print("="*60)
print("PHYSICS-BASED EARTHQUAKE ANALYSIS")
print("="*60)

# COMMAND ----------

# ============================================================
# LOAD SILVER DATA
# ============================================================

silver_df = spark.table("silver_earthquakes")
print(f"Total records: {silver_df.count():,}")

# COMMAND ----------

# ============================================================
# PHYSICS FORMULAS APPLIED
# ============================================================

print("\n--- APPLYING PHYSICS FORMULAS ---")

# 1. GUTENBERG-RICHTER ENERGY RELEASE
#    Formula: log10(E) = 1.5*M + 4.8
#    E = Energy in Joules
#    M = Magnitude

# 2. MODIFIED MERCALLI INTENSITY (Simplified)
#    Formula: MMI = 1.5*M - 2.5*log10(D) + 2.0
#    D = Depth in km
#    MMI = Intensity scale (I to XII)

# 3. SEISMIC MOMENT
#    Formula: log10(M0) = 1.5*M + 9.1
#    M0 = Seismic moment in Newton-meters

# 4. RUPTURE LENGTH (Wells & Coppersmith, 1994)
#    Formula: log10(L) = 0.74*M - 3.55
#    L = Rupture length in km

# 5. OMORI'S LAW (Aftershock decay)
#    Formula: n(t) = K / (t + c)^p
#    Typical: p ≈ 1.0, c ≈ 0.05 days

# 6. BATH'S LAW
#    Largest aftershock ≈ Mainshock magnitude - 1.2

physics_df = silver_df.withColumn(
    # Gutenberg-Richter Energy (in Joules, log scale for display)
    "energy_joules_log",
    rnd(lit(1.5) * col("magnitude") + lit(4.8), 2)
).withColumn(
    # Modified Mercalli Intensity
    "mercalli_intensity",
    rnd(lit(1.5) * col("magnitude") - lit(2.5) * log10(col("depth_km") + lit(1)) + lit(2.0), 1)
).withColumn(
    # Mercalli Scale (Roman numerals approximation)
    "mercalli_scale",
    when(col("mercalli_intensity") >= 10, "X+ (Extreme)")
    .when(col("mercalli_intensity") >= 8, "VIII-IX (Severe)")
    .when(col("mercalli_intensity") >= 6, "VI-VII (Strong)")
    .when(col("mercalli_intensity") >= 4, "IV-V (Moderate)")
    .when(col("mercalli_intensity") >= 2, "II-III (Weak)")
    .otherwise("I (Not Felt)")
).withColumn(
    # Seismic Moment (log scale)
    "seismic_moment_log",
    rnd(lit(1.5) * col("magnitude") + lit(9.1), 2)
).withColumn(
    # Rupture Length in km
    "rupture_length_km",
    rnd(pow(lit(10), lit(0.74) * col("magnitude") - lit(3.55)), 2)
).withColumn(
    # Expected largest aftershock (Bath's Law)
    "expected_aftershock_mag",
    rnd(col("magnitude") - lit(1.2), 1)
).withColumn(
    # Tsunami Risk Score (custom formula)
    # Higher for: large magnitude + shallow depth + oceanic location
    "tsunami_risk_score",
    rnd(
        (col("magnitude") * lit(15)) - 
        (col("depth_km") * lit(0.2)) + 
        when(col("depth_km") < 70, lit(25)).otherwise(lit(0)) +
        when(col("magnitude") >= 7.0, lit(30)).otherwise(lit(0)),
        1
    )
).withColumn(
    # Damage Potential Index
    "damage_potential",
    when(col("mercalli_intensity") >= 8, "EXTREME")
    .when(col("mercalli_intensity") >= 6, "HIGH")
    .when(col("mercalli_intensity") >= 4, "MODERATE")
    .when(col("mercalli_intensity") >= 2, "LOW")
    .otherwise("MINIMAL")
).withColumn(
    "physics_calculated_ts", current_timestamp()
)

print("Physics formulas applied:")
print("  ✓ Gutenberg-Richter Energy")
print("  ✓ Modified Mercalli Intensity")
print("  ✓ Seismic Moment")
print("  ✓ Rupture Length (Wells-Coppersmith)")
print("  ✓ Expected Aftershock (Bath's Law)")
print("  ✓ Tsunami Risk Score")
print("  ✓ Damage Potential Index")

# COMMAND ----------

# ============================================================
# SAVE PHYSICS-ENRICHED DATA
# ============================================================

print("\n--- SAVING ENRICHED DATA ---")

# Select columns for physics table
physics_cols = [
    "event_id", "event_time", "latitude", "longitude",
    "magnitude", "depth_km", "place", "tectonic_region",
    "risk_level", "tsunami_potential",
    # Physics columns
    "energy_joules_log", "mercalli_intensity", "mercalli_scale",
    "seismic_moment_log", "rupture_length_km", 
    "expected_aftershock_mag", "tsunami_risk_score", "damage_potential",
    "physics_calculated_ts"
]

physics_final = physics_df.select(physics_cols)

physics_final.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_physics_analysis")

print(f"Saved: {physics_final.count():,} records to gold_physics_analysis")

# COMMAND ----------

# ============================================================
# PHYSICS SUMMARY STATISTICS
# ============================================================

print("\n--- PHYSICS SUMMARY ---")

# Summary by damage potential
print("\nDamage Potential Distribution:")
physics_final.groupBy("damage_potential").agg(
    count("*").alias("count"),
    rnd(avg("magnitude"), 2).alias("avg_mag"),
    rnd(avg("mercalli_intensity"), 1).alias("avg_mmi"),
    rnd(avg("rupture_length_km"), 2).alias("avg_rupture_km")
).orderBy(col("count").desc()).show()

# Summary by Mercalli Scale
print("\nMercalli Intensity Distribution:")
physics_final.groupBy("mercalli_scale").agg(
    count("*").alias("count"),
    rnd(avg("magnitude"), 2).alias("avg_mag")
).orderBy(col("count").desc()).show()

# COMMAND ----------

# ============================================================
# HIGH IMPACT EVENTS (Physics-based)
# ============================================================

print("\n--- TOP 20 HIGH IMPACT EVENTS ---")

spark.sql("""
    SELECT 
        event_time,
        magnitude,
        depth_km,
        place,
        mercalli_scale,
        rupture_length_km,
        tsunami_risk_score,
        damage_potential
    FROM gold_physics_analysis
    WHERE damage_potential IN ('EXTREME', 'HIGH')
    ORDER BY magnitude DESC, tsunami_risk_score DESC
    LIMIT 20
""").show(truncate=40)

# COMMAND ----------

# ============================================================
# REGIONAL PHYSICS SUMMARY
# ============================================================

print("\n--- REGIONAL PHYSICS SUMMARY ---")

regional_physics = physics_final.groupBy("tectonic_region").agg(
    count("*").alias("total_events"),
    rnd(avg("magnitude"), 2).alias("avg_magnitude"),
    rnd(avg("mercalli_intensity"), 1).alias("avg_mmi"),
    rnd(avg("rupture_length_km"), 2).alias("avg_rupture_km"),
    rnd(avg("tsunami_risk_score"), 1).alias("avg_tsunami_score"),
    spark_sum(when(col("damage_potential") == "EXTREME", 1).otherwise(0)).alias("extreme_count"),
    spark_sum(when(col("damage_potential") == "HIGH", 1).otherwise(0)).alias("high_count")
).withColumn("calculated_ts", current_timestamp())

regional_physics.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_regional_physics")

print("Saved to gold_regional_physics")
regional_physics.orderBy(col("extreme_count").desc()).show()

# COMMAND ----------

print("\n" + "="*60)
print("PHYSICS CALCULATIONS COMPLETE")
print("="*60)

print("""
TABLES CREATED:
  - gold_physics_analysis (event-level physics data)
  - gold_regional_physics (regional physics summary)

PHYSICS FORMULAS USED:
  1. Gutenberg-Richter: log10(E) = 1.5*M + 4.8
  2. Modified Mercalli: MMI = 1.5*M - 2.5*log10(D) + 2.0
  3. Seismic Moment: log10(M0) = 1.5*M + 9.1
  4. Wells-Coppersmith: log10(L) = 0.74*M - 3.55
  5. Bath's Law: Aftershock ≈ M - 1.2

>>> Run Notebook 09_dashboard next <<<
""")