# Databricks notebook source

# NOTEBOOK 07: TABLE OPTIMIZATION
# Purpose : Z-ORDER, OPTIMIZE (Compaction), Statistics




from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

print("="*60)
print("TABLE OPTIMIZATION")
print("="*60)
print(f"Started at: {datetime.now()}")

# BRONZE TABLE OPTIMIZATION

print("\n BRONZE TABLE")

# Check current file count before optimization
bronze_files_before = spark.sql("""
    DESCRIBE DETAIL bronze_earthquakes
""").select("numFiles").collect()[0][0]

print(f"Files before OPTIMIZE: {bronze_files_before}")

# OPTIMIZE with Z-ORDER on frequently queried columns
print("Running OPTIMIZE with Z-ORDER on (event_id, time)...")

spark.sql("""
    OPTIMIZE bronze_earthquakes
    ZORDER BY (event_id, time)
""")

bronze_files_after = spark.sql("""
    DESCRIBE DETAIL bronze_earthquakes
""").select("numFiles").collect()[0][0]

print(f"Files after OPTIMIZE: {bronze_files_after}")
print(f"Files compacted: {bronze_files_before - bronze_files_after}")


# SILVER TABLE OPTIMIZATION


print("\n SILVER TABLE ")

silver_files_before = spark.sql("""
    DESCRIBE DETAIL silver_earthquakes
""").select("numFiles").collect()[0][0]

print(f"Files before OPTIMIZE: {silver_files_before}")

# Z-ORDER on columns used in WHERE, JOIN, and aggregations
print("Running OPTIMIZE with Z-ORDER on (magnitude, event_time, tectonic_region)...")

spark.sql("""
    OPTIMIZE silver_earthquakes
    ZORDER BY (magnitude, event_time, tectonic_region)
""")

silver_files_after = spark.sql("""
    DESCRIBE DETAIL silver_earthquakes
""").select("numFiles").collect()[0][0]

print(f"Files after OPTIMIZE: {silver_files_after}")
print(f"Files compacted: {silver_files_before - silver_files_after}")


# GOLD TABLES OPTIMIZATION


print("\n GOLD TABLES ")

gold_tables = [
    ("gold_regional_risk", "risk_score, tectonic_region"),
    ("gold_temporal_metrics", "event_date"),
    ("gold_region_summary", "risk_rank, tectonic_region"),
]

for table_name, zorder_cols in gold_tables:
    try:
        files_before = spark.sql(f"DESCRIBE DETAIL {table_name}").select("numFiles").collect()[0][0]
        
        spark.sql(f"""
            OPTIMIZE {table_name}
            ZORDER BY ({zorder_cols})
        """)
        
        files_after = spark.sql(f"DESCRIBE DETAIL {table_name}").select("numFiles").collect()[0][0]
        
        print(f"{table_name}: {files_before} → {files_after} files")
    except Exception as e:
        print(f"{table_name}: Error - {str(e)[:50]}")

# gold_kpi_summary is single row, just optimize without ZORDER
spark.sql("OPTIMIZE gold_kpi_summary")
print("gold_kpi_summary: Optimized")


# COMPUTE STATISTICS FOR QUERY OPTIMIZER


print("\n--- COMPUTING TABLE STATISTICS ---")

tables_to_analyze = [
    "bronze_earthquakes",
    "silver_earthquakes",
    "gold_regional_risk",
    "gold_temporal_metrics",
    "gold_region_summary",
    "gold_kpi_summary",
    "ref_tectonic_regions"
]

for table in tables_to_analyze:
    try:
        spark.sql(f"ANALYZE TABLE {table} COMPUTE STATISTICS")
        print(f"  {table}: Statistics computed ✓")
    except Exception as e:
        print(f"  {table}: Error - {str(e)[:40]}")



# VACUUM OLD FILES (Optional - keeps last 7 days)


print("\n--- VACUUM (Remove old files) ---")

# Note: Default retention is 7 days (168 hours)
# VACUUM removes files older than retention period

vacuum_tables = ["bronze_earthquakes", "silver_earthquakes"]

for table in vacuum_tables:
    try:
        # Use default retention (7 days) - safe for CE
        spark.sql(f"VACUUM {table}")
        print(f"  {table}: Vacuumed ✓")
    except Exception as e:
        print(f"  {table}: Skipped - {str(e)[:40]}")


# TABLE SIZE REPORT

print("\n--- TABLE SIZE REPORT ---")

all_tables = [
    "bronze_earthquakes",
    "silver_earthquakes", 
    "gold_regional_risk",
    "gold_temporal_metrics",
    "gold_region_summary",
    "gold_kpi_summary"
]

print(f"{'Table':<25} {'Rows':>15} {'Files':>10} {'Size (MB)':>12}")
print("-" * 65)

for table in all_tables:
    try:
        detail = spark.sql(f"DESCRIBE DETAIL {table}").collect()[0]
        rows = spark.table(table).count()
        files = detail["numFiles"]
        size_mb = round(detail["sizeInBytes"] / (1024*1024), 2)
        print(f"{table:<25} {rows:>15,} {files:>10} {size_mb:>12}")
    except Exception as e:
        print(f"{table:<25} Error: {str(e)[:30]}")


print("OPTIMIZATION COMPLETE")

print(f"\nCompleted at: {datetime.now()}")