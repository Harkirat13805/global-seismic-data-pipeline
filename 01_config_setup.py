# Databricks notebook source

# NOTEBOOK 1: CONFIG AND SETUP
# Run this first - creates database, control table, and reference data
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, DateType, BooleanType
from pyspark.sql.functions import lit, current_timestamp
from datetime import datetime

spark = SparkSession.builder.getOrCreate()


# COMMAND ----------

# CONFIGURATION

API_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
API_COUNT_URL = "https://earthquake.usgs.gov/fdsnws/event/1/count"

CONFIG = {
    "api_url": API_URL,
    "api_count_url": API_COUNT_URL,
    "min_magnitude": 2.5,
    "initial_start_date": "2024-01-01",  
    "watermark_column": "time",
    "batch_days": 30
}

print("API URL:", CONFIG["api_url"])
print("Min Magnitude:", CONFIG["min_magnitude"])
print("Watermark Column:", CONFIG["watermark_column"])
print("Initial Start Date:", CONFIG["initial_start_date"])

# COMMAND ----------

# CREATE CONTROL TABLE FOR WATERMARK

spark.sql("DROP TABLE IF EXISTS control_watermark")

spark.sql("""
CREATE TABLE control_watermark (
    table_name STRING,
    watermark_column STRING,
    watermark_value TIMESTAMP,
    last_updated TIMESTAMP,
    records_processed LONG
)
USING DELTA
""")

print("Control table created: control_watermark")

# COMMAND ----------

# INSERT INITIAL WATERMARK

spark.sql(f"""
    INSERT INTO control_watermark VALUES (
        'bronze_earthquakes',
        'time',
        TIMESTAMP '{CONFIG["initial_start_date"]} 00:00:00',
        current_timestamp(),
        0
    )
""")

print(f"Initial watermark set to: {CONFIG['initial_start_date']}")


# COMMAND ----------

# VERIFY WATERMARK

print("Current watermark:")
spark.sql("SELECT * FROM control_watermark").show(truncate=False)

# COMMAND ----------

# CREATE REFERENCE TABLE - TECTONIC REGIONS

spark.sql("DROP TABLE IF EXISTS ref_tectonic_regions")

tectonic_data = [
    ("CALIFORNIA", "California", -125.0, -114.0, 32.0, 42.0, 1, "transform"),
    ("ALASKA", "Alaska", -180.0, -130.0, 50.0, 72.0, 2, "subduction"),
    ("JAPAN", "Japan", 128.0, 148.0, 30.0, 46.0, 3, "subduction"),
    ("INDONESIA", "Indonesia", 95.0, 140.0, -11.0, 6.0, 4, "subduction"),
    ("CHILE", "Chile", -76.0, -66.0, -56.0, -17.0, 5, "subduction"),
    ("PHILIPPINES", "Philippines", 116.0, 128.0, 5.0, 20.0, 6, "subduction"),
    ("MEXICO", "Mexico", -118.0, -86.0, 14.0, 33.0, 7, "subduction"),
    ("MEDITERRANEAN", "Mediterranean", -10.0, 40.0, 30.0, 46.0, 8, "collision"),
    ("HIMALAYA", "Himalaya", 70.0, 100.0, 25.0, 40.0, 9, "collision"),
    ("CARIBBEAN", "Caribbean", -90.0, -60.0, 10.0, 25.0, 10, "complex"),
    ("NEW_ZEALAND", "New Zealand", 165.0, 180.0, -50.0, -34.0, 11, "transform"),
    ("OTHER", "Other Regions", -180.0, 180.0, -90.0, 90.0, 99, "unknown")
]

tectonic_schema = StructType([
    StructField("region_code", StringType(), False),
    StructField("region_name", StringType(), False),
    StructField("min_lon", DoubleType(), False),
    StructField("max_lon", DoubleType(), False),
    StructField("min_lat", DoubleType(), False),
    StructField("max_lat", DoubleType(), False),
    StructField("priority", IntegerType(), False),
    StructField("plate_type", StringType(), True)
])

tectonic_df = spark.createDataFrame(tectonic_data, tectonic_schema)
tectonic_df.write.format("delta").mode("overwrite").saveAsTable("ref_tectonic_regions")

print(f"Reference table created: ref_tectonic_regions ({tectonic_df.count()} rows)")

# COMMAND ----------

# CREATE RISK CLASSIFICATION REFERENCE

spark.sql("DROP TABLE IF EXISTS ref_risk_classification")

risk_data = [
    ("CRITICAL", 1, 7.0, None, "#FF0000"),
    ("HIGH", 2, 6.0, 7.0, "#FF6600"),
    ("MODERATE", 3, 5.0, 6.0, "#FFCC00"),
    ("LOW", 4, 4.0, 5.0, "#99CC00"),
    ("MINIMAL", 5, 2.0, 4.0, "#00CC00")
]

risk_schema = StructType([
    StructField("risk_level", StringType(), False),
    StructField("risk_rank", IntegerType(), False),
    StructField("min_magnitude", DoubleType(), False),
    StructField("max_magnitude", DoubleType(), True),
    StructField("color_code", StringType(), True)
])

risk_df = spark.createDataFrame(risk_data, risk_schema)
risk_df.write.format("delta").mode("overwrite").saveAsTable("ref_risk_classification")

print(f"Reference table created: ref_risk_classification ({risk_df.count()} rows)")

# COMMAND ----------

# VERIFY SETUP


print("SETUP COMPLETE")


print("\nTables created:")
tables = spark.sql("SHOW TABLES").collect()
for t in tables:
    print(f"  - {t['tableName']}")

print("\nWatermark status:")
spark.sql("SELECT * FROM control_watermark").show(truncate=False)

print("\nReference data:")
spark.sql("SELECT region_code, region_name FROM ref_tectonic_regions").show(truncate=False)