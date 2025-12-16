# Databricks notebook source
# Databricks notebook source
# ============================================================
# NOTEBOOK 09: SEISMIC ANALYTICS DASHBOARD
# Purpose : Visual dashboard with KPIs and charts
# Platform: Databricks Community Edition
# Usage   : Use display() for charts, change chart type in UI
# ============================================================

# COMMAND ----------

# MAGIC %md
# MAGIC # 🌍 Global Seismic Analytics Dashboard
# MAGIC ---

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Key Performance Indicators

# COMMAND ----------

# KPI Summary
kpi = spark.sql("SELECT * FROM gold_kpi_summary").collect()[0]

# Display KPIs as HTML
displayHTML(f"""
<style>
    .dashboard {{ font-family: Arial, sans-serif; }}
    .kpi-row {{ display: flex; flex-wrap: wrap; gap: 15px; justify-content: center; margin: 20px 0; }}
    .kpi-box {{ 
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        border-radius: 12px; 
        padding: 20px 30px; 
        min-width: 160px;
        text-align: center;
        box-shadow: 0 4px 15px rgba(0,0,0,0.3);
        border: 1px solid #0f3460;
    }}
    .kpi-box.critical {{ background: linear-gradient(135deg, #d63031 0%, #e17055 100%); }}
    .kpi-box.warning {{ background: linear-gradient(135deg, #e67e22 0%, #f39c12 100%); }}
    .kpi-box.info {{ background: linear-gradient(135deg, #0984e3 0%, #74b9ff 100%); }}
    .kpi-box.success {{ background: linear-gradient(135deg, #00b894 0%, #55efc4 100%); }}
    .kpi-value {{ font-size: 28px; font-weight: bold; color: #fff; }}
    .kpi-label {{ font-size: 12px; color: #dfe6e9; margin-top: 8px; text-transform: uppercase; }}
    .data-range {{ text-align: center; color: #b2bec3; margin-top: 20px; font-size: 13px; }}
</style>

<div class="dashboard">
    <div class="kpi-row">
        <div class="kpi-box">
            <div class="kpi-value">{kpi['total_earthquakes']:,}</div>
            <div class="kpi-label">Total Earthquakes</div>
        </div>
        <div class="kpi-box critical">
            <div class="kpi-value">{kpi['critical_events']:,}</div>
            <div class="kpi-label">Critical (M7+)</div>
        </div>
        <div class="kpi-box warning">
            <div class="kpi-value">{kpi['high_risk_events']:,}</div>
            <div class="kpi-label">High Risk (M6+)</div>
        </div>
        <div class="kpi-box info">
            <div class="kpi-value">{kpi['tsunami_events']:,}</div>
            <div class="kpi-label">Tsunami Potential</div>
        </div>
    </div>
    <div class="kpi-row">
        <div class="kpi-box success">
            <div class="kpi-value">{kpi['max_magnitude']}</div>
            <div class="kpi-label">Max Magnitude</div>
        </div>
        <div class="kpi-box">
            <div class="kpi-value">{kpi['avg_magnitude']}</div>
            <div class="kpi-label">Avg Magnitude</div>
        </div>
        <div class="kpi-box">
            <div class="kpi-value">{kpi['active_regions']}</div>
            <div class="kpi-label">Active Regions</div>
        </div>
        <div class="kpi-box">
            <div class="kpi-value">{round(kpi['avg_depth_km'], 1)}</div>
            <div class="kpi-label">Avg Depth (km)</div>
        </div>
    </div>
    <div class="data-range">
        📅 Data Range: {str(kpi['data_start'])[:10]} to {str(kpi['data_end'])[:10]} | 
        🔄 Last Refresh: {str(kpi['refresh_ts'])[:19]}
    </div>
</div>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Events by Tectonic Region
# MAGIC *Click chart icon (top-right of table) → Select "Bar" chart*

# COMMAND ----------

# Bar Chart Data - Events by Region
display(spark.sql("""
    SELECT 
        tectonic_region as Region,
        total_events as Events,
        critical_events as Critical,
        high_risk_events as High_Risk,
        max_magnitude as Max_Mag
    FROM gold_region_summary
    ORDER BY total_events DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🗺️ Earthquake Map (M5.0+)
# MAGIC *Click chart icon → Select "Map" → Set Latitude/Longitude fields*

# COMMAND ----------

# Map Data - Significant earthquakes
display(spark.sql("""
    SELECT 
        latitude,
        longitude,
        magnitude,
        depth_km,
        place,
        risk_level,
        tectonic_region,
        event_time
    FROM silver_earthquakes
    WHERE magnitude >= 5.0
    ORDER BY event_time DESC
    LIMIT 3000
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📅 Daily Earthquake Trend
# MAGIC *Click chart icon → Select "Line" chart → X-axis: event_date, Y-axis: total_events*

# COMMAND ----------

# Time Series - Daily events
display(spark.sql("""
    SELECT 
        event_date,
        total_events,
        max_magnitude,
        critical_events,
        ROUND(rolling_7d_count / 7.0, 0) as rolling_7d_avg
    FROM gold_temporal_metrics
    ORDER BY event_date
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Magnitude Distribution
# MAGIC *Click chart icon → Select "Pie" or "Bar" chart*

# COMMAND ----------

# Magnitude Distribution
display(spark.sql("""
    SELECT 
        CASE 
            WHEN magnitude >= 8 THEN '8+ Great'
            WHEN magnitude >= 7 THEN '7-7.9 Major'
            WHEN magnitude >= 6 THEN '6-6.9 Strong'
            WHEN magnitude >= 5 THEN '5-5.9 Moderate'
            WHEN magnitude >= 4 THEN '4-4.9 Light'
            WHEN magnitude >= 3 THEN '3-3.9 Minor'
            ELSE '< 3 Micro'
        END as Magnitude_Category,
        COUNT(*) as Count
    FROM silver_earthquakes
    GROUP BY 1
    ORDER BY Count DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔬 Physics Analysis - Damage Potential
# MAGIC *Based on Modified Mercalli Intensity*

# COMMAND ----------

# Damage Potential from Physics
display(spark.sql("""
    SELECT 
        damage_potential,
        COUNT(*) as event_count,
        ROUND(AVG(magnitude), 2) as avg_magnitude,
        ROUND(AVG(mercalli_intensity), 1) as avg_mmi,
        ROUND(AVG(rupture_length_km), 2) as avg_rupture_km
    FROM gold_physics_analysis
    GROUP BY damage_potential
    ORDER BY 
        CASE damage_potential 
            WHEN 'EXTREME' THEN 1 
            WHEN 'HIGH' THEN 2 
            WHEN 'MODERATE' THEN 3 
            WHEN 'LOW' THEN 4 
            ELSE 5 
        END
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🌊 Tsunami Risk by Region
# MAGIC *Based on physics-calculated tsunami risk score*

# COMMAND ----------

# Regional Tsunami Risk
display(spark.sql("""
    SELECT 
        tectonic_region,
        total_events,
        avg_magnitude,
        avg_mmi as Avg_Mercalli,
        avg_tsunami_score as Tsunami_Score,
        extreme_count + high_count as High_Impact_Events
    FROM gold_regional_physics
    ORDER BY avg_tsunami_score DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔴 Recent Major Earthquakes (M6.0+)

# COMMAND ----------

# Recent significant events
display(spark.sql("""
    SELECT 
        event_time,
        magnitude,
        depth_km,
        place,
        tectonic_region,
        risk_level,
        tsunami_potential
    FROM silver_earthquakes
    WHERE magnitude >= 6.0
    ORDER BY event_time DESC
    LIMIT 50
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Monthly Trends
# MAGIC *Line chart showing monthly aggregates*

# COMMAND ----------

# Monthly summary
display(spark.sql("""
    SELECT 
        year,
        month,
        SUM(total_events) as Events,
        ROUND(AVG(avg_magnitude), 2) as Avg_Mag,
        MAX(max_magnitude) as Max_Mag,
        SUM(critical_events) as Critical,
        SUM(tsunami_events) as Tsunami
    FROM gold_temporal_metrics
    GROUP BY year, month
    ORDER BY year, month
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Pipeline Status

# COMMAND ----------

# Watermark Status
print("PIPELINE WATERMARK STATUS")
print("-" * 60)
spark.sql("""
    SELECT 
        table_name,
        watermark_value,
        last_updated,
        records_processed
    FROM control_watermark
""").show(truncate=False)

# Table Counts
print("\nTABLE ROW COUNTS")
print("-" * 60)

tables = [
    "bronze_earthquakes", "silver_earthquakes",
    "gold_regional_risk", "gold_temporal_metrics", 
    "gold_region_summary", "gold_kpi_summary",
    "gold_physics_analysis", "gold_regional_physics"
]

for t in tables:
    try:
        cnt = spark.table(t).count()
        print(f"  {t}: {cnt:,}")
    except:
        print(f"  {t}: NOT FOUND")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ✅ Dashboard Complete
# MAGIC 
# MAGIC **To create visualizations:**
# MAGIC 1. Click the chart icon (📊) at top-right of each table output
# MAGIC 2. Select chart type (Bar, Line, Pie, Map, etc.)
# MAGIC 3. Configure axes and settings
# MAGIC 4. Click "Save" to keep the visualization
# MAGIC 
# MAGIC **To refresh data:**
# MAGIC - Run the full pipeline (Notebooks 02-09) or
# MAGIC - Just re-run this dashboard notebook
# MAGIC 
# MAGIC **Tables used:**
# MAGIC - `gold_kpi_summary` - KPIs
# MAGIC - `gold_region_summary` - Regional data
# MAGIC - `gold_temporal_metrics` - Time series
# MAGIC - `gold_physics_analysis` - Physics calculations
# MAGIC - `silver_earthquakes` - Event details