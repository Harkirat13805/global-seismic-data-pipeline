
---

## 📂 Data Layers

### 🥉 Bronze Layer
- Raw earthquake events stored in Delta tables
- Append-only ingestion
- Includes ingestion metadata (`batch_id`, `ingestion_ts`)
- Watermark updated after each batch

### 🥈 Silver Layer
- Deduplication using `event_id`
- Schema normalization & null handling
- Risk classification (LOW / MODERATE / HIGH / CRITICAL)
- Tectonic region enrichment

### 🥇 Gold Layer
- KPI summary metrics
- Region-wise earthquake statistics
- Temporal trend aggregations
- Optimized tables for BI consumption

---

## 📊 Dashboard Metrics

- Total earthquakes processed
- Average & maximum magnitude
- High-risk & critical events
- Tsunami-prone earthquakes
- Region-wise seismic activity
- Daily & monthly trend analysis

---

## ⚙️ Technologies Used

- Python  
- PySpark  
- Delta Lake  
- Databricks (Serverless, Free Edition)  
- REST APIs  
- Databricks SQL Dashboard  

---

## 🚀 Pipeline Features

- Incremental ingestion using watermark-based CDC
- Fault-tolerant and resumable execution
- Serverless-compatible architecture
- Control tables for state management
- BI-ready analytics layer

---

## 📈 Scale

- **2.8M+ earthquake records processed**
- Incremental daily loads
- Multi-stage Databricks job pipeline

---

## 📁 Repository Structure

