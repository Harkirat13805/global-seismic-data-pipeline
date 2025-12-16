
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
<img width="1727" height="1003" alt="image" src="https://github.com/user-attachments/assets/5302c34b-7443-416e-937d-76f3a0d68d6a" />

---

## 📈 Scale

- **2.8M+ earthquake records processed**
- Incremental daily loads
- Multi-stage Databricks job pipeline

---

## 📁 PIPELINE



<img width="1714" height="1017" alt="image" src="https://github.com/user-attachments/assets/1e2679bb-eb3d-473c-adf8-40fda6bdd419" />


