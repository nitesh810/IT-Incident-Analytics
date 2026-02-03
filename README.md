# IT Incident Analytics Pipeline

## Project Overview
This project implements a **serverless GCP data pipeline** to process incident data. It ingests CSV files from **Google Cloud Storage (GCS)**, cleans and transforms them using **PySpark on Dataproc Serverless**, and publishes **analytics-ready tables in BigQuery**. The pipeline follows the **Medallion Architecture** (Bronze → Silver → Gold) and includes data validation for reliable reporting.

**Key Technologies:**  
- Python, PySpark  
- GCP: Cloud Run Gen2, Dataproc Serverless, BigQuery, Cloud Storage  
- Data Engineering Concepts: Medallion Architecture, Data Quality, ETL/ELT  

---

## ASCII Medallion Architecture

```
   +----------------------+
   |     Bronze Layer     |
   | Raw CSVs from GCS    |
   +----------+-----------+
              |
              v
   +----------------------+
   |     Silver Layer     |
   |  Cleaned & Deduped   |
   | Standardized Columns |
   +----------+-----------+
              |
              v
   +----------------------+
   |      Gold Layer      |
   | Analytics-ready KPI  |
   | BigQuery Tables      |
   +----------------------+
```
## Data Flow

```
GCS Bucket (incident CSVs)
        |
        v
Cloud Run Gen2 Function
        |
        v
Trigger Dataproc Serverless
PySpark Processing
        |
        v
Bronze Layer Table (BigQuery)
        |
        v
Silver Layer Table (BigQuery)
        |
        v
Gold Layer Table (BigQuery)
        |
        v
Downstream Analytics / Reporting
```

## Folder Structure

```
it-incident-analytics/
│
├── cloud_run_function/
│   ├── main.py                    # Cloud Run Function entry point (GCS trigger)
│   └── requirements.txt           # google-cloud-storage, google-cloud-dataproc
│
├── spark/
│   └── incident_job.py            # PySpark transformations on Dataproc Serverless
│
├── bigquery/
	│   ├── tables/
│   │   ├── bronze_incident.sql
│   │   ├── silver_incident.sql
│   │   └── gold_incident_kpi.sql
│   │
│   └── views/
│       └── incident_dashboard_view.sql
│
├── sample_data/
│   └── incident_sample.csv
│
├── README.md
└── .gitignore
```

---

## Key Features

- **Serverless Architecture:** Cloud Run Gen2 triggers Dataproc jobs automatically.  
- **Medallion Architecture:** Structured Bronze → Silver → Gold transformation layers.  
- **Data Quality:** Duplicate removal, null imputation, standardized columns.  
- **Analytics-Ready:** Gold tables compute incident KPIs like volume, SLA adherence, priority trends.  
- **BigQuery Integration:** Tables ready for downstream reporting and dashboarding.  

---

## Sample Data

Located in `sample_data/incident_sample.csv`, includes columns:  

- `incident_id`  
- `priority`  
- `category`  
- `sla_met`  

