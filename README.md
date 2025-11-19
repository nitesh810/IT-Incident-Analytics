# 🚀 IT Incident Analytics – End-to-End Data Pipeline

This project demonstrates a complete automated data pipeline built using **Google Cloud Platform (GCP)**, **Dataproc Serverless (PySpark)**, **BigQuery**, and **Power BI**. The pipeline ingests incident CSV data, processes it, and visualizes insights in a real‑time dashboard.

---

## 📌 Architecture Overview

```text
             ┌────────────────────────────┐
             │        GCS BUCKET          │
             │   retail_2025xxxx.csv      │
             └──────────────┬─────────────┘
                            EventArc
                                 │
                                 ▼
                      ┌──────────────────┐
                      │    CLOUD RUN     │
                      │ gcs_to_bq.py     │
                      │  (Load to BQ)    │
                      └─────────┬────────┘
                                │
                                ▼
                      ┌──────────────────┐
                      │     BIGQUERY     │
                      │  Raw retail_*    │
                      └─────────┬────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │   DATAPROC SERVERLESS │
                    │ retail_transform.py   │
                    │ (PySpark Transform)   │
                    └──────────┬────────────┘
                               │
                               ▼
                    ┌───────────────────────┐
                    │     BIGQUERY FINAL    │
                    │ region_sales_all etc │
                    └──────────┬────────────┘
                               │
                               ▼
                    ┌───────────────────────┐
                    │      POWER BI         │
                    │  Incident Dashboard   │
                    └───────────────────────┘
```

---

## 📂 Project Summary

* CSV files uploaded to **GCS** automatically trigger the pipeline.
* **Cloud Run Function** loads each CSV into a new BigQuery table.
* **Dataproc PySpark** merges all `retail_*` raw tables and generates analytical tables.
* **Power BI** connects to BigQuery and visualizes incident KPIs.

---

## 🎯 Purpose

A fully automated, production-style data pipeline demonstrating ingestion, transformation, and dashboarding for IT incident analytics.
