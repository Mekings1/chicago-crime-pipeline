# 🔵 Chicago Crime Analytics Pipeline

[![Project - Data Engineering Zoomcamp](https://img.shields.io/badge/Project-Data%20Engineering%20Zoomcamp-blue)](https://github.com/DataTalksClub/data-engineering-zoomcamp)
[![Python - 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![dbt - 1.7+](https://img.shields.io/badge/dbt-1.7+-orange.svg)](https://www.getdbt.com/)

An end-to-end data engineering project built as a capstone for the **Data Engineering Zoomcamp**. This pipeline automates the ingestion of Chicago's open crime data, processes it in a cloud-native environment, and surfaces analytical insights via an interactive dashboard.

---

## 📖 Problem Statement
The City of Chicago publishes every recorded crime incident since 2001 via its Open Data Portal. However, the sheer volume of data makes it difficult to derive quick insights without a structured data warehouse.

This project builds a fully automated batch pipeline to answer:
- **Which crime types are most prevalent?**
- **How has overall crime volume changed month-over-month?**
- **Which crime types have the highest and lowest arrest rates?**

---

## 🏗️ Architecture

The pipeline leverages a "Modern Data Stack" approach with a focus on efficiency and portability:

1.  **Ingestion:** Python & Prefect extract data from the **Socrata API** in Parquet format.
2.  **Data Lake:** Raw files are stored in **AWS S3**, partitioned by `year` and `month`.
3.  **Data Warehouse:** **DuckDB** acts as the analytical engine, providing high-speed processing on the local machine.
4.  **Transformation:** **dbt-duckdb** handles the T in ELT, creating a staging layer and high-performance analytical marts.
5.  **Visualization:** A **Streamlit** dashboard provides a real-time interface for data exploration.

---

## 🛠️ Technologies

| Layer | Tool |
| :--- | :--- |
| **Cloud Storage** | AWS S3 |
| **Infrastructure as Code** | Terraform |
| **Orchestration** | Prefect (local) |
| **Data Lake** | AWS S3 (Parquet, partitioned by year/month) |
| **Data Warehouse** | DuckDB |
| **Transformations** | dbt-duckdb |
| **Dashboard** | Streamlit + Plotly |
| **Language** | Python 3.10+ |

---

## 📂 Project Structure

```bash
chicago-crime-pipeline/
├── terraform/                  # S3 bucket provisioning
│   └── main.tf
├── pipeline/
│   ├── ingest.py               # Prefect ingestion flow (Raw -> S3)
│   └── warehouse.py            # DuckDB table initialization
├── dbt_project/
│   └── chicago_crime/
│       ├── models/
│       │   ├── staging/        # stg_chicago_crime.sql (Cleaning/Casting)
│       │   ├── marts/          # Analytical models (Aggregations)
│       │   └── schema.yml      # dbt tests and documentation
├── dashboard/
│   └── app.py                  # Streamlit dashboard application
├── .env.example                # Environment variable template
├── requirements.txt            # Project dependencies
└── README.md

```
---

## How to Reproduce

### 1. Prerequisites
- Python 3.10+
- AWS CLI configured (`aws configure`)
- Terraform installed
- Git

### 2. Clone & install

```bash
git clone https://github.com/mekingz1/chicago-crime-pipeline.git
cd chicago-crime-pipeline
python -m venv venv
# Windows:
.\venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate

pip install -r requirements.txt
```

### 3. Configure environment

```bash
cp .env.example .env
# Edit .env with your AWS credentials and bucket name
```

### 4. Provision S3 bucket with Terraform

```bash
cd terraform
terraform init
terraform apply
cd ..
```

### 5. Ingest data

```bash
# Ingest a date range (incremental — safe to re-run)
python pipeline/ingest.py --start 2024-01 --end 2024-06
```

### 6. Build the warehouse

```bash
python pipeline/warehouse.py
```

### 7. Run dbt transformations

```bash
cd dbt_project/chicago_crime
dbt run 
dbt test 
cd ../..
```

### 8. Launch the dashboard

```bash
streamlit run dashboard/app.py
```

Open `http://localhost:8501` in your browser.


### 9. [BONUS] Run the full pipeline

```bash
python pipeline/run_pipeline.py --start 2022-01 --end 2024-12
```

This runs ingestion → warehouse → dbt → dashboard in sequence. 
Open `http://localhost:8501` when it completes.

---

## Data Warehouse Design

The raw table is physically sorted by `year → month → primary_type`.
This mimics Hive-style partitioning within DuckDB — range queries on date
and filters on crime type scan far fewer rows without a traditional index.

The dbt mart layer is materialised as tables so the dashboard queries are
instant with no re-aggregation at read time.

---

## Incremental Ingestion

The pipeline writes a `manifest.json` to S3 after each successful month.
On re-runs, completed months are skipped automatically. To backfill:

```bash
python pipeline/ingest.py --start 2020-01 --end 2023-12
```