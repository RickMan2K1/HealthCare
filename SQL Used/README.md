```markdown
# 🏥 Healthcare Data Pipeline & Analytics

An end-to-end, enterprise-grade cloud data engineering pipeline built on **AWS Glue**, **PySpark**, **Amazon S3**, **AWS Athena**, and **Streamlit**. 

This project ingests raw daily staffing logs and provider metadata from the **Centers for Medicare & Medicaid Services (CMS)**, transforms them through a 3-tier **Medallion Architecture**, and delivers interactive operational insights on nursing workload (HPRD), contractor reliance, and facility performance.

---

## 🏗️ System Architecture

```text
                        ┌────────────────────────────────────────────────────────┐
                        │                 AWS S3 DATA LAKE                       │
                        │                                                        │
                        │  s3://healthcare-rick/                                 │
                        │    ├── bronze/       (Raw Parquet conversion)          │
                        │    ├── silver/       (Cleaned & partitioned Parquet)   │
                        │    ├── gold/         (Aggregated Star Schema Parquet)  │
                        │    └── quarantine/   (Corrupted/invalid daily logs)   │
                        └───────────────────┬────────────────────────────────────┘
                                            │
                                            ▼
                        ┌────────────────────────────────────────────────────────┐
                        │             AWS GLUE DATA CATALOG                      │
                        │                                                        │
                        │    ├── Database: healthcare_silver                     │
                        │    └── Database: healthcare_gold                       │
                        └───────────────────┬────────────────────────────────────┘
                                            │
                                            ▼
                        ┌────────────────────────────────────────────────────────┐
                        │            ANALYTICS & CONSUMPTION                     │
                        │                                                        │
                        │    ├── AWS Athena    (Ad-hoc SQL analytics engine)     │
                        │    └── Streamlit     (Interactive app via PyAthena)    │
                        └────────────────────────────────────────────────────────┘

```

---

## 🥇 Medallion Layer Architecture

### 🥉 Bronze Layer (Ingestion)

* **Objective:** Converts raw CMS CSV exports (`PBJ_Daily_Nurse_Staffing` & `NH_ProviderInfo`) into immutable S3 Parquet format without schema alteration.
* **Storage Location:** `s3://healthcare-rick/bronze/`

### 🥈 Silver Layer (Cleaning & Standardizing)

* **Objective:** Cleans text artifacts, strips quotes, normalizes CCNs to 6-digit zero-padded strings, standardizes dates, and calculates daily staffing hours.
* **Storage Location:** `s3://healthcare-rick/silver/` (Partitioned by `State`)
* **Glue Catalog Database:** `healthcare_silver`
* **Data Quality & Quarantine:** Diverts invalid records (e.g., negative resident census or malformed dates) to `s3://healthcare-rick/quarantine/`.

### 🥇 Gold Layer (Analytics & Star Schema)

* **Objective:** Aggregates daily Silver records up to facility-month summaries (`Year_Month`), calculating non-distributive metrics such as weighted **Monthly HPRD** and **Contractor Reliance Share (%)**.
* **Storage Location:** `s3://healthcare-rick/gold/` (Partitioned by `State`)
* **Glue Catalog Database:** `healthcare_gold`
* **Tables:** `fact_gold_monthly_staffing`, `dim_gold_provider`

---

## 🔄 Automated Pipeline Workflow (AWS Glue)

The pipeline is orchestrated automatically using an AWS Glue Workflow: **`wf_healthcare_medallion_pipeline`**.

```text
[ Trigger: trig_start_pipeline ] (On-Demand / Schedule)
               │
               ▼
   [ Glue Job: healthcare_bronze_to_silver ]
               │
               ├── (Data Quality Failures) ──► [ Route to s3://.../quarantine/ ]
               │
        (Event: SUCCEEDED)
               │
               ▼
   [ Trigger: trig_silver_success ] (Event Watcher)
               │
               ▼
   [ Glue Job: healthcare_silver_to_gold ]
               │
        (Event: SUCCEEDED)
               │
               ▼
   [ Catalogs Updated & Streamlit / Athena Ready ]

```

---

## 🛠️ Tech Stack & Prerequisites

* **Cloud Infrastructure:** AWS S3, AWS Glue (PySpark 3.4 / Glue 4.0 runtime), AWS Athena, AWS Glue Data Catalog.
* **Languages & Libraries:** Python 3.10+, PySpark, SQL, Streamlit, PyAthena, Plotly, Pandas.
* **Tools:** AWS CLI, Git.

---

## 🚀 Quick Start Guide

### 1. Repository Setup

```bash
git clone [https://github.com/RickMan2K1/HealthCare.git](https://github.com/RickMan2K1/HealthCare.git)
cd HealthCare

```

### 2. Environment Configuration

Create a virtual environment and install dependencies:

```bash
python -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate
pip install -r requirements.txt

```

### 3. AWS Credentials Setup

Ensure your local terminal has AWS CLI access configured to connect to Athena:

```bash
aws configure

```

### 4. Running the Pipeline Workflow in AWS

To launch the automated ETL workflow directly in AWS:

```bash
aws glue start-workflow-run --name wf_healthcare_medallion_pipeline

```

### 5. Launching the Local Streamlit App

Run the interactive dashboard locally:

```bash
streamlit run app.py

```

---

## 📁 Repository Structure

```text
├── app.py                            # Streamlit Dashboard application
├── jobs/
│   ├── healthcare_bronze_to_silver.py # PySpark script for Silver layer & Quarantine
│   └── healthcare_silver_to_gold.py   # PySpark script for Gold aggregation
├── requirements.txt                  # Python dependencies
└── README.md                         # Project documentation

```

---

## 📊 Core Data Metrics & Formulas

| Metric | Level | Mathematical Formula |
| --- | --- | --- |
| **Total Nurse Hours** | Daily / Monthly | $\text{Hrs\_RN} + \text{Hrs\_LPN} + \text{Hrs\_CNA}$ |
| **Nurse Hours Per Resident Day (HPRD)** | Daily | $\frac{\text{Total\_Nurse\_Hours}}{\text{MDScensus}}$ |
| **Weighted Monthly HPRD** | Monthly | $\frac{\sum_{\text{Month}} \text{Total\_Nurse\_Hours}}{\sum_{\text{Month}} \text{MDScensus}}$ |
| **Contractor Reliance Share (%)** | Monthly | $\left( \frac{\sum_{\text{Month}} \text{Total\_Contract\_Hours}}{\sum_{\text{Month}} \text{Total\_Nurse\_Hours}} \right) \times 100$ |

---

```

```