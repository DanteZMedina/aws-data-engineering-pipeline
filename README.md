# 🚕 NYC Taxi Data Engineering Pipeline (AWS | PySpark | Lambda | Streamlit)

## 📌 Overview

This project implements a complete **end-to-end data engineering pipeline on AWS**, designed to ingest, process, and visualize NYC Taxi data using modern data architecture principles.

The solution leverages **event-driven ingestion**, **distributed processing with PySpark**, and a **data lake architecture in S3**, exposing insights through an interactive Streamlit dashboard.

---

## 🏗️ Architecture

```text
                ┌────────────────────┐
                │   S3 (uploads/)    │
                │  Incoming files    │
                └─────────┬──────────┘
                          │ (trigger)
                          ▼
                ┌────────────────────┐
                │      Lambda        │
                │  Move to raw/      │
                └─────────┬──────────┘
                          ▼
                ┌────────────────────┐
                │    S3 (raw/)       │
                │  Raw data storage  │
                └─────────┬──────────┘
                          ▼
                ┌────────────────────┐
                │   PySpark (EC2)    │
                │   ETL Processing   │
                └─────────┬──────────┘
                          ▼
                ┌────────────────────┐
                │  S3 (processed/)   │
                │  Cleaned data      │
                └─────────┬──────────┘
                          ▼
                ┌────────────────────┐
                │   Streamlit App    │
                │   Visualization    │
                └────────────────────┘
```

---

## ⚙️ Tech Stack

* **AWS S3** → Data lake storage
* **AWS Lambda** → Event-driven ingestion
* **AWS EC2** → Data processing environment
* **PySpark** → Distributed data processing
* **Python** → Core language
* **Streamlit** → Data visualization
* **Parquet** → Optimized storage format

---

## 🗂️ S3 Data Lake Structure

```text
s3://xideralaws-curso-dante/
│
├── uploads/     # Incoming files (trigger Lambda)
├── raw/         # Raw data after ingestion
├── processed/   # Cleaned & transformed data
├── result/      # Optional outputs
```

---

## 🔄 Pipeline Workflow

### 1. Data Ingestion (Event-Driven)

* Files are uploaded into:

  ```
  s3://bucket/uploads/
  ```
* This triggers an **AWS Lambda function** that:

  * Moves files to `raw/`
  * Prevents infinite loops using prefix validation

---

### 2. Raw Data Storage

* Data is stored in:

  ```
  s3://bucket/raw/
  ```
* Maintains original structure for traceability

---

### 3. Data Processing (PySpark)

Run ETL job:

```bash
spark-submit etl/etl_spark.py
```

This step:

* Reads all parquet files from S3
* Applies dynamic schema normalization
* Cleans and transforms data
* Writes results to:

```
s3://bucket/processed/
```

---

### 4. Data Visualization

Run Streamlit:

```bash
streamlit run app/app.py
```

* Loads processed data
* Displays interactive insights

---

## 📂 Project Structure

```text
aws-data-engineering-pipeline/
│
├── app/
│   └── app.py                  # Streamlit dashboard
│
├── etl/
│   ├── etl_spark.py            # Main ETL logic
│   ├── analysis.py             # Data analysis
│   └── run_etl.sh              # Execution script
│
├── ingestion/
│   └── download_data.py        # Data ingestion
│
├── lambda/
│   └── move_to_raw.py          # S3 trigger handler
│
├── data-sample/
│   └── taxi_zone_lookup.csv
│
├── requirements.txt
└── README.md
```

---

## 🔐 Security Best Practices

* No credentials are hardcoded
* AWS access handled via:

  * Environment variables
  * IAM roles (recommended)
* Secrets removed from version control

---

## 💡 Key Engineering Decisions

* **Event-driven ingestion** using S3 + Lambda
* **Separation of data layers** (uploads → raw → processed)
* **Scalable processing** with PySpark
* **Schema normalization** to handle inconsistent datasets
* **Loop prevention logic** in Lambda triggers

---

## 🚀 Future Improvements

* Orchestration with **Apache Airflow**
* CI/CD pipeline with **GitHub Actions**
* Data quality checks (Great Expectations)
* Partitioned data lake for performance optimization
* Deployment with Docker

---

## 🧠 What I Learned

* Designing data pipelines on AWS
* Handling event-driven architectures
* Working with distributed data processing (Spark)
* Managing data lakes and storage layers
* Applying security best practices in cloud environments

---

## 👨‍💻 Author

**Dante Medina**
