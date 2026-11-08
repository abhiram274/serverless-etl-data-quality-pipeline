# Serverless ETL & Data Quality Pipeline on AWS

[![AWS Architecture](https://img.shields.io/badge/AWS-S3%20%7C%20Lambda%20%7C%20RDS%20Postgres%20%7C%20Athena-232F3E?logo=amazon-aws)](https://aws.amazon.com/)
[![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python)](https://python.org)
[![Testing](https://img.shields.io/badge/Tests-24%2F24%20Passed-brightgreen)](https://github.com/)
[![Streamlit](https://img.shields.io/badge/Dashboard-Streamlit-FF4B4B?logo=streamlit)](https://streamlit.io/)

A complete, production-grade, event-driven **Serverless ETL and Data Quality Lakehouse Pipeline** built on AWS. 

This repository implements the end-to-end data lifecycle: from high-throughput batch CSV ingestion, automated S3 event notifications, zero-trust anti-recursion protections, and modular data quality isolation, to star-schema PostgreSQL warehouse loading, serverless Amazon Athena query federation, CloudWatch/SNS alerting, and an interactive Streamlit operations dashboard.

---

## 1. High-Level Architecture

```text
                                  +-------------------------------------------------------------+
                                  |                     AWS Serverless Pipeline                 |
                                  +-------------------------------------------------------------+

[ Ingestion Source ] ────────────> [ S3 Bucket: raw/ ]
                                           |
                                  (ObjectCreated:Put)
                                           v
                             +---------------------------+
                             |   AWS Lambda ETL Worker   |
                             | - Anti-Recursion Filter   |
                             | - Extraction & Profiling  |
                             | - CloudWatch JSON Logging |
                             +-------------+-------------+
                                           |
                                           v
                             +---------------------------+
                             |   Modular Quality Engine  |
                             |  (Rule 1: Deduplication)  |
                             |  (Rule 2: Missing Check)  |
                             |  (Rule 3: Amount Check)   |
                             |  (Rule 4: Date Parse Check|
                             +------+-------------+------+
                                    |             |
                   Valid Records    |             | Quarantined Records
                                    v             v
       +----------------------------+--+       +--+--------------------------+
       |     S3 Bucket: curated/       |       |    S3 Bucket: quarantine/   |
       |  (Standardized & Validated)   |       | (Raw + Rejection Reasons)   |
       +--------------+----------------+       +--------------+--------------+
                      |                                       |
                      |                                       v
                      |                         +----------------------------+
                      |                         |    S3 Bucket: reports/     |
                      |                         |   (JSON Execution Audits)  |
                      |                         +--------------+-------------+
                      |                                        |
         +------------+------------+                           v
         |                         |             +---------------------------+
         v                         v             | CloudWatch Metrics & SNS  |
+------------------+     +------------------+    | - Execution Duration      |
|  S3: warehouse/  |     | Amazon RDS       |    | - Error Threshold Alarms  |
| (Hive Parquet /  |     | PostgreSQL       |    | - High Rejection Alerting |
|  year=YY/mon=MM) |     | (Star Schema DB) |    +---------------------------+
+--------+---------+     +---------+--------+
         |                         |
         v                         v
+------------------+     +---------------------------------------------------+
| AWS Glue Catalog |     |        Interactive Streamlit Dashboard            |
| & Amazon Athena  |────>|  (Executive KPIs, Revenue Curves, Top Products,   |
| (Serverless SQL) |     |   Regional Performance, Quality Health Monitors)  |
+------------------+     +---------------------------------------------------+
```

---

## 2. Star Schema Data Warehouse Design

Implemented in PostgreSQL ([`sql/schema.sql`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/sql/schema.sql)):

```text
                  +----------------------+
                  |     dim_customer     |
                  +----------------------+
                  | PK  customer_key     |
                  | UK  customer_id      |
                  |     region           |
                  +----------+-----------+
                             | 1
                             |
                             | N
+----------------------+  +--+-------------------+  +----------------------+
|     dim_product      |  |     fact_orders      |  |       dim_date       |
+----------------------+  +----------------------+  +----------------------+
| PK  product_key      |<-| PK  order_key        |->| PK  date_key         |
| UK  product_id       |1N| UK  order_id (UK)    |N1| UK  full_date        |
|     product_name     |  | FK  customer_key     |  |     year / quarter   |
|     category         |  | FK  product_key      |  |     month / day      |
|     unit_price       |  | FK  date_key         |  |     is_weekend       |
+----------------------+  |     order_timestamp  |  +----------------------+
                          |     quantity         |
                          |     unit_price       |
                          |     total_amount     |
                          |     payment_method   |
                          |     order_status     |
                          |     batch_id         |
                          +----------------------+
```

---

## 3. Modular Data Quality Rules & Isolation

Located in [`src/quality/`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/src/quality/):
1. **Rule 1 – Deduplication** ([`check_duplicates.py`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/src/quality/check_duplicates.py)): Flags primary key `order_id` duplicates and identical transaction rows.
2. **Rule 2 – Missing Fields** ([`check_missing.py`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/src/quality/check_missing.py)): Flags missing/null/empty mandatory attributes (`customer_id`, `payment_method`, `quantity`).
3. **Rule 3 – Numeric Integrity** ([`check_amounts.py`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/src/quality/check_amounts.py)): Enforces strictly positive constraints ($\text{unit\_price} > 0$, $\text{quantity} > 0$, $\text{total\_amount} > 0$).
4. **Rule 4 – Temporal Validity** ([`check_dates.py`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/src/quality/check_dates.py)): Enforces parseable ISO calendar timestamps.

### Record Reconciliation & Isolation Guarantee
Rejected records are **never discarded** and **never admitted into the PostgreSQL warehouse**:
$$\text{Total Ingested Records (10,200)} = \text{Curated Records (9,450)} + \text{Quarantined Records (750)}$$
- Quarantined records preserve all original attributes with an explicit appended `rejection_reasons` column.
- Idempotency is guaranteed via `ON CONFLICT (order_id) DO UPDATE` in SQL loader.

---

## 4. End-to-End Verified Results & Project Metrics

| Phase / Component | Tested Metric | Result | Audit Status |
| :--- | :--- | :--- | :--- |
| **Phase 1: Generator** | Total Records Generated | **10,200** | Clean baseline + controlled anomalies |
| **Phase 2: S3 & Lambda** | Anti-Recursion Filter | **100% Blocked** | Downstream triggers prevented |
| **Phase 3: DQ Engine** | Curated Clean Pass | **9,450 records** | Validated & routed to `curated/` |
| **Phase 3: DQ Engine** | Quarantined Errors | **750 records** | Tagged & routed to `quarantine/` |
| **Phase 3: Warehouse** | PostgreSQL Facts Loaded | **9,450 facts** | 0 invalid rows admitted |
| **Phase 3: Idempotency** | Duplicate Re-run Load | **9,450 facts** | Zero duplicate facts created |
| **Phase 4: Analytics** | Gross Revenue Tracked | **\$2,470,011.05** | Verified across Athena & DB |
| **Phase 5: Alerting** | SNS Threshold Alert | **Triggered** | 7.35% rejection rate > 5.0% threshold |
| **Overall Test Suite** | Automated Tests | **24 Passed / 0 Failed** | 100% Pass Rate |

---

## 5. Repository File Structure

```text
serverless-etl-data-quality-pipeline/
├── .env.example                     # Environment template (Local, S3 & RDS)
├── .gitignore                       # Git exclusions (.venv, caches, local DBs)
├── app.py                           # Interactive Streamlit Analytics & Quality Dashboard
├── HOW_TO_RUN.md                    # Detailed runbook & AWS configuration guide
├── README.md                        # Production project documentation
├── requirements.txt                 # Pinned dependencies
├── cloudwatch/
│   └── alarms_and_sns.json          # CloudFormation / CloudWatch Alarms & SNS Template
├── data/
│   ├── clean/                       # Reference clean dataset (10,200 rows)
│   │   └── retail_transactions_clean.csv
│   └── raw/                         # Raw dataset with injected anomalies (10,200 rows)
│       └── retail_transactions.csv
├── iam/
│   ├── lambda_assume_role_policy.json # IAM Trust Policy for Lambda
│   ├── lambda_execution_policy.json   # Least-privilege Lambda Policy (S3 + CW)
│   └── s3_bucket_policy.json          # S3 TLS & Encryption Enforcement
├── reports/
│   ├── quality_audit_phase3_batch_001.json # Quality Execution Audit Report
│   └── statistics_report.json              # Data Exploration Report
├── sql/
│   ├── athena_analytical_queries.sql# 5 Analytical queries (Revenue, Products, Cohorts)
│   ├── athena_ddl.sql               # Athena / Glue Catalog Partitioned DDL
│   └── schema.sql                   # Star Schema DDL (PostgreSQL: tables, PKs, FKs, indexes)
├── src/
│   ├── __init__.py
│   ├── analytics_service.py         # Dashboard analytics & KPI aggregation engine
│   ├── athena_exporter.py           # S3 warehouse Hive partitioner (year=YYYY/month=MM)
│   ├── e2e_validator.py             # Phase 5 End-to-End Orchestrator & Validator
│   ├── explorer.py                  # Profiling & Exploration module
│   ├── generator.py                 # Synthetic data & controlled anomaly generator
│   ├── lambda_function.py           # AWS Lambda Handler & ETL transformations
│   ├── main.py                      # Phase 1 Orchestrator
│   ├── monitoring.py                # CloudWatch metrics publisher & SNS alerting client
│   ├── phase3_pipeline.py           # Phase 3 Orchestrator (Quality Engine + Warehouse Load)
│   ├── s3_client.py                 # S3 Manager (boto3 + offline emulator)
│   ├── db/
│   │   ├── __init__.py
│   │   ├── connection.py            # PostgreSQL & SQLite connection context manager
│   │   └── loader.py                # Star schema loader with idempotent upserts
│   └── quality/
│       ├── __init__.py
│       ├── check_amounts.py         # Rule 3: Unit price, quantity, total amount validations
│       ├── check_dates.py           # Rule 4: Timestamp parseability & calendar rules
│       ├── check_duplicates.py      # Rule 1: Primary key order_id & row deduplication
│       ├── check_missing.py         # Rule 2: Non-nullable mandatory field completeness
│       └── engine.py                # DQ Engine orchestrator & report generator
└── tests/
    ├── test_phase2_e2e.py           # Phase 2 S3 -> Lambda -> S3 E2E test
    ├── test_phase2_unit.py          # Phase 2 unit & anti-recursion tests
    ├── test_phase3.py               # Phase 3 modular rules & warehouse loader tests
    ├── test_phase4.py               # Phase 4 analytics & Athena partitioner tests
    ├── test_phase5.py               # Phase 5 comprehensive test suite (reconciliation, SNS)
    └── test_pipeline.py             # Phase 1 pipeline tests
```

---

## 6. Execution Commands & Runbook (Windows)

Detailed AWS Console steps, RDS VPC configuration, and IAM role deployment are in [`HOW_TO_RUN.md`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/HOW_TO_RUN.md).

### 6.1 Setup Environment
```powershell
cd C:\Users\DELL\Downloads\serverless-etl-data-quality-pipeline
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
```

### 6.2 Execute Full End-to-End Validation Run
```powershell
python src/e2e_validator.py
```

### 6.3 Run Complete Automated Test Suite (24 Tests)
```powershell
python -m unittest discover -s tests -p "test_*.py" -v
```

### 6.4 Launch Interactive Streamlit Dashboard
```powershell
streamlit run app.py
```
*(Access dashboard at `http://localhost:8501`)*.
