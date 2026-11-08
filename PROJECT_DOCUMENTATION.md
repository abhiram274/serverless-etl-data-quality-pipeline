# Project Overview & Engineering Whitepaper: Serverless ETL & Data Quality Pipeline on AWS

---

## Executive Summary

The **Serverless ETL & Data Quality Pipeline on AWS** is an enterprise-grade cloud data engineering platform designed to automate the ingestion, schema standardization, multi-stage validation, analytical processing, and business intelligence reporting of high-volume retail transactions.

Built with modern cloud-native design principles (zero-server maintenance, pay-per-execution economics, event-driven reactive scaling, and strict data quality boundaries), this system guarantees that **corrupted, duplicate, or malformed data never contaminates production data warehouses or downstream machine learning systems**, while maintaining a 100% auditable record of all ingested data.

---

## 1. Project Motive & Core Problem Statement

### 1.1 The Industry Challenge
In modern retail, e-commerce, and financial enterprises, data originates from distributed, heterogeneous sources: point-of-sale (POS) terminals, web transactions, mobile applications, and third-party payment gateways. This ingestion stream frequently suffers from severe data anomalies:
- **Duplicate events** caused by network retries or distributed broker duplicates.
- **Missing mandatory attributes** caused by client schema drift or failed frontend form validations.
- **Invalid numerical states** such as negative item pricing, zero basket totals, or rounding errors.
- **Incompatible temporal stamps** caused by regional timezone inconsistencies and malformed strings.

### 1.2 Traditional Architecture Shortcomings
1. **Server Idleness & Inflexibility**: Running 24/7 dedicated compute clusters (such as EC2 or Spark clusters) for intermittent or bursty data leads to enormous cloud waste.
2. **"Garbage In, Garbage Out"**: Many traditional ETL pipelines either silently ingest corrupt data (poisoning analytics tables) or throw unhandled exceptions that halt the entire ingestion batch.
3. **Loss of Telemetry**: Dropping invalid records without tracking failure reasons makes debugging impossible.
4. **Recursive Loops**: Misconfigured event triggers between cloud storage and compute functions often result in catastrophic billing spikes due to recursive loop invocations.

### 1.3 Project Mission
To design and implement a **resilient, serverless, automated lakehouse pipeline** that:
1. Reactively handles CSV ingestion via event-driven storage notifications.
2. Isolates corrupted data without failing the pipeline.
3. Enforces zero-trust anti-recursion barriers.
4. Guarantees idempotent loading into a PostgreSQL star schema warehouse.
5. Provides serverless ad-hoc analytics via AWS Glue and Amazon Athena.
6. Publishes operational telemetry and alarms to CloudWatch and SNS.
7. Surfaces real-time metrics and quality health through an interactive operations dashboard.

---

## 2. Technical Architecture & Component Interaction

```text
                                  ┌─────────────────────────────────────────────────────────────┐
                                  │                  AWS Cloud Serverless Boundary              │
                                  └─────────────────────────────────────────────────────────────┘

  [ POS / E-Commerce ]
   (10,200 Transactions)
            │
            ▼ (Batch Upload)
 ┌──────────────────────┐
 │   S3 Bucket: raw/    │
 └──────────┬───────────┘
            │
            │ (S3 Event: s3:ObjectCreated:Put)
            ▼
 ┌───────────────────────────────────────────────────────────────────────────┐
 │                      AWS Lambda ETL Ingestion Worker                      │
 │  ┌─────────────────────────────────────────────────────────────────────┐  │
 │  │ 1. Zero-Trust Anti-Recursion Gate (Prefix & Destination Validation) │  │
 │  └──────────────────────────────────┬──────────────────────────────────┘  │
 │                                     ▼                                     │
 │  ┌─────────────────────────────────────────────────────────────────────┐  │
 │  │ 2. In-Memory Extraction & Schema Normalization                      │  │
 │  └──────────────────────────────────┬──────────────────────────────────┘  │
 │                                     ▼                                     │
 │  ┌─────────────────────────────────────────────────────────────────────┐  │
 │  │ 3. Modular Data Quality Engine:                                     │  │
 │  │    - Rule 1: check_duplicates.py (Primary Key order_id uniqueness)  │  │
 │  │    - Rule 2: check_missing.py    (Required identifier completeness) │  │
 │  │    - Rule 3: check_amounts.py    (Positive quantity & price bounds) │  │
 │  │    - Rule 4: check_dates.py      (ISO temporal validity)            │  │
 │  └──────────────────────────────────┬──────────────────────────────────┘  │
 └─────────────────────────────────────┼─────────────────────────────────────┘
                                       │
                    ┌──────────────────┴──────────────────┐
     (Valid Records │                                     │ (Invalid Records)
      Pass Rate: 92.65%)                                  │ (Rejection Rate: 7.35%)
                    ▼                                     ▼
      ┌───────────────────────────┐         ┌───────────────────────────┐
      │    S3 Bucket: curated/    │         │  S3 Bucket: quarantine/   │
      │ (Clean, Schema-Enforced)  │         │ (Raw Data + Audit Reason) │
      └─────────────┬─────────────┘         └─────────────┬─────────────┘
                    │                                     │
                    │                                     ▼
                    │                       ┌───────────────────────────┐
                    │                       │    S3 Bucket: reports/    │
                    │                       │ (JSON Quality Audit Logs) │
                    │                       └─────────────┬─────────────┘
                    │                                     │
       ┌────────────┴────────────┐                        ▼
       │                         │          ┌───────────────────────────┐
       ▼                         ▼          │ CloudWatch & SNS Alerting │
┌──────────────┐          ┌──────────────┐  │ - Custom Metrics          │
│ S3:warehouse/│          │  Amazon RDS  │  │ - Failure Pager Alarms    │
│(Hive Parquet)│          │  PostgreSQL  │  │ - Rejection Breaches (>5%)│
└──────┬───────┘          │ (Star Schema)│  └───────────────────────────┘
       │                  └──────┬───────┘
       ▼                         │
┌──────────────┐                 │
│  AWS Glue &  │                 │
│Amazon Athena │─────────────────┼─────────────────────────┐
│(Serverless)  │                 │                         │
└──────────────┘                 ▼                         ▼
                 ┌───────────────────────────────────────────────────────┐
                 │        Interactive Streamlit Operations Dashboard     │
                 │ - Executive KPIs ($2.47M Revenue, 9,450 Orders, etc.) │
                 │ - Dynamic Date Filtering & Interactive Plotly Curves  │
                 │ - Regional & Category Breakdown Visualizations        │
                 │ - Data Quality Health & Root-Cause Rejection Analysis │
                 └───────────────────────────────────────────────────────┘
```

---

## 3. Key Phases & Delivered Capabilities

### Phase 1: Controlled Data Engineering & Baseline Profiling
- **Data Generator** ([`src/generator.py`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/src/generator.py)): Generates a realistic enterprise retail transactions dataset containing exactly 10,200 records across 12 business columns.
- **Controlled Quality Anomalies**: Synthesizes 150 duplicate records, 300 missing field values, 120 faulty financial amounts ($\le \$0$), and 180 unparseable date strings.
- **Pristine Reference Baseline**: Produces a counterpart clean dataset with zero nulls and zero duplicates to serve as a gold standard.
- **Data Quality Profiler** ([`src/explorer.py`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/src/explorer.py)): Calculates descriptive metrics, column distributions, and exports audit report [`reports/statistics_report.json`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/reports/statistics_report.json).

### Phase 2: S3 Data Lake & Serverless Lambda Ingestion
- **Five-Tier S3 Data Lake**: Segregates data into `raw/`, `curated/`, `warehouse/`, `quarantine/`, and `reports/` storage zones.
- **Encryption & Versioning**: Enforces TLS requests only and server-side encryption (`AES256`).
- **Serverless Event-Driven Lambda** ([`src/lambda_function.py`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/src/lambda_function.py)): Automatically invoked upon `s3:ObjectCreated:Put` events in `raw/`.
- **Anti-Recursion Protection**: Implements a zero-trust guard that rejects files outside `raw/` and explicitly blocks downstream prefixes from triggering redundant Lambda invocations.

### Phase 3: Modular Data Quality & PostgreSQL Warehouse
- **Modular Quality Rule Architecture** ([`src/quality/`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/src/quality/)): Segregates validation checks into isolated modules (`check_duplicates.py`, `check_missing.py`, `check_amounts.py`, `check_dates.py`).
- **Data Quarantine Routing**: Corrupted records are written to `quarantine/` with an appended `rejection_reasons` column explaining the failure.
- **Strict Data Isolation**: Zero corrupted records ever enter the database.
- **Star Schema Database** ([`sql/schema.sql`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/sql/schema.sql)): Implements `dim_customer`, `dim_product`, `dim_date`, and `fact_orders` with surrogate primary keys and foreign key constraints.
- **Idempotent Upsert Loader** ([`src/db/loader.py`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/src/db/loader.py)): Uses `ON CONFLICT (order_id) DO UPDATE` to guarantee duplicate pipeline executions never create duplicate facts.

### Phase 4: AWS Glue, Amazon Athena & Streamlit Dashboard
- **Hive-Style Parquet Partitioning** ([`src/athena_exporter.py`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/src/athena_exporter.py)): Organizes warehouse data into `year=YYYY/month=MM/` directory trees to minimize query scan costs.
- **Amazon Athena Analytical Queries** ([`sql/athena_analytical_queries.sql`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/sql/athena_analytical_queries.sql)): Calculates monthly revenue, top 10 products, regional sales performance, cancellation rates, and customer lifetime value (LTV) cohorts.
- **Interactive Streamlit Dashboard** ([`app.py`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/app.py)): Real-time visualization platform featuring:
  - Executive KPI cards.
  - Interactive monthly revenue spline curves and order volume bar charts.
  - Product performance rankings and category market-share doughnut charts.
  - Regional sales distribution maps.
  - Data quality telemetry (pass rates, rejection root-cause charts, historical JSON audit reports).
  - Date interval filtering and one-click CSV data export.

### Phase 5: Monitoring, Automated Testing & Verification
- **CloudWatch Telemetry** ([`src/monitoring.py`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/src/monitoring.py)): Publishes custom metrics (`TotalInputRecords`, `ValidCuratedRecords`, `RejectedQuarantinedRecords`, `QualityPassRatePct`, `ExecutionDurationSeconds`).
- **Amazon SNS Incident Notifications**: Dispatches automated email alerts when a pipeline failure occurs or when the rejection rate exceeds a configurable SLA threshold (e.g. $> 5.0\%$).
- **CloudFormation Alarms Template** ([`cloudwatch/alarms_and_sns.json`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/cloudwatch/alarms_and_sns.json)): Declares infrastructure-as-code alarms for Lambda errors, latency breaches, and high rejection counts.
- **Comprehensive Automated Test Suite** (24 Tests): 100% pass rate across unit, integration, reconciliation, and database tests.
- **End-to-End Orchestrator** ([`src/e2e_validator.py`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/src/e2e_validator.py)): Validates the full workflow from initial raw ingestion to dashboard metrics in a single automated command.

---

## 4. Business Value & Practical Use Cases

### 1. High-Volume E-Commerce & Retail Ingestion
Enables retail brands to ingest multi-channel orders from thousands of stores or web apps, automatically catching formatting flaws or pricing anomalies before reporting daily numbers to finance.

### 2. Safeguarding Downstream Machine Learning
Recommendation engines and demand-forecasting models rely on high-fidelity historical data. By quarantining negative amounts, duplicate transactions, and malformed dates, this pipeline prevents corrupted inputs from degrading ML model accuracy.

### 3. Financial & Compliance Auditing
Rather than silently deleting bad records, this architecture preserves all rejected records in S3 `quarantine/` with human-readable diagnostic tags. Compliance and data governance teams can review audit trails without needing database rollbacks.

### 4. Zero-Maintenance Infrastructure
Because compute is handled by serverless AWS Lambda and Amazon Athena, operational costs are near zero during idle periods and scale automatically when processing large batch uploads.

---

## 5. Technical Specifications & Stack Summary

| Layer | Technology | Primary Role |
| :--- | :--- | :--- |
| **Language & Core Engine** | Python 3.12 | Ingestion logic, transformation, validation, and analytics |
| **Data Manipulation** | Pandas 2.2, NumPy 2.2 | In-memory vectorized transformations and statistical calculations |
| **Storage & Data Lake** | Amazon S3 | Five-zone partitioned storage lake (`raw/`, `curated/`, `warehouse/`, `quarantine/`, `reports/`) |
| **Serverless Compute** | AWS Lambda | Event-driven ETL processing triggered by S3 file arrivals |
| **Data Warehouse** | Amazon RDS PostgreSQL | Star-schema storage with transactional referential integrity and upsert logic |
| **Database Driver** | psycopg (v3) / SQLite | High-performance PostgreSQL connectivity with local offline fallback |
| **Serverless Analytics** | AWS Glue, Amazon Athena | External partitioned SQL table queries over Hive-partitioned Parquet files |
| **Monitoring & Alerting** | AWS CloudWatch, Amazon SNS | Operational metrics publishing, error alarms, and SLA breach notifications |
| **Infrastructure as Code** | AWS CloudFormation | Declarative provisioning of alarms and notification topics |
| **Visual Dashboard** | Streamlit, Plotly | Interactive operations UI, dynamic date filtering, and KPI visualization |
| **Testing & CI/CD** | unittest, pytest, Git, GitHub | 24 automated unit, integration, and reconciliation tests |

---

## 6. How the Pipeline Operates (Daily Workflow)

1. **Drop File**: An upstream system drops a batch of retail transactions into `s3://<bucket>/raw/retail_transactions.csv`.
2. **Auto-Trigger**: S3 detects the new object and triggers `RetailServerlessETLFunction` via an `ObjectCreated` event.
3. **Anti-Recursion Check**: The Lambda function inspects the key prefix. If the file is not inside `raw/` or belongs to downstream folders, it terminates immediately.
4. **Validation & Partitioning**:
   - The modular Data Quality Engine inspects every row against all 4 validation rules.
   - 9,450 clean records are standardized and written to `s3://<bucket>/curated/`.
   - 750 invalid records are written to `s3://<bucket>/quarantine/` with annotated failure reasons.
   - An execution audit summary is written to `s3://<bucket>/reports/quality_audit_<batch>.json`.
5. **Warehouse Load**: The curated records are upserted into the PostgreSQL database (`fact_orders`, `dim_customer`, `dim_product`, `dim_date`) with full foreign-key resolution.
6. **Athena Partitioning**: Clean facts are saved to Hive-partitioned paths (`year=YYYY/month=MM/`) for serverless SQL querying.
7. **Telemetry & Alerts**:
   - Execution duration, record counts, and pass rates are published to CloudWatch.
   - If the rejection rate breaches the 5.0% threshold, Amazon SNS dispatches an alert email to data engineers.
8. **Dashboard Refresh**: Executives and operations teams view updated KPIs, revenue trends, and quality health in the Streamlit web dashboard.

---

## 7. Reference Documentation Links

- **Step-by-Step Command Runbook**: [`HOW_TO_RUN.md`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/HOW_TO_RUN.md)
- **Repository Readme & Architecture**: [`README.md`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/README.md)
- **PostgreSQL Star Schema DDL**: [`sql/schema.sql`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/sql/schema.sql)
- **Athena DDL & Partitions**: [`sql/athena_ddl.sql`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/sql/athena_ddl.sql)
- **Athena Analytical Queries**: [`sql/athena_analytical_queries.sql`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/sql/athena_analytical_queries.sql)
- **CloudWatch & SNS CloudFormation**: [`cloudwatch/alarms_and_sns.json`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/cloudwatch/alarms_and_sns.json)
- **End-to-End Validation Script**: [`src/e2e_validator.py`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/src/e2e_validator.py)
