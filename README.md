# Serverless ETL & Data Quality Pipeline on AWS

An enterprise data lake and serverless ETL pipeline built with **Python 3.12, Amazon S3, AWS Lambda, Amazon RDS PostgreSQL, AWS Glue, Amazon Athena, Streamlit, Plotly, Pandas, and IAM**.

---

## 1. High-Level Architecture

```text
[ Raw CSV Ingestion ] ──────> [ S3 Bucket: raw/ ]
                                      │
                             (S3 ObjectCreated Event)
                                      ▼
                           [ AWS Lambda ETL Worker ]
                           ├── Anti-Recursion Gate
                           └── Modular Data Quality Engine:
                               ├── check_duplicates.py
                               ├── check_missing.py
                               ├── check_amounts.py
                               └── check_dates.py
                                      │
                 ┌────────────────────┴────────────────────┐
                 ▼                                         ▼
     [ S3 Bucket: curated/ ]                   [ S3 Bucket: quarantine/ ]
     (Standardized, 0 DQ errors)               (Preserved raw rows + rejection_reasons)
                 │                                         │
                 ├────────────────────────┐                ▼
                 ▼                        ▼    [ S3 Bucket: reports/ ]
     [ S3: warehouse/ ]     [ Amazon RDS PostgreSQL ]  (Execution Audit Reports)
     (Hive Parquet / CSV)   (Star Schema Warehouse)                │
     year=YYYY/month=MM/                  │                        │
                 │                        └────────────┬───────────┘
                 ▼                                     ▼
     [ AWS Glue & Athena ]                 [ Streamlit Dashboard ]
     (Serverless Analytics)                (Interactive Visualizations)
```

---

## 2. Project Directory Layout

```text
serverless-etl-data-quality-pipeline/
├── .env.example                     # Environment template (Local, S3 & RDS)
├── .gitignore                       # Git exclusions (.venv, caches, local DBs)
├── app.py                           # Interactive Streamlit Analytics & Quality Dashboard
├── HOW_TO_RUN.md                    # Detailed runbook & AWS configuration guide
├── README.md                        # Project documentation
├── requirements.txt                 # Pinned dependencies
├── data/
│   ├── clean/                       # Reference clean dataset
│   │   └── retail_transactions_clean.csv
│   └── raw/                         # Raw dataset with injected anomalies (10,200 rows)
│       └── retail_transactions.csv
├── iam/
│   ├── lambda_assume_role_policy.json # IAM Trust Policy for Lambda
│   ├── lambda_execution_policy.json   # Least-privilege Lambda Policy (S3 + CW)
│   └── s3_bucket_policy.json          # S3 TLS & Encryption Enforcement
├── reports/
│   ├── quality_audit_phase3_batch_001.json # Phase 3 Data Quality Execution Audit Report
│   └── statistics_report.json              # Phase 1 Data Exploration Report
├── sql/
│   ├── athena_analytical_queries.sql# 5 Athena queries (Revenue, Products, Regions, Trends)
│   ├── athena_ddl.sql               # Athena / Glue Catalog Partitioned Table DDL
│   └── schema.sql                   # Star Schema DDL (PostgreSQL: tables, PKs, FKs, indexes)
├── src/
│   ├── __init__.py
│   ├── analytics_service.py         # Dashboard analytics & KPI aggregation engine
│   ├── athena_exporter.py           # S3 warehouse Hive partitioner (year=YYYY/month=MM)
│   ├── explorer.py                  # Profiling & Exploration module
│   ├── generator.py                 # Synthetic data & controlled anomaly generator
│   ├── lambda_function.py           # AWS Lambda Handler & ETL transformations
│   ├── main.py                      # Phase 1 Orchestrator
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
    └── test_pipeline.py             # Phase 1 pipeline tests
```

---

## 3. Phase 4 Deliverables

### PART A: AWS Glue Data Catalog & Amazon Athena
1. **Catalog & Partitioned DDL**: [`sql/athena_ddl.sql`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/sql/athena_ddl.sql) defines `retail_analytics_db` and partitioned external tables (`year`, `month`) targeting Snappy-compressed Parquet.
2. **Analytical SQL Queries**: [`sql/athena_analytical_queries.sql`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/sql/athena_analytical_queries.sql):
   - Total & monthly revenue aggregations.
   - Top 10 products by revenue and units sold.
   - Regional sales performance and market share percentages.
   - Order cancellation rate by territory.
   - Customer purchasing cohorts and lifetime value (LTV) segmentation.
3. **Warehouse Partitioner**: [`src/athena_exporter.py`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/src/athena_exporter.py) partitions curated records into S3 `warehouse/fact_orders/year=YYYY/month=MM/` for Athena querying.

### PART B: Interactive Streamlit Dashboard
1. **Dashboard UI Application**: [`app.py`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/app.py) powered by Plotly and Streamlit.
2. **Analytics Service**: [`src/analytics_service.py`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/src/analytics_service.py) executes queries against PostgreSQL (with SQLite fallback).
3. **Core Dashboard Features**:
   - **Executive KPIs**: Total revenue, orders, customers, cancellation rate %, average order value.
   - **Revenue Analytics**: Interactive line and bar trends with date range filter and one-click CSV export.
   - **Product Analytics**: Top 10 products and category revenue share pie chart.
   - **Regional Analytics**: Territorial revenue bars and order volumes.
   - **Data Quality Health**: Live metrics on pass rates, quarantined counts, rejection root-cause charts, and historical execution reports.

---

## 4. Test Suite & Verification

All **18 automated tests** across Phases 1, 2, 3, and 4 passed:

```powershell
python -m unittest discover -s tests -p "test_*.py" -v
```

### Launch Dashboard
```powershell
streamlit run app.py
```
*(Runs at `http://localhost:8501`)*.
