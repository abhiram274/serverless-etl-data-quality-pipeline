# Serverless ETL & Data Quality Pipeline on AWS

An enterprise data lake and serverless ETL pipeline built with **Python 3.12, Amazon S3, AWS Lambda, Amazon RDS PostgreSQL, Pandas, CloudWatch, and IAM**.

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
                 │                                         ▼
                 │                             [ S3 Bucket: reports/ ]
                 │                             (JSON Audit & Metrics Reports)
                 ▼
     [ Amazon RDS PostgreSQL Warehouse ]
     (Star Schema: fact_orders, dim_customer, dim_product, dim_date)
```

---

## 2. Directory Layout

```text
serverless-etl-data-quality-pipeline/
├── .env.example                     # Environment template (Local, S3 & RDS)
├── .gitignore                       # Git exclusions (.venv, caches, local DBs)
├── HOW_TO_RUN.md                    # Detailed runbook, AWS RDS networking & console setup
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
│   └── schema.sql                   # Star Schema DDL (PostgreSQL: tables, PKs, FKs, indexes)
├── src/
│   ├── __init__.py
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
    └── test_pipeline.py             # Phase 1 pipeline tests
```

---

## 3. Phase 3 Deliverables

### Part A: Data Quality Engine
- **Modular Quality Checks**:
  1. [`src/quality/check_duplicates.py`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/src/quality/check_duplicates.py): Flags primary business key duplicates.
  2. [`src/quality/check_missing.py`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/src/quality/check_missing.py): Flags null or whitespace-only mandatory fields.
  3. [`src/quality/check_amounts.py`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/src/quality/check_amounts.py): Flags negative prices, zero quantities, and invalid totals.
  4. [`src/quality/check_dates.py`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/src/quality/check_dates.py): Validates ISO timestamps and flags unparseable date strings.
- **Data Partitioning**: Valid records stream to `curated/`; corrupted records stream to `quarantine/` preserving original fields plus a tagged `rejection_reasons` column.
- **Execution Audit Report**: Emits a deterministic JSON report ([`reports/quality_audit_phase3_batch_001.json`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/reports/quality_audit_phase3_batch_001.json)) containing input, valid, invalid, and per-rule failure statistics.

### Part B: Data Warehouse (Star Schema on PostgreSQL)
- **Schema DDL**: [`sql/schema.sql`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/sql/schema.sql) defining `dim_customer`, `dim_product`, `dim_date`, and `fact_orders` with PKs, FKs, and multi-column indexes.
- **Connection Management**: [`src/db/connection.py`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/src/db/connection.py) with contextual transactions and automatic rollback.
- **Idempotent Upsert Loader**: [`src/db/loader.py`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/src/db/loader.py) using `ON CONFLICT (order_id) DO UPDATE` to guarantee that duplicate re-ingestion does not create duplicate rows.
- **Quality Isolation Guarantee**: Only records that passed all four validation rules enter the warehouse.

---

## 4. Test Suite & Verification

All 13 automated tests across Phase 1, Phase 2, and Phase 3 passed:

```powershell
python -m unittest discover -s tests -p "test_*.py" -v
```

Execute the Phase 3 pipeline end-to-end:
```powershell
python src/phase3_pipeline.py
```
