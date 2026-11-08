# Serverless ETL & Data Quality Pipeline on AWS

An enterprise data lake and serverless ETL pipeline built with **Python 3.12, Amazon S3, AWS Lambda, Pandas, CloudWatch, and IAM**.

---

## 1. Architecture Overview

```text
[ Raw Data Ingestion ] ──────> [ S3 Bucket: raw/ ]
                                        │
                               (S3 ObjectCreated:Put)
                                        ▼
                             [ AWS Lambda ETL Worker ]
                             ├── 1. Anti-Recursion Gate
                             ├── 2. Extraction & In-Memory Read
                             ├── 3. Schema Standardization
                             ├── 4. Data Quality Rules & DQ Routing
                             └── 5. Structured CloudWatch Logging
                                        │
                   ┌────────────────────┴────────────────────┐
                   ▼                                         ▼
       [ S3 Bucket: curated/ ]                   [ S3 Bucket: quarantine/ ]
       (Clean, validated & casted)               (Rejected with audit reason)
                   │
                   ▼
       [ S3 Bucket: reports/ ]
       (Execution JSON telemetry)
```

---

## 2. Directory Structure

```text
serverless-etl-data-quality-pipeline/
├── .env.example                     # Environment template (Local & AWS)
├── .gitignore                       # Git exclusions
├── HOW_TO_RUN.md                    # Detailed runbook & AWS setup guide
├── README.md                        # Project documentation
├── requirements.txt                 # Dependencies
├── data/
│   ├── clean/                       # Reference clean dataset (10,200 rows)
│   │   └── retail_transactions_clean.csv
│   └── raw/                         # Raw dataset with anomalies (10,200 rows)
│       └── retail_transactions.csv
├── iam/
│   ├── lambda_assume_role_policy.json # IAM Trust Policy
│   ├── lambda_execution_policy.json   # Least-privilege Lambda Policy (S3 + CW)
│   └── s3_bucket_policy.json          # S3 TLS & Encryption Enforcement
├── reports/
│   └── statistics_report.json       # Phase 1 Data Exploration Report
├── src/
│   ├── __init__.py
│   ├── explorer.py                  # Profiling & Exploration module
│   ├── generator.py                 # Synthetic data & anomaly generator
│   ├── lambda_function.py           # AWS Lambda Handler & ETL transformations
│   ├── main.py                      # Phase 1 Orchestrator
│   └── s3_client.py                 # S3 Manager (boto3 + offline emulator)
└── tests/
    ├── test_phase2_e2e.py           # Phase 2 S3 -> Lambda -> S3 E2E test
    ├── test_phase2_unit.py          # Phase 2 unit & anti-recursion tests
    └── test_pipeline.py             # Phase 1 pipeline tests
```

---

## 3. Data Lake Prefix Structure

The S3 bucket enforces a partitioned five-tier storage lake:
1. `raw/`: Unprocessed batch and streaming ingestion drops.
2. `curated/`: Cleaned, schema-standardized, validated records ready for consumption.
3. `warehouse/`: Parquet-compacted analytical partitions for Athena/Glue.
4. `quarantine/`: Failed records with tagged `rejection_reasons`.
5. `reports/`: Audit logs, profiling summaries, and execution metrics.

---

## 4. Key Engineering Implementations in Phase 2

### 4.1 Anti-Recursion Trigger Safeguard
Recursive S3-Lambda triggering occurs when an ETL Lambda writes back to the same prefix it listens to.
- **S3 Level**: Notification filter is strictly configured with prefix `raw/` and suffix `.csv`.
- **Lambda Code Level**: [`src/lambda_function.py`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/src/lambda_function.py) enforces a zero-trust guard that rejects keys outside `raw/` and drops events belonging to `curated/`, `warehouse/`, `quarantine/`, or `reports/`.

### 4.2 Standardizations & Data Quality Rules
- Lowercased and stripped whitespace in column headers.
- Formatted timestamps to standard ISO `YYYY-MM-DD HH:MM:SS`.
- Cast quantities and prices to standard `int` and `round(float, 2)`.
- Records with missing IDs, negative amounts, or malformed dates are routed to `quarantine/` with explicit root-cause tags (`MISSING_CUSTOMER_ID`, `INVALID_UNIT_PRICE`, `MALFORMED_DATE`, etc.).

### 4.3 Structured CloudWatch Logging
All Lambda invocations produce JSON log lines containing execution telemetry (`event_type`, `metrics`, `rejection_summary`), queryable in **CloudWatch Logs Insights**.

---

## 5. Quick Start (Windows)

Detailed step-by-step instructions with AWS Console click-paths are in [`HOW_TO_RUN.md`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/HOW_TO_RUN.md).

### Run Test Suite (7/7 Tests Passed)
```powershell
python -m unittest discover -s tests -p "test_*.py" -v
```

### Run Phase 2 End-to-End Simulation
```powershell
python tests/test_phase2_e2e.py
```
