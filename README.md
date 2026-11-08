# Serverless ETL & Data Quality Pipeline on AWS (Phase 1)

A production-grade, modular foundation for an enterprise retail transaction ETL and data quality pipeline.

This repository implements **Phase 1: Local Baseline, Controlled Data Generation, Anomaly Injection, and Profiling Audit Engine**. AWS cloud services (S3, Lambda, Glue, Athena, Step Functions, CloudWatch) will be integrated in Phase 2.

---

## 1. Architecture & Project Layout

```text
serverless-etl-data-quality-pipeline/
├── .env.example                     # Environment template (local & AWS staging)
├── .gitignore                       # Git exclusion rules
├── README.md                        # Project documentation & operational runbook
├── requirements.txt                 # Pinned dependencies
├── data/
│   ├── clean/                       # Clean reference baseline data
│   │   └── retail_transactions_clean.csv
│   └── raw/                         # Raw retail data with controlled anomalies
│       └── retail_transactions.csv
├── reports/
│   └── statistics_report.json       # Generated data profiling & quality report
├── src/
│   ├── __init__.py
│   ├── generator.py                 # Synthetic dataset generator & anomaly injector
│   ├── explorer.py                  # Profiling, statistical exploration & DQ validator
│   └── main.py                      # Orchestration entry point
└── tests/
    └── test_pipeline.py             # Unit & integration tests
```

---

## 2. Dataset Schema (12 Fields)

Both the raw and clean datasets contain **exactly 10,200 records** conforming to this schema:

| Column | Data Type | Description | Example |
| :--- | :--- | :--- | :--- |
| `order_id` | String | Unique transaction identifier | `ORD-0000001` |
| `customer_id` | String | Customer identity key | `CUST-01245` |
| `product_id` | String | Product SKU | `PRD-ELC-001` |
| `product_name` | String | Commercial product title | `Ultra HD 4K Monitor 27-inch` |
| `category` | String | Product category | `Electronics` |
| `quantity` | Integer | Items purchased | `2` |
| `unit_price` | Float | Unit price in USD | `329.99` |
| `total_amount` | Float | Total transaction amount | `659.98` |
| `order_date` | String / DateTime | ISO Timestamp of order | `2025-06-14 18:22:10` |
| `region` | String | Customer geographical region | `North America` |
| `payment_method` | String | Payment method used | `Credit Card` |
| `order_status` | String | Fulfillment lifecycle state | `Delivered` |

---

## 3. Controlled Data Quality Anomalies (Injected in Raw Dataset)

To simulate real-world streaming/batch ingestion issues that the Phase 2 AWS serverless pipeline will validate and remediate:
1. **Duplicates**: 150 injected records (142 complete row duplicates, 150 duplicate order IDs).
2. **Missing Fields (Null / NaN)**: Exactly 300 missing values distributed evenly across `customer_id` (100), `payment_method` (100), and `quantity` (100).
3. **Invalid Amounts**: 120 faulty amounts, including negative unit prices (`-$X`), zero totals (`$0.00`), and negative totals (`-$99.99`).
4. **Malformed Dates**: 180 unparseable date strings (e.g., `INVALID_DATE_STRING`, `2025/31/12 25:99:99`, `31-02-2025`, `9999-99-99`).

The **clean reference dataset** maintains zero nulls, zero duplicates, and strictly valid calculations.

---

## 4. Windows Installation & Setup Guide

### Prerequisites
- Python 3.12 (e.g. `C:\Python312\python.exe` or installed in `PATH`)
- Windows PowerShell or Command Prompt

### Step 1: Open PowerShell in Project Directory
```powershell
cd C:\Users\DELL\Downloads\serverless-etl-data-quality-pipeline
```

### Step 2: (Optional) Create and Activate Virtual Environment
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

> **Note**: If your system has multiple Python versions or alias redirection issues with Windows Store `python.exe`, use your direct Python 3.12 binary path (e.g. `C:\Python312\python.exe`).

### Step 3: Install Dependencies
```powershell
pip install -r requirements.txt
```

### Step 4: Configure Environment Variables
```powershell
Copy-Item .env.example .env
```

---

## 5. Execution Commands

### Run Full Phase 1 Pipeline (Data Generation + Profiling)
```powershell
python src/main.py
```
*(Or specify custom parameters)*:
```powershell
python src/main.py --records 10200 --seed 42 --raw-path data/raw/retail_transactions.csv --clean-path data/clean/retail_transactions_clean.csv --report-path reports/statistics_report.json
```

### Run Generator Module Standalone
```powershell
python src/generator.py
```

### Run Profiler & Report Generator Standalone
```powershell
python src/explorer.py
```

### Run Test Suite
```powershell
python -m unittest discover -s tests -p "test_*.py" -v
```
*(Or with pytest once installed)*:
```powershell
pytest -v
```

---

## 6. Phase 1 Verification Results

- **Raw Data Path**: [`data/raw/retail_transactions.csv`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/data/raw/retail_transactions.csv) (10,200 rows)
- **Clean Data Path**: [`data/clean/retail_transactions_clean.csv`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/data/clean/retail_transactions_clean.csv) (10,200 rows)
- **Quality Audit Report**: [`reports/statistics_report.json`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/reports/statistics_report.json)
- **Test Suite Status**: 3/3 Tests Passed.
