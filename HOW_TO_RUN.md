# Comprehensive Runbook: Phases 1, 2, 3, and 4

This operations manual details environment setup on Windows, AWS services configuration (S3, Lambda, RDS PostgreSQL, Glue Data Catalog, Athena), and running the Streamlit dashboard.

---

## 1. Local Environment Setup (Windows)

### Step 1.1: Activate Virtual Environment
Open Windows PowerShell:
```powershell
cd C:\Users\DELL\Downloads\serverless-etl-data-quality-pipeline
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### Step 1.2: Install Full Project Dependencies
```powershell
pip install -r requirements.txt
```
*(Packages: `pandas`, `numpy`, `python-dotenv`, `pytest`, `boto3`, `botocore`, `psycopg[binary]`, `streamlit`, `plotly`, `pyarrow`)*.

### Step 1.3: Environment Configuration
Copy `.env.example` to `.env`:
```powershell
Copy-Item .env.example .env
```
Ensure configuration matches your targets:
- `USE_SQLITE_FALLBACK=true` allows running all tests and dashboard locally with zero external network dependencies.
- Set `USE_SQLITE_FALLBACK=false` with your `RDS_HOST` and credentials to query live Amazon RDS PostgreSQL.

---

## 2. AWS Glue Data Catalog & Amazon Athena Configuration

### Step 2.1: S3 Warehouse Prefix Layout
The S3 data lake organizes analytical data into Hive-partitioned directory paths:
```text
s3://YOUR_DATALAKE_BUCKET/warehouse/fact_orders/year=YYYY/month=MM/*.parquet
```

### Step 2.2: Glue Data Catalog Setup
1. Open **AWS Glue Console** -> **Databases** -> **Add database**.
   - Name: `retail_analytics_db`
   - Location: `s3://YOUR_DATALAKE_BUCKET/warehouse/`
2. **Option A: Automated Glue Crawler**:
   - Create crawler `retail-warehouse-crawler` targeting `s3://YOUR_DATALAKE_BUCKET/warehouse/`.
   - Output database: `retail_analytics_db`.
   - Run crawler to auto-discover partitions (`year`, `month`).
3. **Option B: Athena DDL Execution**:
   - Open **Amazon Athena Query Editor**.
   - Run DDL script: [`sql/athena_ddl.sql`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/sql/athena_ddl.sql).
   - Discover partitions: `MSCK REPAIR TABLE retail_analytics_db.fact_orders;`.

### Step 2.3: Running Athena Analytical Queries
Execute analytical SQL queries defined in [`sql/athena_analytical_queries.sql`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/sql/athena_analytical_queries.sql):
- **Query 1**: Total and monthly revenue aggregations.
- **Query 2**: Top 10 products by revenue and units sold.
- **Query 3**: Regional sales performance and market share percentages.
- **Query 4**: Order cancellation rate and regional fulfillment analysis.
- **Query 5**: Customer lifetime spend cohorts and purchasing velocity.

---

## 3. Interactive Streamlit Dashboard

The dashboard ([`app.py`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/app.py)) connects directly to the curated warehouse database and historical JSON quality audit reports.

### Features
1. **Executive Overview**: Headline KPIs (Total Revenue, Orders, Unique Customers, Cancellation Rate, Average Order Value).
2. **Revenue Analytics**: Interactive monthly revenue trend line charts and completed order bars with CSV export.
3. **Product & Category Analytics**: Horizontal bar chart of Top 10 revenue-generating products and category doughnut distribution.
4. **Regional Analytics**: Territorial revenue distribution and order volumes.
5. **Data Quality Health**: Live inspection of latest batch health (Pass rate %, Quarantined counts, Rejection root-cause bars, historical JSON audit logs).

### Launching the Dashboard
From your PowerShell terminal:
```powershell
streamlit run app.py
```
Access the dashboard in your web browser at: `http://localhost:8501`.

---

## 4. Full Automated Test Suite Execution

All 18 automated tests verify Phase 1 (Data generation & controlled anomalies), Phase 2 (S3 event triggers & anti-recursion), Phase 3 (Modular DQ engine & star schema loader), and Phase 4 (Analytics service & KPIs).

Run from PowerShell:
```powershell
python -m unittest discover -s tests -p "test_*.py" -v
```
*(All 18/18 tests pass in ~1 second)*.
