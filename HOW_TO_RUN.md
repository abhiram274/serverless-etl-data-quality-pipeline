# Step-by-Step Execution Guide & Runbook

This guide contains the exact copy-paste PowerShell commands and AWS Console steps to run and verify every component of the **Serverless ETL & Data Quality Pipeline on AWS**.

---

## Table of Contents
1. [Prerequisites & System Check](#1-prerequisites--system-check)
2. [Step 1: Local Environment Setup](#step-1-local-environment-setup)
3. [Step 2: Phase 1 – Data Generation & Profiling](#step-2-phase-1--data-generation--profiling)
4. [Step 3: Phase 2 – S3 Ingestion & Serverless Lambda ETL](#step-3-phase-2--s3-ingestion--serverless-lambda-etl)
5. [Step 4: Phase 3 – Modular Data Quality & PostgreSQL Warehouse](#step-4-phase-3--modular-data-quality--postgresql-warehouse)
6. [Step 5: Phase 4 – Athena Analytics & Streamlit Dashboard](#step-5-phase-4--athena-analytics--streamlit-dashboard)
7. [Step 6: Phase 5 – CloudWatch Monitoring, SNS Alerting & Full E2E Test](#step-6-phase-5--cloudwatch-monitoring-sns-alerting--full-e2e-test)
8. [Step 7: Automated Test Suite (24/24 Tests)](#step-7-automated-test-suite-2424-tests)
9. [Step 8: Deploying to Real AWS (Cloud Deployment Guide)](#step-8-deploying-to-real-aws-cloud-deployment-guide)
10. [Troubleshooting & FAQ](#troubleshooting--faq)

---

## 1. Prerequisites & System Check

Open **Windows PowerShell** (or Terminal) and verify Python:
```powershell
# Navigate into project directory
cd C:\Users\DELL\Downloads\serverless-etl-data-quality-pipeline

# Check Python version (Python 3.12 is recommended)
python --version
```
*(If `python` defaults to Windows Store alias, use your direct binary path like `& C:\Python312\python.exe`)*.

---

## Step 1: Local Environment Setup

Run the following commands to create your isolated virtual environment and install dependencies:

```powershell
# 1. Create a clean virtual environment
python -m venv .venv

# 2. Activate the virtual environment
.\.venv\Scripts\Activate.ps1

# 3. Upgrade pip
python -m pip install --upgrade pip

# 4. Install all project requirements
pip install -r requirements.txt

# 5. Create your environment configuration file from template
Copy-Item .env.example .env
```

> **Note on Offline vs AWS Mode:**  
> By default in `.env`, `USE_SQLITE_FALLBACK=true`. This allows **every test, pipeline stage, and dashboard to execute immediately offline** with zero external network dependencies. When you are ready to connect to Amazon RDS PostgreSQL and live AWS S3, set `USE_SQLITE_FALLBACK=false` and enter your AWS credentials.

---

## Step 2: Phase 1 – Data Generation & Profiling

Generates the exact 10,200 retail records with controlled anomalies (duplicates, nulls, invalid amounts, corrupted dates) and computes statistical metrics:

```powershell
# Run Phase 1 Orchestrator
python src/main.py
```

### Expected Output:
- `data/raw/retail_transactions.csv` (10,200 rows with injected anomalies)
- `data/clean/retail_transactions_clean.csv` (10,200 pristine reference rows)
- `reports/statistics_report.json` (Full profiling & anomaly audit)

---

## Step 3: Phase 2 – S3 Ingestion & Serverless Lambda ETL

Tests S3 bucket prefix partitioning (`raw/`, `curated/`, `warehouse/`, `quarantine/`, `reports/`), anti-recursion trigger protection, and Lambda ETL extraction:

```powershell
# Run Phase 2 End-to-End Simulation
python tests/test_phase2_e2e.py
```

### What this verifies:
- File uploaded to `raw/`
- Anti-recursion filter drops simulated calls outside `raw/`
- Lambda extracts and partitions records into `curated/` and `quarantine/`
- Structured CloudWatch JSON logs emitted to console

---

## Step 4: Phase 3 – Modular Data Quality & PostgreSQL Warehouse

Executes the four modular Data Quality rules, partitions valid vs invalid records, writes audit reports, and loads curated facts into the Star Schema warehouse:

```powershell
# Run Phase 3 Pipeline
python src/phase3_pipeline.py
```

### What this verifies:
- `check_duplicates.py` flags order ID duplicates
- `check_missing.py` flags missing customer IDs and payments
- `check_amounts.py` flags negative prices and totals
- `check_dates.py` flags malformed dates
- `sql/schema.sql` creates tables (`dim_customer`, `dim_product`, `dim_date`, `fact_orders`)
- Idempotent upsert (`ON CONFLICT (order_id) DO UPDATE`) guarantees zero duplicates on re-runs
- 0 invalid records enter the warehouse

---

## Step 5: Phase 4 – Athena Analytics & Streamlit Dashboard

### 5.1 Export Hive Partitions for Athena
Partitions curated data into `year=YYYY/month=MM/` format:
```powershell
python -c "from src.s3_client import S3DataLakeManager; from src.athena_exporter import WarehouseParquetExporter; import pandas as pd; df = pd.read_csv('data/clean/retail_transactions_clean.csv'); WarehouseParquetExporter(S3DataLakeManager()).export_partitioned_warehouse_data(df)"
```

### 5.2 Launch Interactive Streamlit Dashboard
```powershell
streamlit run app.py
```

- Opens your web browser at: **`http://localhost:8501`**
- Features:
  - **Overview KPIs**: Total Revenue, Completed Orders, Customers, Cancellation Rate.
  - **Revenue Analytics**: Interactive line curves with date filter and CSV export.
  - **Product Analytics**: Top 10 products and category market shares.
  - **Regional Analytics**: Territorial distributions.
  - **Data Quality Health**: Quarantined records breakdown, pass rate %, and root-cause failure charts.

---

## Step 6: Phase 5 – CloudWatch Monitoring, SNS Alerting & Full E2E Test

Runs the comprehensive production validation script simulating the entire end-to-end lifecycle:

```powershell
# Run Full End-to-End Validator
python src/e2e_validator.py
```

### Lifecycle steps executed & audited:
1. Generates 10,200 records.
2. Ingests into S3 `raw/`.
3. Triggers Lambda with anti-recursion filter.
4. Reconciles records: $\text{10,200 Total} = \text{9,450 Curated} + \text{750 Quarantined}$.
5. Loads Star Schema warehouse database.
6. Exports Hive-partitioned Athena datasets.
7. Computes and checks dashboard KPIs.
8. Publishes 7 metrics to CloudWatch namespace `RetailServerlessETL`.
9. Dispatches incident alert to Amazon SNS for $> 5.0\%$ rejection rate threshold.

---

## Step 7: Automated Test Suite (24/24 Tests)

Run all unit, integration, and reconciliation tests with a single command:

```powershell
python -m unittest discover -s tests -p "test_*.py" -v
```

*(Runs and passes all 24 automated tests in ~1.5 seconds)*.

---

## Step 8: Deploying to Real AWS (Cloud Deployment Guide)

When ready to connect to real AWS infrastructure:

### 8.1 Configure AWS CLI Credentials
```powershell
# Install AWS CLI v2 if not already installed, then run:
aws configure
```
Provide your:
- `AWS Access Key ID`: `AKIA...`
- `AWS Secret Access Key`: `...`
- `Default region name`: `us-east-1`
- `Default output format`: `json`

### 8.2 Create and Configure S3 Bucket
```powershell
# Define your unique bucket name
$BUCKET_NAME = "my-retail-datalake-prod-" + (Get-Random)

# Create S3 Bucket
aws s3 mb s3://$BUCKET_NAME --region us-east-1

# Enable Bucket Versioning
aws s3api put-bucket-versioning --bucket $BUCKET_NAME --versioning-configuration Status=Enabled

# Enable Default Encryption (SSE-S3 / AES256)
aws s3api put-bucket-encryption --bucket $BUCKET_NAME --server-side-encryption-configuration '{"Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]}'

# Apply Security Bucket Policy (from iam/s3_bucket_policy.json)
(Get-Content iam/s3_bucket_policy.json).replace('YOUR_BUCKET_NAME', $BUCKET_NAME) | Set-Content iam/s3_bucket_policy_active.json
aws s3api put-bucket-policy --bucket $BUCKET_NAME --policy file://iam/s3_bucket_policy_active.json
```

### 8.3 Create IAM Roles for Lambda
```powershell
# Create execution role
aws iam create-role --role-name RetailServerlessETLLambdaRole --assume-role-policy-document file://iam/lambda_assume_role_policy.json

# Attach least-privilege policy (from iam/lambda_execution_policy.json)
(Get-Content iam/lambda_execution_policy.json).replace('YOUR_BUCKET_NAME', $BUCKET_NAME) | Set-Content iam/lambda_execution_policy_active.json
aws iam put-role-policy --role-name RetailServerlessETLLambdaRole --policy-name RetailETLPermissions --policy-document file://iam/lambda_execution_policy_active.json
```

### 8.4 Package and Deploy AWS Lambda Function
```powershell
# 1. Package code
Compress-Archive -Path src\lambda_function.py, src\s3_client.py, src\quality -DestinationPath function.zip -Force

# 2. Create Lambda function with AWS SDK Pandas Layer
aws lambda create-function `
  --function-name RetailServerlessETLFunction `
  --runtime python3.12 `
  --role $(aws iam get-role --role-name RetailServerlessETLLambdaRole --query "Role.Arn" --output text) `
  --handler lambda_function.lambda_handler `
  --zip-file fileb://function.zip `
  --timeout 120 `
  --memory-size 512 `
  --layers "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:1" `
  --environment "Variables={S3_BUCKET_NAME=$BUCKET_NAME,AWS_DEFAULT_REGION=us-east-1}"
```

### 8.5 Configure S3 Trigger (with Anti-Recursion Prefix)
```powershell
# Grant S3 permission to invoke Lambda
aws lambda add-permission `
  --function-name RetailServerlessETLFunction `
  --principal s3.amazonaws.com `
  --statement-id s3invoke-etl `
  --action "lambda:InvokeFunction" `
  --source-arn "arn:aws:s3:::$BUCKET_NAME"

# Configure S3 Event Notification for raw/*.csv
$NOTIFICATION_CONFIG = @"
{
  "LambdaFunctionConfigurations": [
    {
      "LambdaFunctionArn": "$((aws lambda get-function --function-name RetailServerlessETLFunction --query 'Configuration.FunctionArn' --output text))",
      "Events": ["s3:ObjectCreated:*"],
      "Filter": {
        "Key": {
          "FilterRules": [
            { "Name": "prefix", "Value": "raw/" },
            { "Name": "suffix", "Value": ".csv" }
          ]
        }
      }
    }
  ]
}
"@
$NOTIFICATION_CONFIG | Out-File -Encoding ascii s3_notification.json
aws s3api put-bucket-notification-configuration --bucket $BUCKET_NAME --notification-configuration file://s3_notification.json
```

### 8.6 Provision Amazon RDS PostgreSQL
1. Create a PostgreSQL instance in private VPC subnets.
2. In `sg-retail-rds-db`, allow inbound TCP port `5432` only from Lambda security group `sg-retail-lambda`.
3. Run the schema creation script [`sql/schema.sql`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/sql/schema.sql) against your database.
4. Update `.env` with your host, username, and password:
   ```ini
   USE_SQLITE_FALLBACK=false
   RDS_HOST=your-rds-endpoint.us-east-1.rds.amazonaws.com
   RDS_PORT=5432
   RDS_DB_NAME=retail_warehouse
   RDS_USERNAME=dw_admin
   RDS_PASSWORD=YourSecurePassword123!
   ```

### 8.7 Deploy CloudWatch Alarms & SNS Topic
```powershell
aws cloudformation create-stack `
  --stack-name RetailETL-MonitoringStack `
  --template-body file://cloudwatch/alarms_and_sns.json `
  --parameters ParameterKey=AlertEmail,ParameterValue=your-email@example.com ParameterKey=RejectionThresholdCount,ParameterValue=200
```

---

## Troubleshooting & FAQ

### Q1: `python` command fails or opens Microsoft Store
**Fix**: Use the direct path to Python 3.12:
```powershell
& C:\Python312\python.exe src/main.py
```

### Q2: How do I test the pipeline without an AWS account?
**Answer**: You don't need an AWS account. The code automatically detects missing AWS credentials and transparently switches to the local S3 emulator (`.s3_emulator/`) and SQLite database engine (`data/warehouse.db`). Everything runs locally out-of-the-box.

### Q3: How do I verify no invalid records entered the database?
**Verification command**:
```powershell
python -c "from src.db.connection import DatabaseConnection; db = DatabaseConnection(use_sqlite=True); conn = db.get_connection(); cursor = conn.__enter__().cursor(); cursor.execute('SELECT COUNT(*) FROM fact_orders WHERE total_amount <= 0 OR quantity <= 0;'); print('Invalid rows in DB:', cursor.fetchone()[0])"
```
*(Prints `Invalid rows in DB: 0`)*.

---

## Quick Reference Command Summary

| Action | PowerShell Command |
| :--- | :--- |
| **Run Everything (E2E Validation)** | `python src/e2e_validator.py` |
| **Run All 24 Tests** | `python -m unittest discover -s tests -p "test_*.py" -v` |
| **Run Streamlit Dashboard** | `streamlit run app.py` |
| **Regenerate Phase 1 Raw Data** | `python src/main.py` |
| **Run Phase 3 Quality & DB Load** | `python src/phase3_pipeline.py` |
