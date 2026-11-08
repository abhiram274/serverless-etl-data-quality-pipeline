# Comprehensive Operations & Runbook: Phase 1 & Phase 2

This guide details how to set up your local environment on Windows, configure AWS IAM credentials securely, provision S3 & Lambda, prevent recursion loops, and run tests.

---

## 1. Local Environment Setup (Windows)

### Step 1.1: Verify Python 3.12
Open PowerShell:
```powershell
python --version
# or if using direct path:
& C:\Python312\python.exe --version
```

### Step 1.2: Set Up Virtual Environment
```powershell
cd C:\Users\DELL\Downloads\serverless-etl-data-quality-pipeline
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### Step 1.3: Install Dependencies
```powershell
pip install -r requirements.txt
```

### Step 1.4: Environment File Configuration
Copy `.env.example` to `.env`:
```powershell
Copy-Item .env.example .env
```
Populate `.env` with your deployment values:
```ini
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET_NAME=your-unique-retail-datalake-name
```

---

## 2. Connecting to AWS Services

### Method A: AWS CLI Configuration (Standard)
1. Install [AWS CLI v2 for Windows](https://awscli.amazonaws.com/AWSCLIV2.msi).
2. Configure your profile:
   ```powershell
   aws configure
   ```
   Provide:
   - **AWS Access Key ID**: `AKIA...`
   - **AWS Secret Access Key**: `wJalrXUtnFEMI...`
   - **Default region name**: `us-east-1`
   - **Default output format**: `json`

### Method B: Environment Variables (CI/CD or Temporary Sessions)
```powershell
$env:AWS_ACCESS_KEY_ID="AKIA..."
$env:AWS_SECRET_ACCESS_KEY="wJalrXUtnFEMI..."
$env:AWS_SESSION_TOKEN="AQoD..."  # If using temporary credentials/SSO
$env:AWS_DEFAULT_REGION="us-east-1"
```

> **Security Note**: Never hardcode credentials into source code or commit `.env` to Git. The project uses standard credential resolution order via `boto3`.

---

## 3. AWS Console & Infrastructure Deployment Runbook

### Task 1: Create and Configure S3 Data Lake
1. **Create Bucket**:
   - Bucket name: e.g. `retail-serverless-data-lake-prod`
   - Region: `us-east-1`
   - **Bucket Versioning**: **Enable**
   - **Default Encryption**: **Enable Server-Side Encryption (SSE-S3 / AES256)**
   - **Block Public Access**: Keep **all 4 public access blocks checked (Enabled)**.
2. **Apply Bucket Policy**:
   - Go to **Permissions** -> **Bucket Policy** -> Paste content from [`iam/s3_bucket_policy.json`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/iam/s3_bucket_policy.json) (replace `YOUR_BUCKET_NAME` with your bucket name).
   - Enforces TLS only and AES256 encryption.
3. **Initialize Prefixes**:
   - Create empty folder prefixes: `raw/`, `curated/`, `warehouse/`, `quarantine/`, `reports/`.

---

### Task 2: Configure IAM Execution Role for Lambda
1. Open **AWS IAM Console** -> **Roles** -> **Create Role**.
2. **Trusted Entity**: AWS service -> **Lambda**.
3. **Role Name**: `RetailServerlessETLLambdaRole`.
4. **Permissions**:
   - Click **Create Policy** -> **JSON tab**.
   - Paste contents of [`iam/lambda_execution_policy.json`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/iam/lambda_execution_policy.json) (replace `YOUR_BUCKET_NAME`).
   - Policy Name: `RetailServerlessETLS3Policy`.
   - Attach this policy to the role.

---

### Task 3: Deploy AWS Lambda Function
1. **Package Lambda Code**:
   In PowerShell:
   ```powershell
   Compress-Archive -Path src\lambda_function.py, src\s3_client.py -DestinationPath function.zip -Force
   ```
2. **Create Function in AWS Console**:
   - Function Name: `RetailServerlessETLFunction`
   - Runtime: `Python 3.12`
   - Architecture: `x86_64`
   - Execution Role: `RetailServerlessETLLambdaRole`
3. **Add AWS Pandas Layer**:
   - In Lambda Console -> **Layers** -> **Add a layer** -> **AWS Layers** -> Select `AWSSDKPandas-Python312` (provides Pandas & NumPy optimized for Lambda).
4. **Configuration Settings**:
   - Memory: `512 MB` - `1024 MB`
   - Timeout: `2 minutes`
   - Environment Variables:
     - `S3_BUCKET_NAME`: `your-unique-retail-datalake-name`
     - `AWS_DEFAULT_REGION`: `us-east-1`

---

### Task 4: Configure S3 Event Trigger (With Anti-Recursion Safeguard)
1. In S3 Bucket -> **Properties** -> **Event Notifications** -> **Create event notification**.
2. **Event Name**: `RawCSVUploadedTrigger`.
3. **Prefix Filter**: `raw/` *(CRITICAL: Only trigger on files inside raw/)*.
4. **Suffix Filter**: `.csv`.
5. **Event Types**: Check `s3:ObjectCreated:*` (or `Put`, `Post`, `Copy`).
6. **Destination**: `Lambda Function` -> Select `RetailServerlessETLFunction`.
7. **Anti-Recursion Safety Mechanism**:
   - S3 event notification only evaluates `raw/*.csv`.
   - The Lambda handler ([`src/lambda_function.py`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/src/lambda_function.py)) enforces a programmatic guard checking both `ALLOWED_TRIGGER_PREFIX = "raw/"` and explicitly blocking any writes to `curated/`, `quarantine/`, `warehouse/`, or `reports/` from ever being processed.

---

## 4. Execution & Testing Commands

### Run Local Unit & Integration Tests (No AWS account required)
```powershell
python -m unittest discover -s tests -p "test_*.py" -v
```

### Run Phase 2 End-to-End Workflow Verification
```powershell
python tests/test_phase2_e2e.py
```

### Run Phase 1 Data Generator & Profiler
```powershell
python src/main.py
```

### Live AWS End-to-End Test (When connected to AWS)
Upload sample raw file using `s3_client.py`:
```powershell
python -c "from src.s3_client import S3DataLakeManager; mgr = S3DataLakeManager(); mgr.upload_file('data/raw/retail_transactions.csv', 'raw/retail_transactions.csv')"
```
Then inspect CloudWatch logs:
```powershell
aws logs tail /aws/lambda/RetailServerlessETLFunction --follow
```
