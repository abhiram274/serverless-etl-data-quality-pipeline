# Comprehensive Runbook & Operations Guide (Phases 1, 2, and 3)

This operational guide details environment setup, AWS infrastructure configuration (S3, Lambda, RDS PostgreSQL), networking, security groups, IAM authentication, and verification testing on Windows.

---

## 1. Local Environment Setup (Windows)

### Step 1.1: Virtual Environment Setup
Open Windows PowerShell:
```powershell
cd C:\Users\DELL\Downloads\serverless-etl-data-quality-pipeline
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### Step 1.2: Install Pinned Dependencies
```powershell
pip install -r requirements.txt
```
*(Dependencies: `pandas`, `numpy`, `python-dotenv`, `pytest`, `boto3`, `botocore`, `psycopg[binary]`)*.

### Step 1.3: Configure Environment Variables
```powershell
Copy-Item .env.example .env
```
Update `.env` with your deployment values:
- For offline local testing: Leave `USE_SQLITE_FALLBACK=true` (zero network dependency).
- For live AWS RDS deployment: Populate `RDS_HOST`, `RDS_PORT`, `RDS_DB_NAME`, `RDS_USERNAME`, and `RDS_PASSWORD`.

---

## 2. Amazon RDS PostgreSQL Infrastructure Configuration

### Step 2.1: VPC & Subnet Networking Design
For enterprise data warehouse workloads:
1. **VPC**: Deploy in a multi-AZ VPC across at least 2 Availability Zones (e.g. `us-east-1a`, `us-east-1b`).
2. **Subnet Group**:
   - Create a **DB Subnet Group** (`retail-dw-subnet-group`).
   - Associate private subnets (`10.0.10.0/24`, `10.0.11.0/24`).
   - Keep RDS database private (Set **Publicly Accessible**: `No`).

### Step 2.2: Security Group Configuration
Create two security groups:
1. **Lambda Security Group** (`sg-retail-lambda`):
   - **Outbound**: Allow all outbound traffic (`0.0.0.0/0`) or port `5432` to the DB security group.
2. **RDS Security Group** (`sg-retail-rds-db`):
   - **Inbound**: Port `5432` from Source: `sg-retail-lambda` (Security Group reference, never `0.0.0.0/0`).
   - **Outbound**: None.

### Step 2.3: RDS Instance Provisioning
1. Open **Amazon RDS Console** -> **Create database**.
2. **Engine**: PostgreSQL (Version 14+ or 16+).
3. **Template**: Free tier (for development) or Production Multi-AZ.
4. **DB Instance Identifier**: `retail-dw-postgres`.
5. **Master Credentials**:
   - Master username: `dw_admin`
   - Store password securely in **AWS Secrets Manager** (e.g. `retail/rds/credentials`).
6. **Connectivity**:
   - VPC: Select your custom VPC.
   - Subnet Group: `retail-dw-subnet-group`.
   - Public access: **No**.
   - VPC Security Group: `sg-retail-rds-db`.
7. **Database Options**:
   - Initial database name: `retail_warehouse`.
   - Port: `5432`.
   - Storage Encryption: **Enable encryption** using AWS KMS (`aws/rds`).

### Step 2.4: Secure Credential Management via AWS Secrets Manager
In production Lambda execution:
1. Store DB credentials in AWS Secrets Manager as JSON:
   ```json
   {
     "username": "dw_admin",
     "password": "YOUR_STRONG_PASSWORD",
     "host": "retail-dw-postgres.c9xxxx.us-east-1.rds.amazonaws.com",
     "port": 5432,
     "dbname": "retail_warehouse"
   }
   ```
2. Grant Lambda's IAM execution role `secretsmanager:GetSecretValue` on the secret ARN.
3. Retrieve credentials in Lambda initialization using `boto3.client('secretsmanager')` rather than hardcoding in environment variables.

---

## 3. Data Warehouse Star Schema Architecture

SQL script: [`sql/schema.sql`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/sql/schema.sql)

```text
                  ┌──────────────────────┐
                  │     dim_customer     │
                  ├──────────────────────┤
                  │ PK  customer_key     │
                  │ UK  customer_id      │
                  │     region           │
                  └──────────┬───────────┘
                             │ 1
                             │
                             │ N
┌──────────────────────┐  ┌──┴───────────────────┐  ┌──────────────────────┐
│     dim_product      │  │     fact_orders      │  │       dim_date       │
├──────────────────────┤  ├──────────────────────┤  ├──────────────────────┤
│ PK  product_key      ├──┤ PK  order_key        ├──┤ PK  date_key         │
│ UK  product_id       │1N│ UK  order_id         │N1│ UK  full_date        │
│     product_name     │  │ FK  customer_key     │  │     year / quarter   │
│     category         │  │ FK  product_key      │  │     month / day      │
│     unit_price       │  │ FK  date_key         │  │     is_weekend       │
└──────────────────────┘  │     order_timestamp  │  └──────────────────────┘
                          │     quantity         │
                          │     unit_price       │
                          │     total_amount     │
                          │     payment_method   │
                          │     order_status     │
                          │     batch_id         │
                          └──────────────────────┘
```

### Key Engineering Guarantees
1. **Referential Integrity**: Surrogate primary keys (`customer_key`, `product_key`, `date_key`) enforced by Foreign Key constraints.
2. **Idempotent Upsert Logic**: `ON CONFLICT (order_id) DO UPDATE` ensures re-running pipeline batches never creates duplicate rows or violates uniqueness.
3. **Audit Trail**: Every record tracks `batch_id`, `inserted_at`, and `updated_at`.

---

## 4. Execution & Testing Instructions

### Run Complete Test Suite (13 Automated Tests)
Runs Phase 1, Phase 2, and Phase 3 unit & integration tests:
```powershell
python -m unittest discover -s tests -p "test_*.py" -v
```

### Run Phase 3 Pipeline (Quality Engine -> S3 -> Warehouse Load)
```powershell
python src/phase3_pipeline.py
```

### Inspect Output Artifacts
- **Curated Dataset**: `data/clean/` or S3 `curated/`
- **Quarantined Anomalies**: S3 `quarantine/`
- **Quality Audit Report**: [`reports/quality_audit_phase3_batch_001.json`](file:///c:/Users/DELL/Downloads/serverless-etl-data-quality-pipeline/reports/quality_audit_phase3_batch_001.json)
- **Warehouse DB**: `data/warehouse.db` or PostgreSQL `fact_orders` table
