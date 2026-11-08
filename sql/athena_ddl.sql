-- ==============================================================================
-- AWS Glue Data Catalog & Amazon Athena DDL
-- Database & External Partitioned Parquet Tables over S3 Warehouse
-- ==============================================================================

-- 1. Create Athena / Glue Database
CREATE DATABASE IF NOT EXISTS retail_analytics_db
COMMENT 'Retail Data Lakehouse Analytics Database for Serverless Queries'
LOCATION 's3://YOUR_DATALAKE_BUCKET/warehouse/';

-- -----------------------------------------------------------------------------
-- 2. External Partitioned Parquet Table: fact_orders
-- Partitioned by: year, month (Optimizes scan costs & speed)
-- Format: Apache Parquet (Snappy Compressed)
-- -----------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS retail_analytics_db.fact_orders (
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    product_name STRING,
    category STRING,
    quantity INT,
    unit_price DOUBLE,
    total_amount DOUBLE,
    order_timestamp STRING,
    region STRING,
    payment_method STRING,
    order_status STRING,
    batch_id STRING,
    processed_at STRING
)
PARTITIONED BY (
    year STRING,
    month STRING
)
STORED AS PARQUET
LOCATION 's3://YOUR_DATALAKE_BUCKET/warehouse/fact_orders/'
TBLPROPERTIES (
    'parquet.compression'='SNAPPY',
    'classification'='parquet'
);

-- Discover Partitions in Athena
MSCK REPAIR TABLE retail_analytics_db.fact_orders;

-- -----------------------------------------------------------------------------
-- 3. External Dimension Table: dim_customer
-- -----------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS retail_analytics_db.dim_customer (
    customer_id STRING,
    region STRING
)
STORED AS PARQUET
LOCATION 's3://YOUR_DATALAKE_BUCKET/warehouse/dim_customer/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- -----------------------------------------------------------------------------
-- 4. External Dimension Table: dim_product
-- -----------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS retail_analytics_db.dim_product (
    product_id STRING,
    product_name STRING,
    category STRING,
    current_unit_price DOUBLE
)
STORED AS PARQUET
LOCATION 's3://YOUR_DATALAKE_BUCKET/warehouse/dim_product/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');
