-- ==============================================================================
-- Schema: Retail Transaction Enterprise Data Warehouse (Star Schema)
-- Target RDBMS: Amazon RDS PostgreSQL / PostgreSQL 14+
-- ==============================================================================

-- Drop tables in reverse dependency order if recreating
DROP TABLE IF EXISTS fact_orders CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;

-- -----------------------------------------------------------------------------
-- 1. Dimension: dim_customer
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(64) NOT NULL UNIQUE,
    region VARCHAR(64) NOT NULL DEFAULT 'UNKNOWN',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_customer_id ON dim_customer(customer_id);
CREATE INDEX IF NOT EXISTS idx_dim_customer_region ON dim_customer(region);

-- -----------------------------------------------------------------------------
-- 2. Dimension: dim_product
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(64) NOT NULL UNIQUE,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(128) NOT NULL,
    current_unit_price NUMERIC(12, 2) NOT NULL CHECK (current_unit_price > 0),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_product_id ON dim_product(product_id);
CREATE INDEX IF NOT EXISTS idx_dim_product_category ON dim_product(category);

-- -----------------------------------------------------------------------------
-- 3. Dimension: dim_date
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY, -- Format: YYYYMMDD (e.g. 20250614)
    full_date DATE NOT NULL UNIQUE,
    year INT NOT NULL,
    quarter INT NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    month INT NOT NULL CHECK (month BETWEEN 1 AND 12),
    month_name VARCHAR(16) NOT NULL,
    day_of_month INT NOT NULL CHECK (day_of_month BETWEEN 1 AND 31),
    day_of_week INT NOT NULL CHECK (day_of_week BETWEEN 1 AND 7),
    day_name VARCHAR(16) NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dim_date_full_date ON dim_date(full_date);
CREATE INDEX IF NOT EXISTS idx_dim_date_year_month ON dim_date(year, month);

-- -----------------------------------------------------------------------------
-- 4. Fact Table: fact_orders
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_orders (
    order_key BIGSERIAL PRIMARY KEY,
    order_id VARCHAR(64) NOT NULL UNIQUE, -- Natural transaction business key for idempotency
    customer_key INT NOT NULL REFERENCES dim_customer(customer_key),
    product_key INT NOT NULL REFERENCES dim_product(product_key),
    date_key INT NOT NULL REFERENCES dim_date(date_key),
    order_timestamp TIMESTAMP NOT NULL,
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(12, 2) NOT NULL CHECK (unit_price > 0),
    total_amount NUMERIC(12, 2) NOT NULL CHECK (total_amount > 0),
    payment_method VARCHAR(64) NOT NULL,
    order_status VARCHAR(64) NOT NULL,
    batch_id VARCHAR(64) NOT NULL,
    inserted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_orders_order_id ON fact_orders(order_id);
CREATE INDEX IF NOT EXISTS idx_fact_orders_customer_key ON fact_orders(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_product_key ON fact_orders(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_date_key ON fact_orders(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_status ON fact_orders(order_status);
