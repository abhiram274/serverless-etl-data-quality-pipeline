"""Data Warehouse Loader for Star Schema (PostgreSQL & SQLite compatibility).

Loads dimension tables (dim_customer, dim_product, dim_date) and fact table (fact_orders)
with idempotency, upsert handling, and foreign key resolution.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Tuple
import pandas as pd

from src.db.connection import DatabaseConnection

logger = logging.getLogger(__name__)


class WarehouseLoader:
    """Orchestrates loading validated curated records into the Star Schema warehouse."""

    def __init__(self, db_conn: DatabaseConnection):
        self.db = db_conn

    def initialize_schema(self) -> None:
        """Executes table creation DDL."""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            if self.db.use_sqlite:
                cursor.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS dim_customer (
                        customer_key INTEGER PRIMARY KEY AUTOINCREMENT,
                        customer_id TEXT NOT NULL UNIQUE,
                        region TEXT NOT NULL DEFAULT 'UNKNOWN',
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                    );

                    CREATE TABLE IF NOT EXISTS dim_product (
                        product_key INTEGER PRIMARY KEY AUTOINCREMENT,
                        product_id TEXT NOT NULL UNIQUE,
                        product_name TEXT NOT NULL,
                        category TEXT NOT NULL,
                        current_unit_price REAL NOT NULL,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                    );

                    CREATE TABLE IF NOT EXISTS dim_date (
                        date_key INTEGER PRIMARY KEY,
                        full_date TEXT NOT NULL UNIQUE,
                        year INTEGER NOT NULL,
                        quarter INTEGER NOT NULL,
                        month INTEGER NOT NULL,
                        month_name TEXT NOT NULL,
                        day_of_month INTEGER NOT NULL,
                        day_of_week INTEGER NOT NULL,
                        day_name TEXT NOT NULL,
                        is_weekend INTEGER NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS fact_orders (
                        order_key INTEGER PRIMARY KEY AUTOINCREMENT,
                        order_id TEXT NOT NULL UNIQUE,
                        customer_key INTEGER NOT NULL REFERENCES dim_customer(customer_key),
                        product_key INTEGER NOT NULL REFERENCES dim_product(product_key),
                        date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
                        order_timestamp TEXT NOT NULL,
                        quantity INTEGER NOT NULL,
                        unit_price REAL NOT NULL,
                        total_amount REAL NOT NULL,
                        payment_method TEXT NOT NULL,
                        order_status TEXT NOT NULL,
                        batch_id TEXT NOT NULL,
                        inserted_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                    );
                    """
                )
            else:
                # PostgreSQL path: execute sql/schema.sql
                with open("sql/schema.sql", "r", encoding="utf-8") as f:
                    ddl = f.read()
                cursor.execute(ddl)
            logger.info("Warehouse star schema tables initialized successfully.")

    def load_dim_date(self, dates: pd.Series) -> int:
        """Generates and loads unique calendar dimensions for all dates in the batch."""
        unique_dates = pd.to_datetime(dates.dropna().unique())
        if len(unique_dates) == 0:
            return 0

        date_rows = []
        for dt in unique_dates:
            date_key = int(dt.strftime("%Y%m%d"))
            full_date = dt.strftime("%Y-%m-%d")
            year = dt.year
            quarter = (dt.month - 1) // 3 + 1
            month = dt.month
            month_name = dt.strftime("%B")
            day_of_month = dt.day
            day_of_week = dt.isoweekday()
            day_name = dt.strftime("%A")
            is_weekend = 1 if day_of_week in (6, 7) else 0

            date_rows.append(
                (
                    date_key,
                    full_date,
                    year,
                    quarter,
                    month,
                    month_name,
                    day_of_month,
                    day_of_week,
                    day_name,
                    is_weekend,
                )
            )

        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            if self.db.use_sqlite:
                query = """
                INSERT OR IGNORE INTO dim_date (
                    date_key, full_date, year, quarter, month, month_name,
                    day_of_month, day_of_week, day_name, is_weekend
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """
            else:
                query = """
                INSERT INTO dim_date (
                    date_key, full_date, year, quarter, month, month_name,
                    day_of_month, day_of_week, day_name, is_weekend
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date_key) DO NOTHING;
                """
            cursor.executemany(query, date_rows)
        return len(date_rows)

    def load_dim_customer(self, df: pd.DataFrame) -> int:
        """Loads and upserts customers."""
        cust_df = df[["customer_id", "region"]].drop_duplicates(subset=["customer_id"])
        rows = [(row["customer_id"], row["region"]) for _, row in cust_df.iterrows()]

        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            if self.db.use_sqlite:
                query = """
                INSERT INTO dim_customer (customer_id, region)
                VALUES (?, ?)
                ON CONFLICT(customer_id) DO UPDATE SET
                    region = excluded.region,
                    updated_at = CURRENT_TIMESTAMP;
                """
            else:
                query = """
                INSERT INTO dim_customer (customer_id, region)
                VALUES (%s, %s)
                ON CONFLICT(customer_id) DO UPDATE SET
                    region = EXCLUDED.region,
                    updated_at = CURRENT_TIMESTAMP;
                """
            cursor.executemany(query, rows)
        return len(rows)

    def load_dim_product(self, df: pd.DataFrame) -> int:
        """Loads and upserts products."""
        prod_df = df[["product_id", "product_name", "category", "unit_price"]].drop_duplicates(subset=["product_id"])
        rows = [
            (row["product_id"], row["product_name"], row["category"], float(row["unit_price"]))
            for _, row in prod_df.iterrows()
        ]

        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            if self.db.use_sqlite:
                query = """
                INSERT INTO dim_product (product_id, product_name, category, current_unit_price)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(product_id) DO UPDATE SET
                    product_name = excluded.product_name,
                    category = excluded.category,
                    current_unit_price = excluded.current_unit_price,
                    updated_at = CURRENT_TIMESTAMP;
                """
            else:
                query = """
                INSERT INTO dim_product (product_id, product_name, category, current_unit_price)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT(product_id) DO UPDATE SET
                    product_name = EXCLUDED.product_name,
                    category = EXCLUDED.category,
                    current_unit_price = EXCLUDED.current_unit_price,
                    updated_at = CURRENT_TIMESTAMP;
                """
            cursor.executemany(query, rows)
        return len(rows)

    def load_curated_data(self, df: pd.DataFrame, batch_id: str = "batch_001") -> Dict[str, int]:
        """Loads curated transactions into warehouse with FK mapping and idempotent upserts."""
        if df.empty:
            return {"dim_date": 0, "dim_customer": 0, "dim_product": 0, "fact_orders": 0}

        # 1. Load Dimensions
        num_dates = self.load_dim_date(df["order_date"])
        num_customers = self.load_dim_customer(df)
        num_products = self.load_dim_product(df)

        # 2. Fetch Surrogate Keys for Dimensions
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT customer_id, customer_key FROM dim_customer;")
            cust_map = {row[0]: row[1] for row in cursor.fetchall()}

            cursor.execute("SELECT product_id, product_key FROM dim_product;")
            prod_map = {row[0]: row[1] for row in cursor.fetchall()}

        # 3. Assemble Fact Rows
        fact_rows = []
        for _, row in df.iterrows():
            c_key = cust_map.get(row["customer_id"])
            p_key = prod_map.get(row["product_id"])
            dt = pd.to_datetime(row["order_date"])
            d_key = int(dt.strftime("%Y%m%d"))
            order_ts = dt.strftime("%Y-%m-%d %H:%M:%S")

            if c_key is None or p_key is None:
                continue

            fact_rows.append(
                (
                    row["order_id"],
                    c_key,
                    p_key,
                    d_key,
                    order_ts,
                    int(row["quantity"]),
                    float(row["unit_price"]),
                    float(row["total_amount"]),
                    row["payment_method"],
                    row["order_status"],
                    batch_id,
                )
            )

        # 4. Insert Fact Rows (Idempotent: ON CONFLICT(order_id) DO UPDATE)
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            if self.db.use_sqlite:
                query = """
                INSERT INTO fact_orders (
                    order_id, customer_key, product_key, date_key, order_timestamp,
                    quantity, unit_price, total_amount, payment_method, order_status, batch_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(order_id) DO UPDATE SET
                    quantity = excluded.quantity,
                    unit_price = excluded.unit_price,
                    total_amount = excluded.total_amount,
                    payment_method = excluded.payment_method,
                    order_status = excluded.order_status,
                    batch_id = excluded.batch_id,
                    updated_at = CURRENT_TIMESTAMP;
                """
            else:
                query = """
                INSERT INTO fact_orders (
                    order_id, customer_key, product_key, date_key, order_timestamp,
                    quantity, unit_price, total_amount, payment_method, order_status, batch_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(order_id) DO UPDATE SET
                    quantity = EXCLUDED.quantity,
                    unit_price = EXCLUDED.unit_price,
                    total_amount = EXCLUDED.total_amount,
                    payment_method = EXCLUDED.payment_method,
                    order_status = EXCLUDED.order_status,
                    batch_id = EXCLUDED.batch_id,
                    updated_at = CURRENT_TIMESTAMP;
                """
            cursor.executemany(query, fact_rows)

        logger.info(f"Loaded {len(fact_rows)} facts into fact_orders (batch: {batch_id}).")
        return {
            "dim_date": num_dates,
            "dim_customer": num_customers,
            "dim_product": num_products,
            "fact_orders": len(fact_rows),
        }
