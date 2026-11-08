"""Data generator module for retail transactions.

Generates realistic clean reference datasets and controlled corrupted
datasets for ETL and data quality pipeline validation.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Catalog of realistic retail products mapped to category and standard base price
CATALOG: List[Dict[str, object]] = [
    {"product_id": "PRD-ELC-001", "product_name": "Ultra HD 4K Monitor 27-inch", "category": "Electronics", "unit_price": 329.99},
    {"product_id": "PRD-ELC-002", "product_name": "Mechanical Gaming Keyboard RGB", "category": "Electronics", "unit_price": 89.50},
    {"product_id": "PRD-ELC-003", "product_name": "Noise Cancelling Wireless Headphones", "category": "Electronics", "unit_price": 199.99},
    {"product_id": "PRD-ELC-004", "product_name": "Ergonomic Optical Wireless Mouse", "category": "Electronics", "unit_price": 45.00},
    {"product_id": "PRD-APP-101", "product_name": "Classic Oxford Cotton Shirt", "category": "Apparel", "unit_price": 54.00},
    {"product_id": "PRD-APP-102", "product_name": "Slim Fit Denim Jeans", "category": "Apparel", "unit_price": 68.50},
    {"product_id": "PRD-APP-103", "product_name": "Thermal Running Jacket", "category": "Apparel", "unit_price": 115.00},
    {"product_id": "PRD-HOM-201", "product_name": "Stainless Steel Espresso Machine", "category": "Home & Kitchen", "unit_price": 249.00},
    {"product_id": "PRD-HOM-202", "product_name": "Cast Iron Dutch Oven 5.5 Qt", "category": "Home & Kitchen", "unit_price": 85.00},
    {"product_id": "PRD-HOM-203", "product_name": "Robotic Vacuum Cleaner Pro", "category": "Home & Kitchen", "unit_price": 399.99},
    {"product_id": "PRD-BEA-301", "product_name": "Hydrating Facial Moisturizer 100ml", "category": "Beauty & Personal Care", "unit_price": 28.50},
    {"product_id": "PRD-BEA-302", "product_name": "Organic Vitamin C Serum", "category": "Beauty & Personal Care", "unit_price": 34.00},
    {"product_id": "PRD-SPT-401", "product_name": "Adjustable Dumbbell Set 50lbs", "category": "Sports & Outdoors", "unit_price": 179.00},
    {"product_id": "PRD-SPT-402", "product_name": "Waterproof 2-Person Camping Tent", "category": "Sports & Outdoors", "unit_price": 149.95},
    {"product_id": "PRD-BOK-501", "product_name": "Designing Data-Intensive Applications", "category": "Books", "unit_price": 42.50},
]

REGIONS = ["North America", "Europe", "Asia-Pacific", "Latin America", "Middle East & Africa"]
PAYMENT_METHODS = ["Credit Card", "Debit Card", "PayPal", "Bank Transfer", "Apple Pay"]
ORDER_STATUSES = ["Delivered", "Shipped", "Processing", "Cancelled", "Returned"]


@dataclass(frozen=True)
class GeneratorConfig:
    total_records: int = 10_200
    random_seed: int = 42
    start_date: datetime = datetime(2025, 1, 1)
    end_date: datetime = datetime(2025, 12, 31)
    num_customers: int = 2_500


def generate_clean_dataset(config: GeneratorConfig = GeneratorConfig()) -> pd.DataFrame:
    """Generates a consistent, strictly clean baseline retail transaction dataset."""
    rng = np.random.default_rng(config.random_seed)
    n = config.total_records

    order_ids = [f"ORD-{idx:07d}" for idx in range(1, n + 1)]
    customer_ids = [f"CUST-{rng.integers(1, config.num_customers + 1):05d}" for _ in range(n)]

    product_indices = rng.integers(0, len(CATALOG), size=n)
    product_records = [CATALOG[idx] for idx in product_indices]

    product_ids = [p["product_id"] for p in product_records]
    product_names = [p["product_name"] for p in product_records]
    categories = [p["category"] for p in product_records]
    unit_prices = [float(p["unit_price"]) for p in product_records]

    # Quantities: realistic skewed retail basket quantity (1 to 5 units predominantly)
    quantities = [int(q) for q in rng.choice([1, 2, 3, 4, 5, 8, 10], size=n, p=[0.50, 0.25, 0.12, 0.06, 0.04, 0.02, 0.01])]
    total_amounts = [round(float(q * p), 2) for q, p in zip(quantities, unit_prices)]

    # Date range generation
    day_span = (config.end_date - config.start_date).days
    random_days = rng.integers(0, day_span + 1, size=n)
    random_seconds = rng.integers(0, 86400, size=n)
    order_dates = [
        str((config.start_date + timedelta(days=int(d), seconds=int(s))).strftime("%Y-%m-%d %H:%M:%S"))
        for d, s in zip(random_days, random_seconds)
    ]

    regions = [str(r) for r in rng.choice(REGIONS, size=n, p=[0.40, 0.25, 0.20, 0.10, 0.05])]
    payment_methods = [str(m) for m in rng.choice(PAYMENT_METHODS, size=n, p=[0.45, 0.20, 0.15, 0.10, 0.10])]
    order_statuses = [str(s) for s in rng.choice(ORDER_STATUSES, size=n, p=[0.60, 0.15, 0.12, 0.08, 0.05])]

    df = pd.DataFrame(
        {
            "order_id": order_ids,
            "customer_id": customer_ids,
            "product_id": product_ids,
            "product_name": product_names,
            "category": categories,
            "quantity": quantities,
            "unit_price": unit_prices,
            "total_amount": total_amounts,
            "order_date": order_dates,
            "region": regions,
            "payment_method": payment_methods,
            "order_status": order_statuses,
        }
    )
    return df


def inject_controlled_anomalies(
    df: pd.DataFrame,
    seed: int = 42,
    duplicate_count: int = 150,
    missing_count: int = 300,
    invalid_amount_count: int = 120,
    malformed_date_count: int = 180,
) -> pd.DataFrame:
    """Injects intentional, realistic data-quality anomalies while keeping row count exact.

    Controlled issues injected:
    1. Duplicates: Replaces records with duplicates of earlier rows.
    2. Missing values: Replaces key fields (customer_id, payment_method, quantity) with NaN/None.
    3. Invalid amounts: Injects negative unit prices, $0 totals, or negative totals.
    4. Malformed dates: Injects varied invalid/unparseable date strings and conflicting date formats.
    """
    rng = np.random.default_rng(seed)
    corrupted_df = df.copy()
    total_len = len(corrupted_df)

    # Calculate proportional split boundaries
    source_boundary = max(1, total_len // 10)
    dup_target_pool = range(source_boundary, total_len)

    # 1. Duplicate Records
    actual_dup_count = min(duplicate_count, len(dup_target_pool))
    dup_source_indices = rng.integers(0, source_boundary, size=actual_dup_count)
    dup_target_indices = rng.choice(list(dup_target_pool), size=actual_dup_count, replace=False)
    for src, tgt in zip(dup_source_indices, dup_target_indices):
        corrupted_df.iloc[tgt] = corrupted_df.iloc[src]

    # 2. Missing Values (across customer_id, payment_method, quantity)
    remaining_indices = list(set(range(total_len)) - set(dup_target_indices))
    actual_missing_count = min(missing_count, len(remaining_indices))
    selected_missing = rng.choice(remaining_indices, size=actual_missing_count, replace=False)

    split = actual_missing_count // 3
    # Customer ID missing
    corrupted_df.loc[selected_missing[:split], "customer_id"] = np.nan
    # Payment Method missing
    corrupted_df.loc[selected_missing[split : 2 * split], "payment_method"] = np.nan
    # Quantity missing
    corrupted_df.loc[selected_missing[2 * split :], "quantity"] = np.nan

    # 3. Invalid Amounts (negative prices, zero totals, negative total)
    available_indices = list(set(remaining_indices) - set(selected_missing))
    actual_amt_count = min(invalid_amount_count, len(available_indices))
    selected_invalid_amt = rng.choice(available_indices, size=actual_amt_count, replace=False)

    amt_split = actual_amt_count // 3
    # Negative unit price
    corrupted_df.loc[selected_invalid_amt[:amt_split], "unit_price"] = -1.0 * corrupted_df.loc[
        selected_invalid_amt[:amt_split], "unit_price"
    ].astype(float)
    # Zero total amount
    corrupted_df.loc[selected_invalid_amt[amt_split : 2 * amt_split], "total_amount"] = 0.00
    # Negative total amount
    corrupted_df.loc[selected_invalid_amt[2 * amt_split :], "total_amount"] = -99.99

    # 4. Malformed Dates
    available_indices = list(set(available_indices) - set(selected_invalid_amt))
    actual_date_count = min(malformed_date_count, len(available_indices))
    selected_dates = rng.choice(available_indices, size=actual_date_count, replace=False)

    date_anomalies = [
        "INVALID_DATE_STRING",
        "2025/31/12 25:99:99",
        "31-02-2025",
        "9999-99-99",
        "NOT_AVAILABLE",
        "2025-13-45",
    ]
    for idx in selected_dates:
        corrupted_df.loc[idx, "order_date"] = str(rng.choice(date_anomalies))

    # Explicitly ensure clean string type for order_date column
    corrupted_df["order_date"] = corrupted_df["order_date"].astype(str)
    return corrupted_df


def generate_and_save_datasets(
    raw_output_path: str | Path = "data/raw/retail_transactions.csv",
    clean_output_path: str | Path = "data/clean/retail_transactions_clean.csv",
    record_count: int = 10_200,
    seed: int = 42,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Generates both raw (with anomalies) and reference clean datasets, saving to disk."""
    raw_path = Path(raw_output_path)
    clean_path = Path(clean_output_path)

    raw_path.parent.mkdir(parents=True, exist_ok=True)
    clean_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"Generating clean baseline dataset with {record_count} records (seed={seed})...")
    clean_df = generate_clean_dataset(GeneratorConfig(total_records=record_count, random_seed=seed))

    logger.info("Injecting controlled data quality issues into raw dataset...")
    raw_df = inject_controlled_anomalies(clean_df, seed=seed)

    clean_df.to_csv(clean_path, index=False)
    logger.info(f"Clean reference dataset saved successfully to: {clean_path}")

    raw_df.to_csv(raw_path, index=False)
    logger.info(f"Raw dataset with anomalies saved successfully to: {raw_path}")

    return raw_df, clean_df


if __name__ == "__main__":
    generate_and_save_datasets()
