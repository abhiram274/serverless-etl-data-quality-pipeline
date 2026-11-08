"""Tests for Phase 1 data generation, quality checks, and reporting (unittest & pytest compatible)."""

import unittest
from pathlib import Path

import pandas as pd

from src.generator import (
    generate_clean_dataset,
    inject_controlled_anomalies,
    GeneratorConfig,
)
from src.explorer import profile_dataset


EXPECTED_COLUMNS = [
    "order_id",
    "customer_id",
    "product_id",
    "product_name",
    "category",
    "quantity",
    "unit_price",
    "total_amount",
    "order_date",
    "region",
    "payment_method",
    "order_status",
]


class TestPhase1Pipeline(unittest.TestCase):
    def test_clean_dataset_generation_exact_count_and_columns(self):
        """Verify that the clean baseline dataset generates exactly 10,200 rows and matches schema."""
        config = GeneratorConfig(total_records=10_200, random_seed=42)
        df = generate_clean_dataset(config)

        self.assertEqual(len(df), 10_200)
        self.assertEqual(list(df.columns), EXPECTED_COLUMNS)

        # Ensure no missing values in clean reference dataset
        self.assertEqual(df.isna().sum().sum(), 0)

        # Ensure no duplicate order IDs in clean baseline
        self.assertEqual(df["order_id"].duplicated().sum(), 0)

        # Ensure all total amounts = quantity * unit_price
        expected_totals = (df["quantity"] * df["unit_price"]).round(2)
        diff = (df["total_amount"] - expected_totals).abs()
        self.assertEqual((diff > 0.01).sum(), 0)

    def test_anomaly_injection_preserves_count(self):
        """Verify corrupted dataset maintains exact record count while introducing specific flaws."""
        config = GeneratorConfig(total_records=10_200, random_seed=42)
        clean_df = generate_clean_dataset(config)
        raw_df = inject_controlled_anomalies(
            clean_df,
            seed=42,
            duplicate_count=150,
            missing_count=300,
            invalid_amount_count=120,
            malformed_date_count=180,
        )

        # 1. Total count must remain exactly 10,200
        self.assertEqual(len(raw_df), 10_200)

        # 2. Duplicate records injected
        self.assertGreaterEqual(raw_df.duplicated().sum(), 100)

        # 3. Missing values present
        self.assertGreaterEqual(raw_df.isna().sum().sum(), 300)

        # 4. Invalid amounts present (<= 0)
        invalid_amounts = (pd.to_numeric(raw_df["total_amount"], errors="coerce") <= 0).sum()
        self.assertGreater(invalid_amounts, 0)

        # 5. Malformed dates present (unparseable)
        parsed = pd.to_datetime(raw_df["order_date"].tolist(), errors="coerce")
        malformed_dates = int(pd.Series(parsed).isna().sum())
        self.assertGreaterEqual(malformed_dates, 180)

    def test_profiler_detects_all_issues(self):
        """Verify profiler accurately catches and counts the anomalies."""
        config = GeneratorConfig(total_records=1000, random_seed=42)
        clean_df = generate_clean_dataset(config)
        raw_df = inject_controlled_anomalies(
            clean_df,
            seed=42,
            duplicate_count=20,
            missing_count=30,
            invalid_amount_count=15,
            malformed_date_count=20,
        )

        raw_profile = profile_dataset(raw_df, dataset_name="test_raw")
        clean_profile = profile_dataset(clean_df, dataset_name="test_clean")

        # Clean profile assertions
        self.assertEqual(clean_profile["data_quality_issues"]["duplicate_records_count"], 0)
        self.assertEqual(clean_profile["data_quality_issues"]["total_missing_values"], 0)
        self.assertEqual(clean_profile["data_quality_issues"]["invalid_total_amounts_lte_zero"], 0)
        self.assertEqual(clean_profile["data_quality_issues"]["malformed_or_unparseable_dates"], 0)

        # Raw profile assertions
        self.assertGreaterEqual(raw_profile["data_quality_issues"]["duplicate_records_count"], 15)
        self.assertGreaterEqual(raw_profile["data_quality_issues"]["total_missing_values"], 30)
        self.assertGreater(raw_profile["data_quality_issues"]["invalid_total_amounts_lte_zero"], 0)
        self.assertGreaterEqual(raw_profile["data_quality_issues"]["malformed_or_unparseable_dates"], 20)


if __name__ == "__main__":
    unittest.main()
