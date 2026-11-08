"""Unit and integration tests for Phase 3:
1. Individual tests for each of the 4 quality rule modules.
2. Engine-level testing (preservation of rejected records, rejection reasons).
3. Warehouse loader testing (FK resolution, zero invalid records in warehouse, upsert idempotency).
"""

import os
import shutil
import unittest
from pathlib import Path

import pandas as pd

from src.quality.check_duplicates import DuplicateCheck
from src.quality.check_missing import MissingFieldsCheck
from src.quality.check_amounts import AmountCheck
from src.quality.check_dates import DateCheck
from src.quality.engine import DataQualityEngine
from src.db.connection import DatabaseConnection
from src.db.loader import WarehouseLoader


class TestPhase3QualityAndWarehouse(unittest.TestCase):
    def setUp(self):
        self.sample_df = pd.DataFrame(
            {
                "order_id": ["ORD-1", "ORD-2", "ORD-3", "ORD-4", "ORD-1"],  # ORD-1 is duplicate
                "customer_id": ["CUST-1", None, "CUST-3", "CUST-4", "CUST-1"],  # ORD-2 missing cust
                "product_id": ["P-1", "P-2", "P-3", "P-4", "P-1"],
                "product_name": ["Prod 1", "Prod 2", "Prod 3", "Prod 4", "Prod 1"],
                "category": ["Electronics", "Books", "Home", "Sports", "Electronics"],
                "quantity": [1, 2, -5, 3, 1],  # ORD-3 invalid quantity
                "unit_price": [10.0, 20.0, 15.0, 25.0, 10.0],
                "total_amount": [10.0, 40.0, -75.0, 75.0, 10.0],  # ORD-3 invalid total
                "order_date": [
                    "2025-01-01 10:00:00",
                    "2025-01-02 11:00:00",
                    "2025-01-03 12:00:00",
                    "INVALID_DATE_STRING",  # ORD-4 malformed date
                    "2025-01-01 10:00:00",
                ],
                "region": ["North America", "Europe", "Asia", "Latin America", "North America"],
                "payment_method": ["Credit Card", "PayPal", "Debit", "Apple Pay", "Credit Card"],
                "order_status": ["Delivered", "Delivered", "Delivered", "Delivered", "Delivered"],
            }
        )

    def test_01_check_duplicates_module(self):
        rule = DuplicateCheck(key_column="order_id")
        failures = rule.evaluate(self.sample_df)
        # Row 4 is the duplicate of ORD-1
        self.assertFalse(failures[0])
        self.assertTrue(any("DUPLICATE_KEY" in r for r in failures[4]))

    def test_02_check_missing_fields_module(self):
        rule = MissingFieldsCheck()
        failures = rule.evaluate(self.sample_df)
        # Row 1 (ORD-2) has missing customer_id
        self.assertTrue(any("MISSING_REQUIRED_FIELD: 'customer_id'" in r for r in failures[1]))
        self.assertFalse(failures[0])

    def test_03_check_amounts_module(self):
        rule = AmountCheck()
        failures = rule.evaluate(self.sample_df)
        # Row 2 (ORD-3) has negative quantity and total_amount
        self.assertTrue(any("INVALID_QUANTITY" in r for r in failures[2]))
        self.assertTrue(any("INVALID_TOTAL_AMOUNT" in r for r in failures[2]))
        self.assertFalse(failures[0])

    def test_04_check_dates_module(self):
        rule = DateCheck(date_column="order_date")
        failures = rule.evaluate(self.sample_df)
        # Row 3 (ORD-4) has invalid date string
        self.assertTrue(any("MALFORMED_DATE" in r for r in failures[3]))
        self.assertFalse(failures[0])

    def test_05_engine_partitions_and_preserves_rejected_records(self):
        engine = DataQualityEngine()
        curated_df, quarantine_df, report = engine.validate_and_partition(self.sample_df, batch_id="unit_b1")

        # In sample_df: only ORD-1 (the first row) has NO errors!
        self.assertEqual(len(curated_df), 1)
        self.assertEqual(curated_df.iloc[0]["order_id"], "ORD-1")

        # 4 records failed (ORD-2, ORD-3, ORD-4, and the 2nd ORD-1)
        self.assertEqual(len(quarantine_df), 4)

        # Verify rejection reasons and preserved columns
        self.assertIn("rejection_reasons", quarantine_df.columns)
        self.assertIn("customer_id", quarantine_df.columns)
        self.assertIn("order_date", quarantine_df.columns)
        self.assertEqual(report["record_counts"]["total_input_records"], 5)
        self.assertEqual(report["record_counts"]["valid_curated_records"], 1)
        self.assertEqual(report["record_counts"]["invalid_quarantined_records"], 4)

    def test_06_warehouse_loading_and_idempotency(self):
        test_db = "data/test_warehouse.db"
        if os.path.exists(test_db):
            os.remove(test_db)

        db_conn = DatabaseConnection(use_sqlite=True, sqlite_db_path=test_db)
        loader = WarehouseLoader(db_conn)
        loader.initialize_schema()

        # Load clean sample
        clean_df = pd.DataFrame(
            {
                "order_id": ["ORD-101", "ORD-102"],
                "customer_id": ["C-1", "C-2"],
                "product_id": ["P-1", "P-2"],
                "product_name": ["Prod 1", "Prod 2"],
                "category": ["Electronics", "Books"],
                "quantity": [2, 1],
                "unit_price": [100.0, 50.0],
                "total_amount": [200.0, 50.0],
                "order_date": ["2025-05-01 10:00:00", "2025-05-02 11:00:00"],
                "region": ["North America", "Europe"],
                "payment_method": ["Credit Card", "PayPal"],
                "order_status": ["Delivered", "Delivered"],
            }
        )

        # Initial Load
        stats = loader.load_curated_data(clean_df, batch_id="batch_initial")
        self.assertEqual(stats["fact_orders"], 2)

        # Upsert Idempotent Re-load
        stats_rerun = loader.load_curated_data(clean_df, batch_id="batch_rerun")
        self.assertEqual(stats_rerun["fact_orders"], 2)

        # Assert no duplicates were created
        with db_conn.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM fact_orders;")
            total_facts = cursor.fetchone()[0]
            self.assertEqual(total_facts, 2)

            cursor.execute("SELECT COUNT(*) FROM dim_customer;")
            total_cust = cursor.fetchone()[0]
            self.assertEqual(total_cust, 2)


if __name__ == "__main__":
    unittest.main()
