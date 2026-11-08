"""Unit and integration tests for Phase 2: S3 Client, Lambda Handler, and Anti-Recursion."""

import json
import os
import shutil
import unittest
from pathlib import Path

import pandas as pd

from src.s3_client import S3DataLakeManager
from src.lambda_function import (
    lambda_handler,
    transform_and_validate_dataframe,
    ALLOWED_TRIGGER_PREFIX,
    FORBIDDEN_TRIGGER_PREFIXES,
)


class TestPhase2ETL(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.test_bucket = "unit-test-data-lake"
        cls.mock_dir = Path(".s3_emulator") / cls.test_bucket
        if cls.mock_dir.exists():
            shutil.rmtree(cls.mock_dir)
        cls.s3_mgr = S3DataLakeManager(bucket_name=cls.test_bucket, local_mock_dir=cls.mock_dir)

    def test_01_lake_prefix_initialization(self):
        """Verify all 5 lake prefixes are correctly initialized."""
        prefixes = self.s3_mgr.initialize_data_lake_prefixes()
        expected = ["raw/", "curated/", "warehouse/", "quarantine/", "reports/"]
        for p in expected:
            self.assertIn(p, prefixes)

    def test_02_put_and_get_s3_object(self):
        """Verify uploading and downloading objects from S3 manager."""
        payload = b"sample,test,csv\n1,2,3"
        key = "raw/sample_test.csv"
        self.s3_mgr.put_object(key=key, body=payload, content_type="text/csv")
        retrieved = self.s3_mgr.get_object(key)
        self.assertEqual(retrieved, payload)

    def test_03_anti_recursion_filter(self):
        """Verify Lambda blocks triggers outside raw/ or from downstream prefixes."""
        for forbidden in FORBIDDEN_TRIGGER_PREFIXES:
            event = {
                "Records": [
                    {
                        "eventName": "ObjectCreated:Put",
                        "s3": {
                            "bucket": {"name": self.test_bucket},
                            "object": {"key": f"{forbidden}test_file.csv"},
                        },
                    }
                ]
            }
            res = lambda_handler(event)
            body = json.loads(res["body"])
            self.assertEqual(body["processed_batches"], 0)

    def test_04_transform_and_validate_dataframe(self):
        """Verify transformation logic splits valid records to curated and anomalies to quarantine."""
        df_raw = pd.DataFrame(
            {
                "order_id": ["ORD-1", "ORD-2", "ORD-3", "ORD-4", "ORD-4"],
                "customer_id": ["C-1", None, "C-3", "C-4", "C-4"],  # ORD-2 missing customer
                "product_id": ["P-1", "P-2", "P-3", "P-4", "P-4"],
                "product_name": ["Prod 1", "Prod 2", "Prod 3", "Prod 4", "Prod 4"],
                "category": ["Electronics", "Books", "Home", "Sports", "Sports"],
                "quantity": [2, 1, -5, 3, 3],  # ORD-3 invalid quantity
                "unit_price": [10.0, 20.0, 15.0, 25.0, 25.0],
                "total_amount": [20.0, 20.0, -75.0, 75.0, 75.0],
                "order_date": [
                    "2025-01-01 10:00:00",
                    "2025-01-02 11:00:00",
                    "2025-01-03 12:00:00",
                    "2025-01-04 13:00:00",
                    "2025-01-04 13:00:00",
                ],
                "region": ["North America", "Europe", "Asia", "Latin America", "Latin America"],
                "payment_method": ["Credit Card", "PayPal", "Debit", "Apple Pay", "Apple Pay"],
                "order_status": ["Delivered", "Delivered", "Delivered", "Delivered", "Delivered"],
            }
        )

        curated, quarantine, metrics = transform_and_validate_dataframe(df_raw)

        # ORD-1 is fully valid, ORD-4 (first instance) is valid -> 2 curated
        # ORD-2 has missing customer, ORD-3 has invalid quantity, ORD-4 (second instance) is duplicate order_id
        self.assertEqual(len(curated), 2)
        self.assertEqual(len(quarantine), 3)
        self.assertEqual(metrics["total_records_processed"], 5)


if __name__ == "__main__":
    unittest.main()
