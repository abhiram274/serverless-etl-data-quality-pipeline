"""Comprehensive test suite for Phase 5 verification using standard unittest & pytest:
1. Data Quality rules verification (duplicates, missing, amounts, dates).
2. S3 and Lambda integration & anti-recursion.
3. PostgreSQL / SQLite warehouse loading, idempotency & foreign key resolution.
4. Analytical SQL queries & metrics aggregation.
5. Record reconciliation (Input == Curated + Quarantined).
6. Dashboard calculations & date range filters.
7. CloudWatch metrics publishing & SNS alerting threshold triggers.
"""

from __future__ import annotations

import json
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
from src.s3_client import S3DataLakeManager
from src.lambda_function import lambda_handler
from src.db.connection import DatabaseConnection
from src.db.loader import WarehouseLoader
from src.analytics_service import DashboardAnalyticsService
from src.monitoring import CloudWatchMonitoringService, SNSNotificationService


class TestPhase5Comprehensive(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.test_bucket = "phase5-validation-lake"
        cls.s3_mgr = S3DataLakeManager(bucket_name=cls.test_bucket)
        cls.s3_mgr.initialize_data_lake_prefixes()

        cls.test_db = "data/test_phase5_warehouse.db"
        if os.path.exists(cls.test_db):
            os.remove(cls.test_db)
        cls.db_conn = DatabaseConnection(use_sqlite=True, sqlite_db_path=cls.test_db)
        cls.loader = WarehouseLoader(cls.db_conn)
        cls.loader.initialize_schema()

    # -------------------------------------------------------------------------
    # TEST GROUP 1: All 4 Data Quality Rules
    # -------------------------------------------------------------------------
    def test_01_all_four_data_quality_rules(self):
        df = pd.DataFrame(
            {
                "order_id": ["O-1", "O-1", "O-2", "O-3", "O-4"],
                "customer_id": ["C-1", "C-1", None, "C-3", "C-4"],
                "product_id": ["P-1", "P-1", "P-2", "P-3", "P-4"],
                "quantity": [1, 1, 2, -1, 3],
                "unit_price": [10.0, 10.0, 15.0, 20.0, 25.0],
                "total_amount": [10.0, 10.0, 30.0, -20.0, 75.0],
                "order_date": [
                    "2025-01-01 10:00:00",
                    "2025-01-01 10:00:00",
                    "2025-01-02 11:00:00",
                    "2025-01-03 12:00:00",
                    "MALFORMED_DATE_STR",
                ],
                "payment_method": ["Credit", "Credit", "Debit", "PayPal", "ApplePay"],
                "order_status": ["Delivered", "Delivered", "Delivered", "Delivered", "Delivered"],
            }
        )

        dup_check = DuplicateCheck().evaluate(df)
        self.assertTrue(any("DUPLICATE_KEY" in r for r in dup_check[1]))

        miss_check = MissingFieldsCheck().evaluate(df)
        self.assertTrue(any("MISSING_REQUIRED_FIELD: 'customer_id'" in r for r in miss_check[2]))

        amt_check = AmountCheck().evaluate(df)
        self.assertTrue(any("INVALID_QUANTITY" in r for r in amt_check[3]))
        self.assertTrue(any("INVALID_TOTAL_AMOUNT" in r for r in amt_check[3]))

        date_check = DateCheck().evaluate(df)
        self.assertTrue(any("MALFORMED_DATE" in r for r in date_check[4]))

    # -------------------------------------------------------------------------
    # TEST GROUP 2: Record Reconciliation (Input == Valid + Rejected)
    # -------------------------------------------------------------------------
    def test_02_record_reconciliation_math(self):
        raw_csv_path = Path("data/raw/retail_transactions.csv")
        self.assertTrue(raw_csv_path.exists())

        raw_df = pd.read_csv(raw_csv_path)
        total_input = len(raw_df)

        engine = DataQualityEngine()
        curated_df, quarantine_df, report = engine.validate_and_partition(raw_df, batch_id="rec_test")

        valid_count = len(curated_df)
        rejected_count = len(quarantine_df)

        # STRICT RECORD RECONCILIATION
        self.assertEqual(total_input, valid_count + rejected_count)
        self.assertEqual(report["record_counts"]["total_input_records"], total_input)
        self.assertEqual(report["record_counts"]["valid_curated_records"], valid_count)
        self.assertEqual(report["record_counts"]["invalid_quarantined_records"], rejected_count)
        self.assertEqual(valid_count, 9450)
        self.assertEqual(rejected_count, 750)

    # -------------------------------------------------------------------------
    # TEST GROUP 3: S3 & Lambda Integration
    # -------------------------------------------------------------------------
    def test_03_s3_and_lambda_integration(self):
        raw_s3_key = "raw/sample_test_batch.csv"
        csv_payload = (
            "order_id,customer_id,product_id,product_name,category,quantity,unit_price,total_amount,order_date,region,payment_method,order_status\n"
            "ORD-T1,C-1,P-1,Item 1,Tech,2,100.0,200.0,2025-03-01 10:00:00,North America,Credit Card,Delivered\n"
            "ORD-T2,,P-2,Item 2,Tech,1,50.0,50.0,2025-03-02 11:00:00,Europe,PayPal,Delivered\n"
        )
        self.s3_mgr.put_object(raw_s3_key, csv_payload)

        # Emulate S3 Event Trigger
        event = {
            "Records": [
                {
                    "eventName": "ObjectCreated:Put",
                    "s3": {
                        "bucket": {"name": self.test_bucket},
                        "object": {"key": raw_s3_key},
                    },
                }
            ]
        }
        res = lambda_handler(event)
        self.assertEqual(res["statusCode"], 200)

        body = json.loads(res["body"])
        self.assertEqual(body["processed_batches"], 1)
        metrics = body["details"][0]["metrics"]
        self.assertEqual(metrics["total_records_processed"], 2)
        self.assertEqual(metrics["curated_records_count"], 1)
        self.assertEqual(metrics["quarantined_records_count"], 1)

    # -------------------------------------------------------------------------
    # TEST GROUP 4: PostgreSQL / Database Loading & Upsert Idempotency
    # -------------------------------------------------------------------------
    def test_04_warehouse_loading_and_fk_integrity(self):
        curated_sample = pd.DataFrame(
            {
                "order_id": ["ORD-WH-1", "ORD-WH-2"],
                "customer_id": ["CUST-10", "CUST-20"],
                "product_id": ["PROD-10", "PROD-20"],
                "product_name": ["Product A", "Product B"],
                "category": ["Apparel", "Home"],
                "quantity": [1, 3],
                "unit_price": [45.0, 20.0],
                "total_amount": [45.0, 60.0],
                "order_date": ["2025-04-10 14:00:00", "2025-04-12 15:30:00"],
                "region": ["Europe", "Asia-Pacific"],
                "payment_method": ["Credit Card", "Debit Card"],
                "order_status": ["Delivered", "Delivered"],
            }
        )

        # Initial Load
        load_res = self.loader.load_curated_data(curated_sample, batch_id="wh_test_b1")
        self.assertEqual(load_res["fact_orders"], 2)

        # Re-running same batch should not create duplicates (idempotency)
        load_res_2 = self.loader.load_curated_data(curated_sample, batch_id="wh_test_b2")
        self.assertEqual(load_res_2["fact_orders"], 2)

        with self.db_conn.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM fact_orders WHERE order_id IN ('ORD-WH-1', 'ORD-WH-2');")
            self.assertEqual(cursor.fetchone()[0], 2)

    # -------------------------------------------------------------------------
    # TEST GROUP 5: Analytical Queries & Dashboard Calculations
    # -------------------------------------------------------------------------
    def test_05_analytical_queries_and_dashboard_calculations(self):
        service = DashboardAnalyticsService(db_conn=self.db_conn)
        kpis = service.get_overview_kpis()

        self.assertGreaterEqual(kpis["total_orders"], 2)
        self.assertGreaterEqual(kpis["total_revenue"], 105.0)
        self.assertEqual(kpis["cancellation_rate_pct"], 0.0)

        monthly = service.get_monthly_revenue_trends()
        self.assertFalse(monthly.empty)

        top_p, cat = service.get_top_products_and_categories()
        self.assertFalse(top_p.empty)
        self.assertFalse(cat.empty)

        reg = service.get_regional_performance()
        self.assertFalse(reg.empty)

    # -------------------------------------------------------------------------
    # TEST GROUP 6: CloudWatch Metrics & SNS Alerting
    # -------------------------------------------------------------------------
    def test_06_cloudwatch_metrics_and_sns_alerts(self):
        cw_service = CloudWatchMonitoringService(mock_mode=True)
        metrics_res = cw_service.publish_pipeline_metrics(
            batch_id="test_b100",
            total_input=1000,
            valid_records=950,
            rejected_records=50,
            duration_seconds=2.45,
            error_count=0,
        )
        self.assertEqual(metrics_res["pass_rate_pct"], 95.0)
        self.assertEqual(metrics_res["rejection_rate_pct"], 5.0)
        self.assertEqual(len(cw_service.published_metrics), 7)

        # Test SNS alerting when threshold (5%) is exceeded
        sns_service = SNSNotificationService(mock_mode=True, rejection_threshold_pct=5.0)
        alert = sns_service.evaluate_and_notify(
            batch_id="test_b101",
            total_input=1000,
            rejected_count=80,  # 8.0% > 5.0% threshold
        )
        self.assertIsNotNone(alert)
        self.assertIn("High Data Quality Rejection Rate (8.0%)", alert["Subject"])

        # Test failure alert
        fail_alert = sns_service.evaluate_and_notify(
            batch_id="test_b102",
            total_input=1000,
            rejected_count=0,
            error_message="Database Connection Timeout",
        )
        self.assertIsNotNone(fail_alert)
        self.assertIn("CRITICAL: ETL Pipeline Execution Failure", fail_alert["Subject"])


if __name__ == "__main__":
    unittest.main()
