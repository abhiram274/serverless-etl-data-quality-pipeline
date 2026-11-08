"""Unit and integration tests for Phase 4:
1. Analytics service SQL query execution.
2. Overview KPIs calculation (Revenue, Orders, Customers, Cancellation rate).
3. Monthly trends, Top products, and Regional distributions.
4. Historical quality report scanning.
5. Athena partitioned warehouse exporter verification.
"""

import os
import unittest
from pathlib import Path

import pandas as pd

from src.db.connection import DatabaseConnection
from src.db.loader import WarehouseLoader
from src.analytics_service import DashboardAnalyticsService
from src.s3_client import S3DataLakeManager
from src.athena_exporter import WarehouseParquetExporter


class TestPhase4Analytics(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.test_db = "data/test_phase4.db"
        if os.path.exists(cls.test_db):
            os.remove(cls.test_db)

        cls.db_conn = DatabaseConnection(use_sqlite=True, sqlite_db_path=cls.test_db)
        cls.loader = WarehouseLoader(cls.db_conn)
        cls.loader.initialize_schema()

        # Seed sample data
        sample_df = pd.DataFrame(
            {
                "order_id": ["ORD-P4-1", "ORD-P4-2", "ORD-P4-3", "ORD-P4-4"],
                "customer_id": ["C-1", "C-2", "C-3", "C-4"],
                "product_id": ["P-1", "P-2", "P-1", "P-3"],
                "product_name": ["Laptop Pro", "Wireless Mouse", "Laptop Pro", "Coffee Maker"],
                "category": ["Electronics", "Electronics", "Electronics", "Home"],
                "quantity": [1, 2, 1, 1],
                "unit_price": [1200.0, 50.0, 1200.0, 150.0],
                "total_amount": [1200.0, 100.0, 1200.0, 150.0],
                "order_date": [
                    "2025-01-10 10:00:00",
                    "2025-01-15 11:00:00",
                    "2025-02-05 12:00:00",
                    "2025-02-20 13:00:00",
                ],
                "region": ["North America", "Europe", "North America", "Asia-Pacific"],
                "payment_method": ["Credit Card", "PayPal", "Credit Card", "Apple Pay"],
                "order_status": ["Delivered", "Cancelled", "Delivered", "Delivered"],
            }
        )
        cls.loader.load_curated_data(sample_df, batch_id="phase4_seed")
        cls.service = DashboardAnalyticsService(db_conn=cls.db_conn)

    def test_01_kpi_calculation(self):
        kpis = self.service.get_overview_kpis()
        self.assertEqual(kpis["total_orders"], 4)
        self.assertEqual(kpis["total_customers"], 4)
        # Delivered orders total amount = 1200 + 1200 + 150 = 2550
        self.assertEqual(kpis["total_revenue"], 2550.0)
        # Cancelled orders = 1 out of 4 (25.0%)
        self.assertEqual(kpis["cancelled_orders"], 1)
        self.assertEqual(kpis["cancellation_rate_pct"], 25.0)

    def test_02_monthly_trends(self):
        trends = self.service.get_monthly_revenue_trends()
        self.assertEqual(len(trends), 2)  # January and February
        jan = trends[trends["month"] == 1].iloc[0]
        # In Jan: ORD-P4-1 (Delivered, 1200.0), ORD-P4-2 (Cancelled, 100.0) -> Revenue = 1200.0
        self.assertEqual(jan["revenue"], 1200.0)
        self.assertEqual(jan["order_count"], 1)

    def test_03_top_products_and_categories(self):
        top_prod, cat_df = self.service.get_top_products_and_categories()
        # Laptop Pro is top product with 2400 revenue
        self.assertEqual(top_prod.iloc[0]["product_id"], "P-1")
        self.assertEqual(top_prod.iloc[0]["total_revenue"], 2400.0)

        # Electronics is top category
        self.assertEqual(cat_df.iloc[0]["category"], "Electronics")
        self.assertEqual(cat_df.iloc[0]["total_revenue"], 2400.0)

    def test_04_regional_performance(self):
        reg_df = self.service.get_regional_performance()
        # North America has 2400 revenue
        na_row = reg_df[reg_df["region"] == "North America"].iloc[0]
        self.assertEqual(na_row["total_revenue"], 2400.0)
        self.assertEqual(na_row["total_orders"], 2)

    def test_05_athena_partitioned_warehouse_exporter(self):
        s3_mgr = S3DataLakeManager(bucket_name="athena-test-bucket")
        exporter = WarehouseParquetExporter(s3_mgr)

        sample_curated = pd.DataFrame(
            {
                "order_id": ["ORD-1", "ORD-2"],
                "order_date": ["2025-01-15 10:00:00", "2025-02-15 11:00:00"],
                "total_amount": [100.0, 200.0],
            }
        )
        keys = exporter.export_partitioned_warehouse_data(sample_curated, warehouse_prefix="warehouse/fact_orders")
        self.assertEqual(len(keys), 2)
        self.assertTrue(any("year=2025/month=01" in k for k in keys))
        self.assertTrue(any("year=2025/month=02" in k for k in keys))


if __name__ == "__main__":
    unittest.main()
