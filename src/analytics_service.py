"""Analytics Service for Streamlit Dashboard.

Queries metrics directly from PostgreSQL database (with SQLite fallback)
and loads data quality execution reports from S3 / disk.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional
import pandas as pd

from src.db.connection import DatabaseConnection

logger = logging.getLogger(__name__)


class DashboardAnalyticsService:
    """Provides high-performance analytical aggregations for the Streamlit dashboard."""

    def __init__(self, db_conn: Optional[DatabaseConnection] = None):
        self.db = db_conn or DatabaseConnection(sqlite_db_path="data/warehouse.db")

    def get_date_bounds(self) -> tuple[str, str]:
        """Returns the min and max order timestamp present in fact_orders."""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT MIN(order_timestamp), MAX(order_timestamp) FROM fact_orders;")
            row = cursor.fetchone()
            if row and row[0] and row[1]:
                return str(row[0])[:10], str(row[1])[:10]
            return "2025-01-01", "2025-12-31"

    def get_overview_kpis(
        self, start_date: Optional[str] = None, end_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """Calculates headline KPI metrics: Total Revenue, Total Orders, Total Customers, Cancellation Rate."""
        date_filter = ""
        params = []
        if start_date and end_date:
            date_filter = "WHERE order_timestamp BETWEEN ? AND ?" if self.db.use_sqlite else "WHERE order_timestamp BETWEEN %s AND %s"
            params = [f"{start_date} 00:00:00", f"{end_date} 23:59:59"]

        query = f"""
        SELECT
            COUNT(DISTINCT order_id) AS total_orders,
            COUNT(DISTINCT customer_key) AS total_customers,
            COALESCE(SUM(CASE WHEN order_status != 'Cancelled' THEN total_amount ELSE 0 END), 0) AS total_revenue,
            COALESCE(SUM(CASE WHEN order_status = 'Cancelled' THEN 1 ELSE 0 END), 0) AS cancelled_orders,
            COALESCE(AVG(CASE WHEN order_status != 'Cancelled' THEN total_amount ELSE NULL END), 0) AS avg_order_value
        FROM fact_orders
        {date_filter};
        """

        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()

        total_orders = row[0] or 0
        total_customers = row[1] or 0
        total_revenue = float(row[2] or 0.0)
        cancelled_orders = row[3] or 0
        avg_order_value = float(row[4] or 0.0)

        cancellation_rate = round((cancelled_orders / total_orders * 100), 2) if total_orders > 0 else 0.0

        return {
            "total_orders": total_orders,
            "total_customers": total_customers,
            "total_revenue": round(total_revenue, 2),
            "cancelled_orders": cancelled_orders,
            "cancellation_rate_pct": cancellation_rate,
            "average_order_value": round(avg_order_value, 2),
        }

    def get_monthly_revenue_trends(
        self, start_date: Optional[str] = None, end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """Extracts monthly aggregated revenue and transaction counts."""
        date_filter = ""
        params = []
        if start_date and end_date:
            date_filter = "AND f.order_timestamp BETWEEN ? AND ?" if self.db.use_sqlite else "AND f.order_timestamp BETWEEN %s AND %s"
            params = [f"{start_date} 00:00:00", f"{end_date} 23:59:59"]

        query = f"""
        SELECT
            d.year,
            d.month,
            d.month_name,
            COUNT(DISTINCT f.order_id) AS order_count,
            ROUND(SUM(f.total_amount), 2) AS revenue,
            ROUND(AVG(f.total_amount), 2) AS avg_basket
        FROM fact_orders f
        JOIN dim_date d ON f.date_key = d.date_key
        WHERE f.order_status != 'Cancelled'
        {date_filter}
        GROUP BY d.year, d.month, d.month_name
        ORDER BY d.year, d.month;
        """

        with self.db.get_connection() as conn:
            df = pd.read_sql_query(query, conn, params=params)

        if not df.empty:
            df["period"] = df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2)
        return df

    def get_top_products_and_categories(
        self, start_date: Optional[str] = None, end_date: Optional[str] = None, top_n: int = 10
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Extracts top products by revenue and category volume breakdown."""
        date_filter = ""
        params = []
        if start_date and end_date:
            date_filter = "AND f.order_timestamp BETWEEN ? AND ?" if self.db.use_sqlite else "AND f.order_timestamp BETWEEN %s AND %s"
            params = [f"{start_date} 00:00:00", f"{end_date} 23:59:59"]

        prod_query = f"""
        SELECT
            p.product_id,
            p.product_name,
            p.category,
            SUM(f.quantity) AS units_sold,
            ROUND(SUM(f.total_amount), 2) AS total_revenue
        FROM fact_orders f
        JOIN dim_product p ON f.product_key = p.product_key
        WHERE f.order_status != 'Cancelled'
        {date_filter}
        GROUP BY p.product_id, p.product_name, p.category
        ORDER BY total_revenue DESC
        LIMIT {top_n};
        """

        cat_query = f"""
        SELECT
            p.category,
            COUNT(DISTINCT f.order_id) AS orders_count,
            SUM(f.quantity) AS units_sold,
            ROUND(SUM(f.total_amount), 2) AS total_revenue
        FROM fact_orders f
        JOIN dim_product p ON f.product_key = p.product_key
        WHERE f.order_status != 'Cancelled'
        {date_filter}
        GROUP BY p.category
        ORDER BY total_revenue DESC;
        """

        with self.db.get_connection() as conn:
            top_products = pd.read_sql_query(prod_query, conn, params=params)
            category_performance = pd.read_sql_query(cat_query, conn, params=params)

        return top_products, category_performance

    def get_regional_performance(
        self, start_date: Optional[str] = None, end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """Extracts territorial performance: orders, unique customers, and regional revenue."""
        date_filter = ""
        params = []
        if start_date and end_date:
            date_filter = "AND f.order_timestamp BETWEEN ? AND ?" if self.db.use_sqlite else "AND f.order_timestamp BETWEEN %s AND %s"
            params = [f"{start_date} 00:00:00", f"{end_date} 23:59:59"]

        query = f"""
        SELECT
            c.region,
            COUNT(DISTINCT f.order_id) AS total_orders,
            COUNT(DISTINCT f.customer_key) AS unique_customers,
            ROUND(SUM(CASE WHEN f.order_status != 'Cancelled' THEN f.total_amount ELSE 0 END), 2) AS total_revenue,
            COUNT(CASE WHEN f.order_status = 'Cancelled' THEN 1 END) AS cancellations
        FROM fact_orders f
        JOIN dim_customer c ON f.customer_key = c.customer_key
        WHERE 1=1
        {date_filter}
        GROUP BY c.region
        ORDER BY total_revenue DESC;
        """

        with self.db.get_connection() as conn:
            df = pd.read_sql_query(query, conn, params=params)

        if not df.empty:
            total_rev = df["total_revenue"].sum()
            df["revenue_share_pct"] = round((df["total_revenue"] / total_rev * 100), 2) if total_rev > 0 else 0
            df["cancellation_rate_pct"] = round((df["cancellations"] / df["total_orders"] * 100), 2)
        return df

    def get_quality_reports_history(self, reports_dir: str = "reports") -> List[Dict[str, Any]]:
        """Scans and parses historical JSON data quality execution reports."""
        rep_path = Path(reports_dir)
        reports = []
        if not rep_path.exists():
            return reports

        for json_file in sorted(rep_path.glob("*.json"), reverse=True):
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    data["file_name"] = json_file.name
                    reports.append(data)
            except Exception as e:
                logger.warning(f"Error loading {json_file}: {e}")
        return reports
