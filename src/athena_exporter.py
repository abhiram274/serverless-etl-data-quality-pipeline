"""Warehouse Exporter: Partitions curated data into S3 warehouse/ directory.

Creates year=YYYY/month=MM Hive-compatible partitioning suitable for
AWS Glue Data Catalog crawling and Amazon Athena serverless queries.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List
import pandas as pd

from src.s3_client import S3DataLakeManager

logger = logging.getLogger(__name__)


class WarehouseParquetExporter:
    """Exports and partitions analytical datasets into S3 warehouse prefix."""

    def __init__(self, s3_mgr: S3DataLakeManager):
        self.s3_mgr = s3_mgr

    def export_partitioned_warehouse_data(
        self, curated_df: pd.DataFrame, warehouse_prefix: str = "warehouse/fact_orders"
    ) -> List[str]:
        """Partitions curated dataset by year and month into Hive partition layouts."""
        if curated_df.empty:
            return []

        df = curated_df.copy()
        dt_series = pd.to_datetime(df["order_date"])
        df["year"] = dt_series.dt.strftime("%Y")
        df["month"] = dt_series.dt.strftime("%m")

        exported_keys = []
        for (year, month), partition_df in df.groupby(["year", "month"]):
            partition_key = f"{warehouse_prefix}/year={year}/month={month}/orders_{year}_{month}.csv"

            # Check if parquet is supported in the runtime environment
            try:
                import pyarrow  # noqa: F401
                parquet_bytes = partition_df.to_parquet(index=False)
                parquet_key = f"{warehouse_prefix}/year={year}/month={month}/orders_{year}_{month}.parquet"
                self.s3_mgr.put_object(parquet_key, parquet_bytes, content_type="application/octet-stream")
                exported_keys.append(parquet_key)
            except Exception:
                # Fallback to standard partitioned CSV for Athena external table
                csv_bytes = partition_df.to_csv(index=False).encode("utf-8")
                self.s3_mgr.put_object(partition_key, csv_bytes, content_type="text/csv")
                exported_keys.append(partition_key)

        logger.info(f"Exported {len(exported_keys)} partition files to S3 warehouse.")
        return exported_keys
