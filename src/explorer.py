"""Exploration and profiling module for retail transaction datasets.

Calculates descriptive metrics, checks integrity rules, detects anomalies,
and exports comprehensive JSON reports.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def profile_dataset(df: pd.DataFrame, dataset_name: str = "dataset") -> Dict[str, Any]:
    """Computes comprehensive data profiling and quality metrics on a DataFrame."""
    total_rows = len(df)
    total_columns = len(df.columns)

    # 1. Duplicate records analysis
    duplicate_full_rows = int(df.duplicated().sum())
    duplicate_order_ids = int(df["order_id"].duplicated().sum()) if "order_id" in df.columns else 0

    # 2. Missing values per column
    missing_by_column = {col: int(df[col].isna().sum()) for col in df.columns}
    total_missing_values = int(sum(missing_by_column.values()))

    # 3. Numeric profiling (quantity, unit_price, total_amount)
    numeric_stats: Dict[str, Any] = {}
    for col in ["quantity", "unit_price", "total_amount"]:
        if col in df.columns:
            series = pd.to_numeric(df[col], errors="coerce").dropna()
            numeric_stats[col] = {
                "count_valid": int(len(series)),
                "min": float(series.min()) if not series.empty else None,
                "mean": round(float(series.mean()), 2) if not series.empty else None,
                "median": round(float(series.median()), 2) if not series.empty else None,
                "max": float(series.max()) if not series.empty else None,
                "std": round(float(series.std()), 2) if not series.empty else None,
            }

    # 4. Data Quality Violations
    # Check invalid negative or zero amounts
    invalid_unit_prices = int((pd.to_numeric(df["unit_price"], errors="coerce") <= 0).sum())
    invalid_total_amounts = int((pd.to_numeric(df["total_amount"], errors="coerce") <= 0).sum())

    # Check date parsing anomalies
    date_series = df["order_date"].astype(str)
    parsed_dates = pd.to_datetime(date_series.tolist(), errors="coerce")
    malformed_dates = int(pd.Series(parsed_dates).isna().sum())

    # Discrepancy between quantity * unit_price and total_amount
    qty = pd.to_numeric(df["quantity"], errors="coerce")
    unit_p = pd.to_numeric(df["unit_price"], errors="coerce")
    tot_amt = pd.to_numeric(df["total_amount"], errors="coerce")
    calc_expected = qty * unit_p
    mismatched_totals = int(((tot_amt - calc_expected).abs() > 0.05).sum())

    # 5. Categorical distributions
    category_breakdown = df["category"].value_counts(dropna=False).to_dict()
    region_breakdown = df["region"].value_counts(dropna=False).to_dict()
    payment_breakdown = df["payment_method"].value_counts(dropna=False).to_dict()
    status_breakdown = df["order_status"].value_counts(dropna=False).to_dict()

    report = {
        "dataset_name": dataset_name,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "record_summary": {
            "total_records": total_rows,
            "total_columns": total_columns,
            "columns": list(df.columns),
        },
        "data_quality_issues": {
            "duplicate_records_count": duplicate_full_rows,
            "duplicate_order_ids": duplicate_order_ids,
            "missing_values_by_column": missing_by_column,
            "total_missing_values": total_missing_values,
            "invalid_unit_prices_lte_zero": invalid_unit_prices,
            "invalid_total_amounts_lte_zero": invalid_total_amounts,
            "malformed_or_unparseable_dates": malformed_dates,
            "amount_discrepancy_count": mismatched_totals,
        },
        "numeric_metrics": numeric_stats,
        "categorical_distributions": {
            "category": {str(k): int(v) for k, v in category_breakdown.items()},
            "region": {str(k): int(v) for k, v in region_breakdown.items()},
            "payment_method": {str(k): int(v) for k, v in payment_breakdown.items()},
            "order_status": {str(k): int(v) for k, v in status_breakdown.items()},
        },
    }
    return report


def generate_statistics_report(
    raw_csv_path: str | Path = "data/raw/retail_transactions.csv",
    clean_csv_path: str | Path = "data/clean/retail_transactions_clean.csv",
    report_output_path: str | Path = "reports/statistics_report.json",
) -> Dict[str, Any]:
    """Loads datasets, profiles both raw and clean sets, and saves the comparison JSON report."""
    raw_path = Path(raw_csv_path)
    clean_path = Path(clean_csv_path)
    out_path = Path(report_output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"Loading raw dataset from {raw_path}...")
    raw_df = pd.read_csv(raw_path)

    logger.info(f"Loading clean reference dataset from {clean_path}...")
    clean_df = pd.read_csv(clean_path)

    raw_profile = profile_dataset(raw_df, dataset_name="raw_retail_transactions")
    clean_profile = profile_dataset(clean_df, dataset_name="clean_reference_retail_transactions")

    full_report = {
        "project": "Serverless ETL & Data Quality Pipeline - Phase 1",
        "phase": "Phase 1 - Local Baseline & Quality Profiling",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "raw_total_records": len(raw_df),
            "clean_total_records": len(clean_df),
            "raw_anomalies_detected": {
                "duplicates": raw_profile["data_quality_issues"]["duplicate_records_count"],
                "missing_values": raw_profile["data_quality_issues"]["total_missing_values"],
                "invalid_amounts": raw_profile["data_quality_issues"]["invalid_total_amounts_lte_zero"]
                + raw_profile["data_quality_issues"]["invalid_unit_prices_lte_zero"],
                "malformed_dates": raw_profile["data_quality_issues"]["malformed_or_unparseable_dates"],
            },
            "clean_anomalies_detected": {
                "duplicates": clean_profile["data_quality_issues"]["duplicate_records_count"],
                "missing_values": clean_profile["data_quality_issues"]["total_missing_values"],
                "invalid_amounts": clean_profile["data_quality_issues"]["invalid_total_amounts_lte_zero"],
                "malformed_dates": clean_profile["data_quality_issues"]["malformed_or_unparseable_dates"],
            },
        },
        "raw_dataset_profile": raw_profile,
        "clean_reference_dataset_profile": clean_profile,
    }

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(full_report, f, indent=2)

    logger.info(f"Data exploration and quality statistics report written to: {out_path}")
    return full_report


if __name__ == "__main__":
    generate_statistics_report()
