"""Data quality validation engine for Retail Transaction pipelines.

Orchestrates modular DQ checks, partitions data into curated vs quarantine sets,
generates detailed execution quality reports, and maintains idempotency.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import pandas as pd

from src.quality.check_duplicates import DuplicateCheck
from src.quality.check_missing import MissingFieldsCheck
from src.quality.check_amounts import AmountCheck
from src.quality.check_dates import DateCheck

logger = logging.getLogger(__name__)


class DataQualityEngine:
    """Orchestrates modular validation rules and splits data into curated and quarantine subsets."""

    def __init__(self):
        self.duplicate_check = DuplicateCheck(key_column="order_id")
        self.missing_check = MissingFieldsCheck()
        self.amount_check = AmountCheck()
        self.date_check = DateCheck(date_column="order_date")

    def validate_and_partition(
        self,
        df: pd.DataFrame,
        batch_id: str = "",
        dataset_name: str = "retail_transactions",
    ) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
        """Executes all 4 validation checks, tags failures, and returns (curated_df, quarantine_df, report)."""
        total_input_records = len(df)
        working_df = df.copy()

        # Generate deterministic batch checksum for idempotency tracking
        data_checksum = hashlib.sha256(pd.util.hash_pandas_object(working_df, index=True).values).hexdigest()
        execution_batch_id = batch_id or f"batch_{data_checksum[:12]}"

        # Initialize tracking maps
        row_rejection_reasons: Dict[int, List[str]] = {idx: [] for idx in working_df.index}

        # Rule 1: Duplicate Records Check
        dup_failures = self.duplicate_check.evaluate(working_df)
        dup_failure_count = sum(1 for v in dup_failures.values() if v)

        # Rule 2: Missing Required Fields Check
        missing_failures = self.missing_check.evaluate(working_df)
        missing_failure_count = sum(1 for v in missing_failures.values() if v)

        # Rule 3: Invalid Amounts Check
        amount_failures = self.amount_check.evaluate(working_df)
        amount_failure_count = sum(1 for v in amount_failures.values() if v)

        # Rule 4: Malformed Dates Check
        date_failures = self.date_check.evaluate(working_df)
        date_failure_count = sum(1 for v in date_failures.values() if v)

        # Aggregate rejection reasons per row
        for idx in working_df.index:
            row_rejection_reasons[idx].extend(dup_failures.get(idx, []))
            row_rejection_reasons[idx].extend(missing_failures.get(idx, []))
            row_rejection_reasons[idx].extend(amount_failures.get(idx, []))
            row_rejection_reasons[idx].extend(date_failures.get(idx, []))

        # Split indices based on presence of rejection reasons
        quarantine_indices = [idx for idx, reasons in row_rejection_reasons.items() if len(reasons) > 0]
        curated_indices = [idx for idx, reasons in row_rejection_reasons.items() if len(reasons) == 0]

        # -----------------------------
        # Curated DataFrame Processing
        # -----------------------------
        curated_df = working_df.loc[curated_indices].copy()
        if not curated_df.empty:
            # Canonical typing and formatting
            curated_df["quantity"] = pd.to_numeric(curated_df["quantity"]).astype(int)
            curated_df["unit_price"] = pd.to_numeric(curated_df["unit_price"]).astype(float).round(2)
            curated_df["total_amount"] = pd.to_numeric(curated_df["total_amount"]).astype(float).round(2)
            parsed_dt = pd.to_datetime(curated_df["order_date"].tolist())
            curated_df["order_date"] = parsed_dt.strftime("%Y-%m-%d %H:%M:%S")

            # Audit metadata
            curated_df["batch_id"] = execution_batch_id
            curated_df["etl_processed_at_utc"] = datetime.now(timezone.utc).isoformat()
            curated_df["quality_status"] = "PASSED_ALL_CHECKS"

        # --------------------------------
        # Quarantine DataFrame Processing
        # --------------------------------
        quarantine_df = working_df.loc[quarantine_indices].copy()
        if not quarantine_df.empty:
            quarantine_df["rejection_reasons"] = ["; ".join(row_rejection_reasons[idx]) for idx in quarantine_indices]
            quarantine_df["rejection_count"] = [len(row_rejection_reasons[idx]) for idx in quarantine_indices]
            quarantine_df["batch_id"] = execution_batch_id
            quarantine_df["etl_processed_at_utc"] = datetime.now(timezone.utc).isoformat()
            quarantine_df["quality_status"] = "FAILED_QUALITY_CHECKS"

        # --------------------------------
        # JSON Quality Report Generation
        # --------------------------------
        quality_report = {
            "report_type": "DataQualityExecutionReport",
            "dataset_name": dataset_name,
            "batch_id": execution_batch_id,
            "data_checksum": data_checksum,
            "execution_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "record_counts": {
                "total_input_records": total_input_records,
                "valid_curated_records": len(curated_df),
                "invalid_quarantined_records": len(quarantine_df),
                "quality_pass_rate_pct": round((len(curated_df) / total_input_records * 100), 2)
                if total_input_records > 0
                else 0.0,
            },
            "validation_failures_by_rule": {
                "duplicate_records": dup_failure_count,
                "missing_required_fields": missing_failure_count,
                "invalid_or_negative_amounts": amount_failure_count,
                "malformed_or_invalid_dates": date_failure_count,
            },
            "rejection_reason_details": {
                "missing_customer_id": sum(1 for r in row_rejection_reasons.values() if any("MISSING_REQUIRED_FIELD: 'customer_id'" in x for x in r)),
                "missing_payment_method": sum(1 for r in row_rejection_reasons.values() if any("MISSING_REQUIRED_FIELD: 'payment_method'" in x for x in r)),
                "invalid_quantity": sum(1 for r in row_rejection_reasons.values() if any("INVALID_QUANTITY" in x for x in r)),
                "invalid_unit_price": sum(1 for r in row_rejection_reasons.values() if any("INVALID_UNIT_PRICE" in x for x in r)),
                "invalid_total_amount": sum(1 for r in row_rejection_reasons.values() if any("INVALID_TOTAL_AMOUNT" in x for x in r)),
                "malformed_dates": sum(1 for r in row_rejection_reasons.values() if any("MALFORMED_DATE" in x for x in r)),
                "duplicate_order_ids": sum(1 for r in row_rejection_reasons.values() if any("DUPLICATE_KEY" in x for x in r)),
            },
            "schema_integrity": {
                "quarantine_preserved_all_columns": set(working_df.columns).issubset(set(quarantine_df.columns)),
                "idempotent_hash": data_checksum,
            },
        }

        return curated_df, quarantine_df, quality_report
