"""AWS Lambda Handler for S3 Ingestion, Extraction, Transformation, and Quality Routing.

Triggers on S3 ObjectCreated events under `raw/`.
Performs:
1. Anti-recursion filter (strict path prefix check).
2. Extraction from S3 into memory.
3. Schema validation, standardization, and type casting.
4. Anomaly detection & quality scoring.
5. Partitioned writes to `curated/` or `quarantine/`.
6. CloudWatch structured logging and telemetry.
"""

from __future__ import annotations

import io
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

# Configure structured CloudWatch logger
logger = logging.getLogger("RetailETLLambda")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('{"timestamp": "%(asctime)s", "level": "%(levelname)s", "message": %(message)s}')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# Expected canonical schema definition
CANONICAL_SCHEMA = {
    "order_id": "string",
    "customer_id": "string",
    "product_id": "string",
    "product_name": "string",
    "category": "string",
    "quantity": "int64",
    "unit_price": "float64",
    "total_amount": "float64",
    "order_date": "string",
    "region": "string",
    "payment_method": "string",
    "order_status": "string",
}

# Recursion prevention safeguards
ALLOWED_TRIGGER_PREFIX = "raw/"
FORBIDDEN_TRIGGER_PREFIXES = ("curated/", "warehouse/", "quarantine/", "reports/")


def log_structured(event_type: str, data: Dict[str, Any]) -> None:
    """Logs a structured JSON message for AWS CloudWatch Insights query parsing."""
    payload = {
        "event_type": event_type,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        **data,
    }
    logger.info(json.dumps(payload))


def transform_and_validate_dataframe(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    """Cleans, validates, transforms, and splits records into curated vs quarantine sets.

    Transformations:
    - Standardize column names (strip whitespace, lowercase).
    - Parse dates into ISO-8601 strings (%Y-%m-%d %H:%M:%S) and capture malformed strings.
    - Convert quantity and price metrics to strict numeric types.
    - Deduplicate identical records and duplicate order_ids.
    - Validate business rules: unit_price > 0, quantity > 0, total_amount > 0.
    """
    # 1. Standardize column names
    df.columns = [c.strip().lower() for c in df.columns]

    missing_schema_cols = [col for col in CANONICAL_SCHEMA if col not in df.columns]
    if missing_schema_cols:
        raise ValueError(f"Input data missing mandatory schema columns: {missing_schema_cols}")

    total_input_rows = len(df)
    quarantine_reasons: Dict[int, List[str]] = {i: [] for i in df.index}

    # 2. Check for missing mandatory identifier fields
    for idx, row in df.iterrows():
        if pd.isna(row["customer_id"]) or str(row["customer_id"]).strip() in ("", "nan", "None"):
            quarantine_reasons[idx].append("MISSING_CUSTOMER_ID")
        if pd.isna(row["payment_method"]) or str(row["payment_method"]).strip() in ("", "nan", "None"):
            quarantine_reasons[idx].append("MISSING_PAYMENT_METHOD")

    # 3. Numeric validation and casting
    numeric_quantities = pd.to_numeric(df["quantity"], errors="coerce")
    numeric_unit_prices = pd.to_numeric(df["unit_price"], errors="coerce")
    numeric_totals = pd.to_numeric(df["total_amount"], errors="coerce")

    for idx in df.index:
        q_val = numeric_quantities[idx]
        p_val = numeric_unit_prices[idx]
        t_val = numeric_totals[idx]

        if pd.isna(q_val) or q_val <= 0:
            quarantine_reasons[idx].append(f"INVALID_QUANTITY: {df.loc[idx, 'quantity']}")
        if pd.isna(p_val) or p_val <= 0:
            quarantine_reasons[idx].append(f"INVALID_UNIT_PRICE: {df.loc[idx, 'unit_price']}")
        if pd.isna(t_val) or t_val <= 0:
            quarantine_reasons[idx].append(f"INVALID_TOTAL_AMOUNT: {df.loc[idx, 'total_amount']}")

    # 4. Date parsing & standardization
    date_series = df["order_date"].astype(str).str.strip()
    parsed_dates = pd.to_datetime(date_series.tolist(), errors="coerce")

    for idx, parsed in zip(df.index, parsed_dates):
        if pd.isna(parsed):
            quarantine_reasons[idx].append(f"MALFORMED_DATE: {df.loc[idx, 'order_date']}")

    # 5. Duplicates detection (Primary Key order_id duplicates)
    duplicated_order_id_mask = df.duplicated(subset=["order_id"], keep="first")
    for idx in df[duplicated_order_id_mask].index:
        quarantine_reasons[idx].append(f"DUPLICATE_ORDER_ID: {df.loc[idx, 'order_id']}")

    # Separate Curated vs Quarantined records
    quarantine_indices = [idx for idx, reasons in quarantine_reasons.items() if len(reasons) > 0]
    curated_indices = [idx for idx, reasons in quarantine_reasons.items() if len(reasons) == 0]

    # Build Curated DataFrame
    curated_df = df.loc[curated_indices].copy()
    curated_df["quantity"] = numeric_quantities.loc[curated_indices].astype(int)
    curated_df["unit_price"] = numeric_unit_prices.loc[curated_indices].astype(float).round(2)
    curated_df["total_amount"] = numeric_totals.loc[curated_indices].astype(float).round(2)
    curated_df["order_date"] = pd.Series(parsed_dates, index=df.index).loc[curated_indices].dt.strftime("%Y-%m-%d %H:%M:%S")

    # Add ingestion auditing metadata columns
    curated_df["etl_processed_at_utc"] = datetime.now(timezone.utc).isoformat()
    curated_df["etl_status"] = "PASSED_QUALITY_CHECKS"

    # Build Quarantine DataFrame
    quarantine_df = df.loc[quarantine_indices].copy()
    quarantine_df["rejection_reasons"] = ["; ".join(quarantine_reasons[idx]) for idx in quarantine_indices]
    quarantine_df["etl_processed_at_utc"] = datetime.now(timezone.utc).isoformat()
    quarantine_df["etl_status"] = "QUARANTINED"

    metrics = {
        "total_records_processed": total_input_rows,
        "curated_records_count": len(curated_df),
        "quarantined_records_count": len(quarantine_df),
        "rejection_summary": {
            "missing_customer_id": sum(1 for r in quarantine_reasons.values() if any("MISSING_CUSTOMER_ID" in x for x in r)),
            "missing_payment_method": sum(1 for r in quarantine_reasons.values() if any("MISSING_PAYMENT_METHOD" in x for x in r)),
            "invalid_quantity": sum(1 for r in quarantine_reasons.values() if any("INVALID_QUANTITY" in x for x in r)),
            "invalid_unit_price": sum(1 for r in quarantine_reasons.values() if any("INVALID_UNIT_PRICE" in x for x in r)),
            "invalid_total_amount": sum(1 for r in quarantine_reasons.values() if any("INVALID_TOTAL_AMOUNT" in x for x in r)),
            "malformed_date": sum(1 for r in quarantine_reasons.values() if any("MALFORMED_DATE" in x for x in r)),
            "duplicate_order_id": sum(1 for r in quarantine_reasons.values() if any("DUPLICATE_ORDER_ID" in x for x in r)),
        },
    }
    return curated_df, quarantine_df, metrics


def lambda_handler(event: Dict[str, Any], context: Any = None) -> Dict[str, Any]:
    """Main AWS Lambda execution entry point.

    Handles S3 Event Notifications, extracts raw data, transforms, partitions,
    and publishes metrics to CloudWatch.
    """
    log_structured("LAMBDA_INVOCATION_START", {"records_received": len(event.get("Records", []))})

    # Local S3 manager import
    from src.s3_client import S3DataLakeManager

    results = []
    for record in event.get("Records", []):
        # Extract bucket and key from S3 Event Notification format
        s3_info = record.get("s3", {})
        bucket_name = s3_info.get("bucket", {}).get("name") or os.getenv("S3_BUCKET_NAME", "retail-serverless-data-lake")
        s3_manager = S3DataLakeManager(bucket_name=bucket_name)
        s3_key = s3_info.get("object", {}).get("key", "")

        log_structured(
            "PROCESSING_S3_OBJECT",
            {"bucket": bucket_name, "key": s3_key, "event_name": record.get("eventName")},
        )

        # TASK 10: Anti-Recursion Trigger Safeguard
        # Strict validation: Only process files inside raw/ and reject non-csv or downstream destinations
        if not s3_key.startswith(ALLOWED_TRIGGER_PREFIX):
            log_structured(
                "RECURSION_GUARD_SKIPPED",
                {"message": f"Skipping key {s3_key}: does not start with {ALLOWED_TRIGGER_PREFIX}"},
            )
            continue

        if any(s3_key.startswith(bad) for bad in FORBIDDEN_TRIGGER_PREFIXES):
            log_structured(
                "RECURSION_GUARD_BLOCKED",
                {"message": f"CRITICAL: Key {s3_key} belongs to downstream storage. Blocking execution."},
            )
            continue

        if not s3_key.lower().endswith(".csv"):
            log_structured("SKIPPED_NON_CSV", {"key": s3_key})
            continue

        # Extract file from S3
        file_bytes = s3_manager.get_object(s3_key)
        df_raw = pd.read_csv(io.BytesIO(file_bytes))

        # Transform and perform DQ validation
        curated_df, quarantine_df, metrics = transform_and_validate_dataframe(df_raw)

        # Generate output keys
        base_name = Path(s3_key).stem
        timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

        curated_key = f"curated/{base_name}_curated_{timestamp_str}.csv"
        quarantine_key = f"quarantine/{base_name}_quarantine_{timestamp_str}.csv"
        report_key = f"reports/{base_name}_etl_audit_{timestamp_str}.json"

        # Save to S3 curated prefix
        if not curated_df.empty:
            curated_bytes = curated_df.to_csv(index=False).encode("utf-8")
            s3_manager.put_object(
                key=curated_key,
                body=curated_bytes,
                content_type="text/csv",
                server_side_encryption="AES256",
            )
            log_structured("STORED_CURATED_DATA", {"key": curated_key, "row_count": len(curated_df)})

        # Save to S3 quarantine prefix
        if not quarantine_df.empty:
            quarantine_bytes = quarantine_df.to_csv(index=False).encode("utf-8")
            s3_manager.put_object(
                key=quarantine_key,
                body=quarantine_bytes,
                content_type="text/csv",
                server_side_encryption="AES256",
            )
            log_structured("STORED_QUARANTINE_DATA", {"key": quarantine_key, "row_count": len(quarantine_df)})

        # Save ETL audit metrics report to S3 reports/ prefix
        report_payload = {
            "source_key": s3_key,
            "processed_at_utc": datetime.now(timezone.utc).isoformat(),
            "metrics": metrics,
            "curated_destination": curated_key if not curated_df.empty else None,
            "quarantine_destination": quarantine_key if not quarantine_df.empty else None,
        }
        s3_manager.put_object(
            key=report_key,
            body=json.dumps(report_payload, indent=2).encode("utf-8"),
            content_type="application/json",
            server_side_encryption="AES256",
        )

        log_structured("ETL_PIPELINE_SUCCESS", report_payload)
        results.append(report_payload)

    return {
        "statusCode": 200,
        "body": json.dumps({"status": "SUCCESS", "processed_batches": len(results), "details": results}),
    }
