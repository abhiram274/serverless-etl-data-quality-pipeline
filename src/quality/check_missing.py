"""Required fields and completeness validation check.

Validates that non-nullable business fields are present, non-null, and non-empty.
"""

from __future__ import annotations

from typing import Dict, List, Optional
import pandas as pd


class MissingFieldsCheck:
    """Validates records for missing, null, or empty string required fields."""

    NAME = "check_missing_fields"
    DEFAULT_REQUIRED_FIELDS = [
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "unit_price",
        "total_amount",
        "order_date",
        "payment_method",
    ]

    def __init__(self, required_fields: Optional[List[str]] = None):
        self.required_fields = required_fields or self.DEFAULT_REQUIRED_FIELDS

    def evaluate(self, df: pd.DataFrame) -> Dict[int, List[str]]:
        """Evaluates required fields and returns mapping of row index -> rejection reasons."""
        failures: Dict[int, List[str]] = {idx: [] for idx in df.index}

        for col in self.required_fields:
            if col not in df.columns:
                for idx in df.index:
                    failures[idx].append(f"MISSING_REQUIRED_COLUMN: '{col}'")
                continue

            # Check nulls or empty string equivalents
            is_na = df[col].isna()
            is_empty_str = df[col].astype(str).str.strip().isin(["", "nan", "None", "NULL", "null"])
            missing_mask = is_na | is_empty_str

            for idx in df[missing_mask].index:
                failures[idx].append(f"MISSING_REQUIRED_FIELD: '{col}'")

        return failures
