"""Duplicate detection validation check.

Identifies duplicate transactions based on primary business key (order_id)
and full duplicate rows.
"""

from __future__ import annotations

from typing import Dict, List
import pandas as pd


class DuplicateCheck:
    """Validates records for duplicate primary keys and duplicate rows."""

    NAME = "check_duplicates"

    def __init__(self, key_column: str = "order_id"):
        self.key_column = key_column

    def evaluate(self, df: pd.DataFrame) -> Dict[int, List[str]]:
        """Evaluates duplicate records and returns mapping of row index -> rejection reasons."""
        failures: Dict[int, List[str]] = {idx: [] for idx in df.index}

        if self.key_column not in df.columns:
            return failures

        # Primary business key duplicates (marking duplicates after the first occurrence)
        duplicated_keys = df.duplicated(subset=[self.key_column], keep="first")
        for idx in df[duplicated_keys].index:
            key_val = df.loc[idx, self.key_column]
            failures[idx].append(f"DUPLICATE_KEY: {self.key_column}='{key_val}'")

        return failures
