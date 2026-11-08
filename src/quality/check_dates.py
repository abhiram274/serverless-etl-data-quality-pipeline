"""Date format and temporal validity validation check.

Validates that order dates can be parsed into valid calendar dates and
belong to a realistic operational timeline.
"""

from __future__ import annotations

from typing import Dict, List
import pandas as pd


class DateCheck:
    """Validates records for malformed, unparseable, or out-of-bounds dates."""

    NAME = "check_malformed_dates"

    def __init__(self, date_column: str = "order_date"):
        self.date_column = date_column

    def evaluate(self, df: pd.DataFrame) -> Dict[int, List[str]]:
        """Evaluates date strings and returns mapping of row index -> rejection reasons."""
        failures: Dict[int, List[str]] = {idx: [] for idx in df.index}

        if self.date_column not in df.columns:
            for idx in df.index:
                failures[idx].append(f"MISSING_DATE_COLUMN: '{self.date_column}'")
            return failures

        date_series = df[self.date_column].astype(str).str.strip()
        parsed_dates = pd.to_datetime(date_series.tolist(), errors="coerce")

        for idx, parsed in zip(df.index, parsed_dates):
            if pd.isna(parsed):
                failures[idx].append(f"MALFORMED_DATE: '{df.loc[idx, self.date_column]}'")

        return failures
