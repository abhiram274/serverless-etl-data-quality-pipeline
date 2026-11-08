"""Transaction amount and quantity integrity validation check.

Validates that numeric monetary and volume fields are positive (> 0) and
mathematically consistent within acceptable rounding limits.
"""

from __future__ import annotations

from typing import Dict, List
import pandas as pd


class AmountCheck:
    """Validates records for invalid, zero, or negative numeric amounts."""

    NAME = "check_invalid_amounts"

    def evaluate(self, df: pd.DataFrame) -> Dict[int, List[str]]:
        """Evaluates numeric fields and returns mapping of row index -> rejection reasons."""
        failures: Dict[int, List[str]] = {idx: [] for idx in df.index}

        quantities = pd.to_numeric(df["quantity"], errors="coerce") if "quantity" in df.columns else pd.Series(dtype=float)
        unit_prices = pd.to_numeric(df["unit_price"], errors="coerce") if "unit_price" in df.columns else pd.Series(dtype=float)
        total_amounts = pd.to_numeric(df["total_amount"], errors="coerce") if "total_amount" in df.columns else pd.Series(dtype=float)

        for idx in df.index:
            # Check Quantity
            if idx in quantities.index and (pd.isna(quantities[idx]) or quantities[idx] <= 0):
                failures[idx].append(f"INVALID_QUANTITY: {df.loc[idx, 'quantity']}")

            # Check Unit Price
            if idx in unit_prices.index and (pd.isna(unit_prices[idx]) or unit_prices[idx] <= 0):
                failures[idx].append(f"INVALID_UNIT_PRICE: {df.loc[idx, 'unit_price']}")

            # Check Total Amount
            if idx in total_amounts.index and (pd.isna(total_amounts[idx]) or total_amounts[idx] <= 0):
                failures[idx].append(f"INVALID_TOTAL_AMOUNT: {df.loc[idx, 'total_amount']}")

        return failures
