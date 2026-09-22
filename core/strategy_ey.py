"""Earnings-yield calculations.

This module contains calculation helpers only. It intentionally does not
produce buy, hold, or sell decisions.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

import pandas as pd

from core.strategy_pct import classify_percentile_status


def calculate_earnings_yield(pe_ttm: float | int | None) -> float | None:
    """Return E/P in percent for a valid positive PE."""
    if pe_ttm is None:
        return None
    pe_value = float(pe_ttm)
    if pe_value <= 0:
        return None
    return 100.0 / pe_value


def calculate_earnings_yield_series(pe_series: pd.Series) -> pd.Series:
    """Return an E/P percentage series without producing trading signals."""
    pe_values = pd.to_numeric(pe_series, errors="coerce")
    result = 100.0 / pe_values.where(pe_values > 0)
    return result.astype(float)


def calculate_months_elapsed(
    start_date: str | date | datetime,
    as_of: str | date | datetime | None = None,
) -> int:
    """Return complete calendar months since the strategy start month."""
    start = pd.Timestamp(start_date)
    current = pd.Timestamp(as_of or date.today())
    months = (current.year - start.year) * 12 + (current.month - start.month)
    return max(0, int(months))


def calculate_valuation_percentile_amount(
    initial_amount: float | None,
    pe_ttm: float | None,
    pe_percentile: float | None,
    month_index: int,
    low_percentile: float = 30,
    high_percentile: float = 70,
    low_multiplier: float = 1.5,
    middle_multiplier: float = 1.0,
    inflation_rate: float = 1.01,
) -> dict[str, Any]:
    """Calculate the valuation-percentile DCA amount."""
    state = classify_percentile_status(
        pe_percentile,
        low_percentile=low_percentile,
        high_percentile=high_percentile,
    )
    if (
        state == "不可用"
        or initial_amount is None
        or pe_ttm is None
        or float(pe_ttm) <= 0
    ):
        return {
            "state": state,
            "amplifier": None,
            "inflation_factor": None,
            "amount": None,
        }
    if state == "高估":
        return {
            "state": state,
            "amplifier": 0.0,
            "inflation_factor": round(
                float(inflation_rate) ** max(0, int(month_index)),
                6,
            ),
            "amount": 0.0,
        }

    amplifier = (
        float(low_multiplier)
        if state == "低估"
        else float(middle_multiplier)
    )
    inflation_factor = float(inflation_rate) ** max(0, int(month_index))
    amount = (
        float(initial_amount)
        * (10.0 / float(pe_ttm)) ** amplifier
        * inflation_factor
    )
    return {
        "state": state,
        "amplifier": round(amplifier, 4),
        "inflation_factor": round(inflation_factor, 6),
        "amount": round(amount, 2),
    }
