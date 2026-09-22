"""PE percentile calculations.

This module contains calculation helpers only. It intentionally does not
produce buy, hold, or sell decisions.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime
from typing import Any

import numpy as np
import pandas as pd


def classify_percentile_status(
    percentile: float | None,
    low_percentile: float = 30,
    high_percentile: float = 70,
) -> str:
    """Return the common low/middle/high valuation status."""
    if percentile is None or pd.isna(percentile):
        return "不可用"
    value = float(percentile)
    if value < low_percentile:
        return "低估"
    if value <= high_percentile:
        return "适中"
    return "高估"


def calculate_percentile(
    pe_series: pd.Series,
    current_pe: float,
    min_samples: int = 60,
) -> float | None:
    """Calculate the percentage of valid PE observations below current PE."""
    values = pd.to_numeric(pe_series, errors="coerce")
    values = values.loc[values > 0].dropna()
    if len(values) < min_samples or current_pe <= 0:
        return None
    return float((values < current_pe).mean() * 100.0)


def calculate_percentile_windows(
    history: pd.DataFrame,
    windows: Sequence[int] = (5, 10),
    min_samples: int = 60,
) -> dict[str, float | int | str | None]:
    """Calculate daily PE percentiles for multiple historical windows."""
    required = {"trade_date", "pe_ttm"}
    missing = required - set(history.columns)
    if missing:
        raise ValueError(f"PE历史缺少字段: {sorted(missing)}")

    data = history.loc[:, ["trade_date", "pe_ttm"]].copy()
    data["trade_date"] = pd.to_datetime(data["trade_date"], errors="coerce")
    data["pe_ttm"] = pd.to_numeric(data["pe_ttm"], errors="coerce")
    data = data.dropna(subset=["trade_date"])
    data = data.loc[data["pe_ttm"] > 0].sort_values("trade_date")
    data = data.drop_duplicates("trade_date", keep="last")
    if data.empty:
        raise RuntimeError("没有可用的正 PE 数据")

    latest_date = pd.Timestamp(data.iloc[-1]["trade_date"])
    current_pe = float(data.iloc[-1]["pe_ttm"])
    result: dict[str, float | int | str | None] = {}

    for years in windows:
        window_start = latest_date - pd.DateOffset(years=years)
        window = data.loc[data["trade_date"] >= window_start]
        percentile = calculate_percentile(
            window["pe_ttm"],
            current_pe,
            min_samples=min_samples,
        )
        result[f"pe_percentile_{years}y"] = (
            round(percentile, 2) if percentile is not None else None
        )
        result[f"pe_window_{years}y_samples"] = int(len(window))
        result[f"pe_window_{years}y_start"] = (
            window["trade_date"].min().date().isoformat()
            if not window.empty
            else None
        )
    return result


def calculate_ma_deviation_series(
    history: pd.DataFrame,
    window: int = 250,
) -> pd.DataFrame:
    """Calculate close, MA, and deviation percentage series."""
    required = {"trade_date", "close"}
    missing = required - set(history.columns)
    if missing:
        raise ValueError(f"均线历史缺少字段: {sorted(missing)}")

    frame = history.loc[:, ["trade_date", "close"]].copy()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame = frame.dropna(subset=["trade_date", "close"])
    frame = frame.sort_values("trade_date").drop_duplicates(
        "trade_date",
        keep="last",
    )
    frame["ma"] = frame["close"].rolling(window=window).mean()
    frame["deviation_pct"] = (
        (frame["close"] - frame["ma"]) / frame["ma"] * 100.0
    )
    return frame.reset_index(drop=True)


def calculate_latest_deviation_percentile(
    deviation_history: pd.DataFrame,
    years: int = 5,
    min_samples: int = 60,
) -> dict[str, Any]:
    """Calculate the current MA-deviation percentile."""
    required = {"trade_date", "deviation_pct"}
    missing = required - set(deviation_history.columns)
    if missing:
        raise ValueError(f"偏离度历史缺少字段: {sorted(missing)}")

    frame = deviation_history.loc[
        :, ["trade_date", "deviation_pct"]
    ].copy()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    frame["deviation_pct"] = pd.to_numeric(
        frame["deviation_pct"],
        errors="coerce",
    )
    frame = frame.dropna(subset=["trade_date", "deviation_pct"])
    frame = frame.sort_values("trade_date")
    if frame.empty:
        return {
            "deviation_pct": None,
            "deviation_percentile": None,
            "deviation_samples": 0,
            "deviation_window_start": None,
        }

    latest = frame.iloc[-1]
    latest_date = pd.Timestamp(latest["trade_date"])
    window_start = latest_date - pd.DateOffset(years=years)
    window = frame.loc[frame["trade_date"] >= window_start]
    current = float(latest["deviation_pct"])
    percentile = (
        float((window["deviation_pct"] < current).mean() * 100.0)
        if len(window) >= min_samples
        else None
    )
    return {
        "deviation_pct": round(current, 4),
        "deviation_percentile": (
            round(percentile, 2) if percentile is not None else None
        ),
        "deviation_samples": int(len(window)),
        "deviation_window_start": (
            window["trade_date"].min().date().isoformat()
            if not window.empty
            else None
        ),
    }


def calculate_volatility_10(
    close_series: pd.Series,
    window: int = 10,
    annualization_factor: int = 252,
) -> float | None:
    """Calculate the latest annualized volatility over N trading days."""
    values = pd.to_numeric(close_series, errors="coerce").dropna()
    if len(values) < window + 1:
        return None
    returns = values.pct_change().dropna().tail(window)
    if len(returns) < window:
        return None
    volatility = float(returns.std(ddof=1) * np.sqrt(annualization_factor) * 100)
    return None if np.isnan(volatility) else volatility


def calculate_volatility_discount(
    volatility_pct: float | None,
    tiers: Sequence[Mapping[str, Any]],
) -> float:
    """Return the configured volatility discount."""
    if volatility_pct is None or pd.isna(volatility_pct):
        return 1.0
    value = float(volatility_pct)
    for tier in tiers:
        maximum = tier.get("max_pct")
        if maximum is None or value < float(maximum):
            return float(tier["discount"])
    return float(tiers[-1]["discount"])


def calculate_ma_deviation_amount(
    initial_amount: float | None,
    deviation_percentile: float | None,
    volatility_pct: float | None,
    volatility_tiers: Sequence[Mapping[str, Any]],
    low_percentile: float = 30,
    high_percentile: float = 70,
    low_multiplier: float = 1.5,
    middle_multiplier: float = 1.0,
) -> dict[str, Any]:
    """Calculate the MA-deviation strategy amount."""
    state = classify_percentile_status(
        deviation_percentile,
        low_percentile=low_percentile,
        high_percentile=high_percentile,
    )
    if state == "不可用" or initial_amount is None:
        return {
            "state": state,
            "base_multiplier": None,
            "volatility_discount": None,
            "final_multiplier": None,
            "amount": None,
        }
    if state == "高估":
        return {
            "state": state,
            "base_multiplier": 0.0,
            "volatility_discount": 1.0,
            "final_multiplier": 0.0,
            "amount": 0.0,
        }

    base_multiplier = (
        float(low_multiplier)
        if state == "低估"
        else float(middle_multiplier)
    )
    discount = calculate_volatility_discount(
        volatility_pct,
        volatility_tiers,
    )
    final_multiplier = base_multiplier * discount
    return {
        "state": state,
        "base_multiplier": round(base_multiplier, 4),
        "volatility_discount": round(discount, 4),
        "final_multiplier": round(final_multiplier, 4),
        "amount": round(float(initial_amount) * final_multiplier, 2),
    }
