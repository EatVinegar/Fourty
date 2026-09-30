r"""AKShare data sources and normalization for index data.

This module is the only project module that calls AKShare. It does not
import Streamlit, SQLite, or any Miaoxiang client. All network calls happen
only when an explicit fetch function is called.

Supported test indices:

- 沪深300: 000300
- 红利低波: H30269
- 红利指数: 000922
- 上证50: 000016
"""

from __future__ import annotations

import re
import time
from datetime import date, datetime, timedelta
from importlib.metadata import version
from typing import Any, Iterable, Mapping, Sequence

import akshare as ak
import pandas as pd


AKSHARE_VERSION = version("akshare")
REQUEST_INTERVAL_SECONDS = 5
DEFAULT_HISTORY_YEARS = 10
MIN_PERCENTILE_SAMPLES = 60

INDEX_CONFIGS: dict[str, dict[str, Any]] = {
    "hs300": {
        "name": "沪深300",
        "csi_symbol": "000300",
        "quote_codes": ("000300",),
        "quote_names": ("沪深300",),
    },
    "dividend_lv": {
        "name": "红利低波",
        "csi_symbol": "H30269",
        "quote_codes": ("H30269",),
        "quote_names": ("红利低波",),
    },
    "csi_dividend": {
        "name": "红利指数",
        "csi_symbol": "000922",
        "quote_codes": ("000922",),
        "quote_names": ("中证红利",),
    },
    "sse50": {
        "name": "上证50",
        "csi_symbol": "000016",
        "quote_codes": ("000016",),
        "quote_names": ("上证50",),
    },
    "hstech": {
        "name": "恒生科技",
        "csi_symbol": "HSTECH",
        "quote_codes": ("HSTECH",),
        "quote_names": ("恒生科技",),
    },
}


def _normalize_date(value: str | date | datetime) -> str:
    """Convert a supported date value to YYYYMMDD for CSI requests."""
    if isinstance(value, datetime):
        return value.strftime("%Y%m%d")
    if isinstance(value, date):
        return value.strftime("%Y%m%d")

    text = str(value).strip()
    if re.fullmatch(r"\d{8}", text):
        return text
    return datetime.strptime(text, "%Y-%m-%d").strftime("%Y%m%d")


def _require_index(index_key: str) -> Mapping[str, Any]:
    try:
        return INDEX_CONFIGS[index_key]
    except KeyError as exc:
        supported = ", ".join(INDEX_CONFIGS)
        raise ValueError(
            f"未知指数 {index_key!r}，支持的值为: {supported}"
        ) from exc


def _find_column(columns: Sequence[str], candidates: Sequence[str]) -> str | None:
    normalized = {str(column): str(column) for column in columns}
    for candidate in candidates:
        if candidate in normalized:
            return normalized[candidate]

    for column in columns:
        text = str(column)
        if any(candidate in text for candidate in candidates):
            return text
    return None


def _normalize_history(raw: pd.DataFrame, index_key: str) -> pd.DataFrame:
    """Normalize CSI history into the project's internal daily schema."""
    if raw is None or raw.empty:
        raise RuntimeError(f"{INDEX_CONFIGS[index_key]['name']} 历史数据为空")

    columns = [str(column) for column in raw.columns]
    date_column = _find_column(columns, ("日期", "date"))
    close_column = _find_column(columns, ("收盘", "close"))
    change_column = _find_column(columns, ("涨跌幅",))
    pe_column = _find_column(
        columns,
        ("滚动市盈率", "市盈率PE(TTM)", "PE(TTM)"),
    )
    if date_column is None or close_column is None or pe_column is None:
        raise RuntimeError(
            f"{INDEX_CONFIGS[index_key]['name']} 历史字段不完整，"
            f"实际列名: {columns}"
        )

    frame = raw.copy()
    frame["trade_date"] = pd.to_datetime(
        frame[date_column],
        errors="coerce",
    ).dt.normalize()
    frame["close"] = pd.to_numeric(frame[close_column], errors="coerce")
    frame["change_pct"] = (
        pd.to_numeric(frame[change_column], errors="coerce")
        if change_column is not None
        else pd.NA
    )
    frame["pe_ttm"] = pd.to_numeric(frame[pe_column], errors="coerce")

    frame = frame.dropna(subset=["trade_date"]).sort_values("trade_date")
    frame = frame.drop_duplicates("trade_date", keep="last")
    frame = frame.loc[
        :, ["trade_date", "close", "change_pct", "pe_ttm"]
    ].reset_index(drop=True)
    frame["index_key"] = index_key
    return frame


def _normalize_quote_frame(raw: pd.DataFrame) -> pd.DataFrame:
    """Normalize the Sina index quote table."""
    if raw is None or raw.empty:
        raise RuntimeError("stock_zh_index_spot_sina 返回空数据")

    columns = [str(column) for column in raw.columns]
    code_column = _find_column(columns, ("代码",))
    name_column = _find_column(columns, ("名称",))
    price_column = _find_column(columns, ("最新价",))
    change_column = _find_column(columns, ("涨跌幅",))
    if None in (code_column, name_column, price_column, change_column):
        raise RuntimeError(f"指数行情字段不完整，实际列名: {columns}")

    frame = raw.copy()
    result = pd.DataFrame(
        {
            "quote_code": frame[code_column].astype(str),
            "quote_name": frame[name_column].astype(str),
            "price": pd.to_numeric(frame[price_column], errors="coerce"),
            "change_pct": pd.to_numeric(
                frame[change_column],
                errors="coerce",
            ),
        }
    )
    return result.dropna(subset=["price"]).reset_index(drop=True)


def _match_quote(
    index_key: str,
    quotes: pd.DataFrame,
) -> Mapping[str, Any] | None:
    """Match one configured index against the Sina quote table."""
    config = _require_index(index_key)
    for code in config["quote_codes"]:
        matched = quotes.loc[
            quotes["quote_code"].str.contains(
                re.escape(str(code)),
                case=False,
                regex=True,
            )
        ]
        if not matched.empty:
            return matched.iloc[-1].to_dict()

    for name in config["quote_names"]:
        matched = quotes.loc[
            quotes["quote_name"].str.contains(
                re.escape(str(name)),
                case=False,
                regex=True,
            )
        ]
        if not matched.empty:
            return matched.iloc[-1].to_dict()
    return None


def list_indices() -> tuple[str, ...]:
    """Return the supported index keys."""
    return tuple(INDEX_CONFIGS)


def fetch_index_history(
    index_key: str,
    start_date: str | date | datetime | None = None,
    end_date: str | date | datetime | None = None,
    history_years: int = DEFAULT_HISTORY_YEARS,
) -> pd.DataFrame:
    """Fetch and normalize daily CSI index history."""
    config = _require_index(index_key)
    end = datetime.now() if end_date is None else end_date
    if start_date is None:
        start = (
            end
            if isinstance(end, datetime)
            else datetime.combine(end, datetime.min.time())
        ) - timedelta(days=history_years * 366)
    else:
        start = start_date

    raw = fetch_csindex_history_raw(
        symbol=config["csi_symbol"],
        start_date=_normalize_date(start),
        end_date=_normalize_date(end),
    )
    return _normalize_history(raw, index_key)


def fetch_csindex_history_raw(
    symbol: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """Call the AKShare CSI history interface."""
    return ak.stock_zh_index_hist_csindex(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
    )


def fetch_hk_index_daily_raw(symbol: str) -> pd.DataFrame:
    """Call the AKShare Sina Hong Kong index history interface."""
    return ak.stock_hk_index_daily_sina(symbol=symbol)


def fetch_trade_calendar_raw() -> pd.DataFrame:
    """Call the AKShare Sina A-share trading calendar interface."""
    return ak.tool_trade_date_hist_sina()


def fetch_hk_index_quotes() -> pd.DataFrame:
    """Fetch and normalize Sina Hong Kong index quotes."""
    raw = ak.stock_hk_index_spot_sina()
    return _normalize_quote_frame(raw)


def match_index_quote(
    index_key: str,
    quotes: pd.DataFrame,
) -> Mapping[str, Any] | None:
    """Match one configured index against a normalized quote table."""
    return _match_quote(index_key, quotes)


def _normalize_index_code(value: object) -> str:
    """Strip the market prefix so sh000300 and 000300 compare equal."""
    text = str(value).strip().upper()
    for prefix in ("SH", "SZ", "BJ", "CS"):
        if text.startswith(prefix) and len(text) > len(prefix):
            return text[len(prefix) :]
    return text


def match_symbol_quote(
    symbol: str,
    name: str,
    quotes: pd.DataFrame,
) -> Mapping[str, Any] | None:
    """Match an arbitrary index symbol against a normalized quote table.

    Used for data-lake indices that are not listed in INDEX_CONFIGS. Returns
    None when the quote source does not carry the index, so callers fall back
    to the latest curated daily close.
    """
    if quotes is None or quotes.empty:
        return None

    target = _normalize_index_code(symbol)
    if target:
        normalized = quotes["quote_code"].map(_normalize_index_code)
        exact = quotes.loc[normalized == target]
        if not exact.empty:
            return exact.iloc[-1].to_dict()

        contained = quotes.loc[
            quotes["quote_code"].str.contains(
                re.escape(str(symbol)),
                case=False,
                regex=True,
            )
        ]
        if not contained.empty:
            return contained.iloc[-1].to_dict()

    label = str(name or "").strip()
    if label:
        matched = quotes.loc[
            quotes["quote_name"].str.contains(
                re.escape(label),
                case=False,
                regex=True,
            )
        ]
        if not matched.empty:
            return matched.iloc[-1].to_dict()
    return None


def fetch_index_quotes() -> pd.DataFrame:
    """Fetch the Sina index quote table once for all configured indices."""
    raw = ak.stock_zh_index_spot_sina()
    return _normalize_quote_frame(raw)


def calculate_pe_metrics(
    history: pd.DataFrame,
    windows: Sequence[int] = (5, 10),
) -> dict[str, Any]:
    """Calculate latest PE, earnings yield, and daily PE percentiles."""
    required = {"trade_date", "pe_ttm"}
    missing = required - set(history.columns)
    if missing:
        raise ValueError(f"历史数据缺少字段: {sorted(missing)}")

    data = history.loc[:, ["trade_date", "pe_ttm"]].copy()
    data["trade_date"] = pd.to_datetime(data["trade_date"], errors="coerce")
    data["pe_ttm"] = pd.to_numeric(data["pe_ttm"], errors="coerce")
    data = data.dropna(subset=["trade_date"])
    data = data.loc[data["pe_ttm"] > 0].sort_values("trade_date")
    data = data.drop_duplicates("trade_date", keep="last")
    if data.empty:
        raise RuntimeError("没有可用的正 PE 数据")

    latest = data.iloc[-1]
    latest_date = pd.Timestamp(latest["trade_date"])
    current_pe = float(latest["pe_ttm"])
    result: dict[str, Any] = {
        "pe_ttm": round(current_pe, 4),
        "earnings_yield_pct": round(100.0 / current_pe, 4),
        "pe_latest_date": latest_date.date().isoformat(),
    }

    for years in windows:
        window_start = latest_date - pd.DateOffset(years=years)
        window = data.loc[data["trade_date"] >= window_start]
        percentile = (
            float((window["pe_ttm"] < current_pe).mean() * 100.0)
            if len(window) >= MIN_PERCENTILE_SAMPLES
            else None
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


def fetch_cn_10y_yield() -> pd.DataFrame:
    """Fetch daily China 10-year government bond yield from Sina."""
    raw = ak.bond_gb_zh_sina(symbol="中国10年期国债")
    if raw is None or raw.empty:
        raise RuntimeError("bond_gb_zh_sina 返回空数据")

    columns = [str(column) for column in raw.columns]
    date_column = _find_column(columns, ("date", "日期"))
    close_column = _find_column(columns, ("close", "收盘"))
    if date_column is None or close_column is None:
        raise RuntimeError(f"国债收益率字段不完整，实际列名: {columns}")

    frame = pd.DataFrame(
        {
            "trade_date": pd.to_datetime(
                raw[date_column],
                errors="coerce",
            ).dt.normalize(),
            "yield_10y": pd.to_numeric(
                raw[close_column],
                errors="coerce",
            ),
        }
    )
    frame = frame.dropna(subset=["trade_date", "yield_10y"])
    frame = frame.sort_values("trade_date")
    frame = frame.drop_duplicates("trade_date", keep="last")
    frame["source"] = "AKShare / bond_gb_zh_sina"
    return frame.reset_index(drop=True)


def build_index_snapshot(
    index_key: str,
    history: pd.DataFrame,
    quotes: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """Build one normalized index snapshot."""
    config = _require_index(index_key)
    metrics = calculate_pe_metrics(history)
    quote = _match_quote(index_key, quotes) if quotes is not None else None

    if quote is None:
        latest = history.sort_values("trade_date").iloc[-1]
        price = float(latest["close"])
        change_pct = latest["change_pct"]
        if pd.isna(change_pct):
            change_pct = None
        else:
            change_pct = float(change_pct)
        quote_source = "stock_zh_index_hist_csindex"
    else:
        price = float(quote["price"])
        change_pct = (
            None if pd.isna(quote["change_pct"]) else float(quote["change_pct"])
        )
        quote_source = "stock_zh_index_spot_sina"

    history_latest = history.sort_values("trade_date").iloc[-1]
    history_change = history_latest["change_pct"]
    if pd.isna(history_change):
        history_change = None
    else:
        history_change = float(history_change)

    return {
        "index_key": index_key,
        "index_name": config["name"],
        "csi_symbol": config["csi_symbol"],
        "history_date": pd.Timestamp(history_latest["trade_date"]).date().isoformat(),
        "history_close": float(history_latest["close"]),
        "history_change_pct": history_change,
        "price": price,
        "change_pct": change_pct,
        "quote_source": quote_source,
        **metrics,
        "source": "AKShare",
    }


def fetch_index_snapshot(
    index_key: str,
    history_years: int = DEFAULT_HISTORY_YEARS,
) -> dict[str, Any]:
    """Fetch one index history and quote, then build its snapshot."""
    return fetch_all_index_snapshots(
        [index_key],
        history_years=history_years,
    )[index_key]


def fetch_all_index_snapshots(
    index_keys: Iterable[str] | None = None,
    history_years: int = DEFAULT_HISTORY_YEARS,
    request_interval_seconds: float = REQUEST_INTERVAL_SECONDS,
) -> dict[str, dict[str, Any]]:
    """Fetch all configured snapshots with one quote call and controlled gaps."""
    keys = tuple(index_keys or INDEX_CONFIGS)
    for index_key in keys:
        _require_index(index_key)

    try:
        quotes = fetch_index_quotes()
    except Exception:
        quotes = None

    snapshots: dict[str, dict[str, Any]] = {}
    for index_key in keys:
        if request_interval_seconds > 0:
            time.sleep(request_interval_seconds)
        history = fetch_index_history(
            index_key,
            history_years=history_years,
        )
        snapshots[index_key] = build_index_snapshot(
            index_key,
            history,
            quotes,
        )
    return snapshots


if __name__ == "__main__":
    for key, snapshot in fetch_all_index_snapshots().items():
        print(key)
        for field, value in snapshot.items():
            print(f"  {field}: {value}")
