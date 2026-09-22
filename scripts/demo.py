r"""AKShare data-source feasibility check for the Fourty project.

This script intentionally makes at most five external data calls:

1. ``stock_zh_index_spot_sina()`` for the latest index quotes.
2. ``stock_zh_index_hist_csindex("000300", ...)`` for CSI 300 daily PE.
3. ``stock_zh_index_hist_csindex("H30269", ...)`` for dividend low-vol PE.
4. ``stock_zh_index_hist_csindex("000922", ...)`` for CSI Dividend PE.
5. ``stock_index_pb_lg("沪深300")`` for CSI 300 PB only.

All PE values and PE percentiles are calculated from the daily observations
returned by ``stock_zh_index_hist_csindex``. No monthly resampling is done.

It does not use Eastmoney interfaces or custom HTTP scraping. It has no retry
loop, scheduler, or automatic refresh. Run it manually only when needed.

All Python scripts in this project must run with the project environment:

    D:\code\Fourty\.venv\python.exe scripts\demo.py
"""

from __future__ import annotations

import sys
import time
from datetime import datetime, timedelta
from typing import Any, Callable


CALL_INTERVAL_SECONDS = 5
MIN_DAILY_PERCENTILE_SAMPLES = 60
INDEX_HISTORY_YEARS = 10

INDEX_CASES = (
    {
        "code": "000300",
        "name": "沪深300",
        "quote_code": "000300",
        "quote_name": "沪深300",
    },
    {
        "code": "H30269",
        "name": "红利低波",
        "quote_code": "H30269",
        "quote_name": "红利低波",
    },
    {
        "code": "000922",
        "name": "红利指数",
        "quote_code": "000922",
        "quote_name": "中证红利",
    },
)


def _import_dependencies() -> tuple[Any, Any]:
    """Import dependencies only when the script is actually executed."""
    try:
        import akshare as ak
        import pandas as pd
    except ImportError as exc:
        raise RuntimeError(
            "缺少依赖。请使用项目虚拟环境 "
            "D:\\code\\Fourty\\.venv\\python.exe 运行本脚本。"
        ) from exc
    return ak, pd


def _configure_console() -> None:
    """Use UTF-8 output on Windows terminals."""
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            try:
                stream.reconfigure(encoding="utf-8", errors="replace")
            except (AttributeError, ValueError):
                pass


def _find_column(columns: list[str], candidates: tuple[str, ...]) -> str | None:
    """Return the first exact column match, then try a partial match."""
    exact = {str(column): str(column) for column in columns}
    for candidate in candidates:
        if candidate in exact:
            return exact[candidate]

    for column in columns:
        text = str(column)
        if any(candidate in text for candidate in candidates):
            return text
    return None


def _prepare_frame(pd: Any, raw: Any, source_name: str) -> Any:
    """Validate an AKShare result and normalize its date column."""
    if raw is None or not hasattr(raw, "empty") or raw.empty:
        raise RuntimeError(f"{source_name} 返回空 DataFrame")

    columns = [str(column) for column in raw.columns]
    date_column = _find_column(columns, ("日期", "date"))
    if date_column is None:
        raise RuntimeError(f"{source_name} 未找到日期列，实际列名: {columns}")

    frame = raw.copy()
    frame["_date"] = pd.to_datetime(frame[date_column], errors="coerce")
    frame = frame.dropna(subset=["_date"]).sort_values("_date")
    if frame.empty:
        raise RuntimeError(f"{source_name} 没有可解析的日期")
    return frame


def _daily_series(pd: Any, frame: Any, value_column: str) -> Any:
    """Return all valid observations without monthly resampling."""
    data = frame[["_date", value_column]].copy()
    data["_value"] = pd.to_numeric(data[value_column], errors="coerce")
    data = data.dropna(subset=["_date", "_value"]).sort_values("_date")
    data = data.drop_duplicates("_date", keep="last").reset_index(drop=True)
    if data.empty:
        raise RuntimeError(f"{value_column} 没有可用数据")
    return data


def _infer_frequency(pd: Any, dates: Any) -> tuple[str, float | None]:
    """Infer the actual observation frequency from median date gaps."""
    ordered = pd.Series(pd.to_datetime(dates, errors="coerce")).dropna().sort_values()
    gaps = ordered.diff().dt.days.dropna()
    if gaps.empty:
        return "unknown", None

    median_gap = float(gaps.median())
    if median_gap <= 4:
        return "逐日", median_gap
    if 5 <= median_gap <= 10:
        return "周度", median_gap
    if 25 <= median_gap <= 35:
        return "月度", median_gap
    return "非规则", median_gap


def _percentile(series: Any, current_value: float) -> float:
    """Calculate the percentage of samples below the current value."""
    return float((series < current_value).mean() * 100.0)


def _extract_index_quote(
    pd: Any,
    raw: Any,
    quote_code: str,
    quote_name: str,
    source_name: str,
) -> dict[str, Any]:
    """Extract one index quote from the Sina index snapshot."""
    if raw is None or not hasattr(raw, "empty") or raw.empty:
        raise RuntimeError(f"{source_name} 返回空 DataFrame")

    frame = raw.copy()
    columns = [str(column) for column in frame.columns]
    code_column = _find_column(columns, ("代码",))
    name_column = _find_column(columns, ("名称",))
    price_column = _find_column(columns, ("最新价",))
    change_column = _find_column(columns, ("涨跌幅",))
    if code_column is None or price_column is None or change_column is None:
        raise RuntimeError(f"{source_name} 缺少代码、最新价或涨跌幅列")

    code_text = frame[code_column].astype(str)
    matched = frame.loc[
        code_text.str.contains(quote_code, case=False, regex=False)
    ]
    if matched.empty and name_column is not None:
        name_text = frame[name_column].astype(str)
        matched = frame.loc[
            name_text.str.contains(quote_name, case=False, regex=False)
        ]
    if matched.empty:
        raise RuntimeError(
            f"{source_name} 未找到指数 {quote_code}/{quote_name}"
        )

    row = matched.iloc[-1]
    price = pd.to_numeric(pd.Series([row[price_column]]), errors="coerce").iloc[0]
    change = pd.to_numeric(pd.Series([row[change_column]]), errors="coerce").iloc[0]
    if pd.isna(price):
        raise RuntimeError(f"{source_name} 的指数 {quote_code} 最新价为空")

    return {
        "index_code": str(row[code_column]),
        "index_name": str(row[name_column]) if name_column else quote_name,
        "price": float(price),
        "change_pct": None if pd.isna(change) else float(change),
        "price_source": source_name,
    }


def _extract_index_quote_from_history(
    pd: Any,
    raw: Any,
    code: str,
    name: str,
    source_name: str,
) -> dict[str, Any]:
    """Use the latest CSI history row when Sina does not include an index."""
    frame = _prepare_frame(pd, raw, f"{name} 历史行情")
    columns = [str(column) for column in frame.columns]
    close_column = _find_column(columns, ("收盘", "最新价"))
    change_column = _find_column(columns, ("涨跌幅",))
    if close_column is None or change_column is None:
        raise RuntimeError(f"{source_name} 缺少收盘价或涨跌幅列")

    row = frame.iloc[-1]
    price = pd.to_numeric(pd.Series([row[close_column]]), errors="coerce").iloc[0]
    change = pd.to_numeric(pd.Series([row[change_column]]), errors="coerce").iloc[0]
    if pd.isna(price):
        raise RuntimeError(f"{source_name} 的指数 {code} 最新收盘价为空")

    return {
        "index_code": code,
        "index_name": name,
        "price": float(price),
        "change_pct": None if pd.isna(change) else float(change),
        "price_source": source_name,
    }


def _call_data_source(
    label: str,
    call: Callable[[], Any],
    result_key: str,
    results: dict[str, Any],
    errors: list[tuple[str, str]],
) -> None:
    """Make one controlled call and record its result or error."""
    print(label)
    try:
        results[result_key] = call()
    except Exception as exc:
        errors.append((label, str(exc)))


def _wait_between_calls() -> None:
    print(f"等待 {CALL_INTERVAL_SECONDS} 秒，避免连续请求...")
    time.sleep(CALL_INTERVAL_SECONDS)


def _build_index_metrics(
    pd: Any,
    history_raw: Any,
    index_quote: dict[str, Any],
    index_name: str,
    pb_raw: Any | None = None,
) -> dict[str, Any]:
    """Build daily PE metrics, with PB only when a valid source exists."""
    history_frame = _prepare_frame(pd, history_raw, f"{index_name} 历史行情")
    pe_column = _find_column(
        [str(column) for column in history_frame.columns],
        ("滚动市盈率", "市盈率PE(TTM)", "PE(TTM)", "市盈率"),
    )
    if pe_column is None:
        raise RuntimeError(
            f"{index_name} 未找到滚动市盈率列，实际列名: "
            f"{[str(column) for column in history_frame.columns]}"
        )

    pe_data = _daily_series(pd, history_frame, pe_column)
    latest_date = pe_data["_date"].iloc[-1]
    current_pe = float(pe_data["_value"].iloc[-1])
    window_start = latest_date - pd.DateOffset(years=5)
    pe_window = pe_data.loc[pe_data["_date"] >= window_start]
    frequency, median_gap_days = _infer_frequency(pd, pe_window["_date"])

    percentile = (
        round(_percentile(pe_window["_value"], current_pe), 2)
        if len(pe_window) >= MIN_DAILY_PERCENTILE_SAMPLES
        else None
    )
    percentile_status = (
        "available"
        if percentile is not None
        else (
            f"insufficient daily history: required "
            f"{MIN_DAILY_PERCENTILE_SAMPLES}, got {len(pe_window)}"
        )
    )

    result: dict[str, Any] = {
        **index_quote,
        "pe": round(current_pe, 4),
        "pe_field": pe_column,
        "pe_date": latest_date.date().isoformat(),
        "earnings_yield_pct": round(100.0 / current_pe, 4),
        "pe_percentile_5y_daily": percentile,
        "pe_window_samples": int(len(pe_window)),
        "pe_window_start": pe_window["_date"].min().date().isoformat(),
        "pe_observed_frequency": frequency,
        "pe_median_gap_days": (
            round(median_gap_days, 2) if median_gap_days is not None else None
        ),
        "pe_history_rows": int(len(history_frame)),
        "pe_history_start": history_frame["_date"].min().date().isoformat(),
        "pe_percentile_status": percentile_status,
        "pe_source": "AKShare / stock_zh_index_hist_csindex",
    }

    if pb_raw is None:
        result.update(
            {
                "pb": None,
                "pb_field": None,
                "pb_date": None,
                "pb_status": "unavailable: 当前测试未配置该指数的 PB 来源",
            }
        )
        return result

    try:
        pb_frame = _prepare_frame(pd, pb_raw, f"{index_name} PB")
        pb_column = _find_column(
            [str(column) for column in pb_frame.columns],
            ("市净率", "等权市净率"),
        )
        if pb_column is None:
            raise RuntimeError(f"{index_name} 未找到市净率列")
        pb_data = _daily_series(pd, pb_frame, pb_column)
        result.update(
            {
                "pb": round(float(pb_data["_value"].iloc[-1]), 4),
                "pb_field": pb_column,
                "pb_date": pb_data["_date"].iloc[-1].date().isoformat(),
                "pb_status": "available",
            }
        )
    except Exception as exc:
        result.update(
            {
                "pb": None,
                "pb_field": None,
                "pb_date": None,
                "pb_status": f"unavailable: {exc}",
            }
        )

    return result


def main() -> int:
    _configure_console()
    started = datetime.now()
    print("AKShare 数据源验证")
    print("测试指数: 沪深300、红利低波、红利指数(000922)")
    print("本脚本每次最多调用五个非东财接口，不重试。")
    print("必须使用项目虚拟环境运行。")
    print(f"开始时间: {started:%Y-%m-%d %H:%M:%S}")
    print()

    ak, pd = _import_dependencies()
    raw_results: dict[str, Any] = {}
    call_errors: list[tuple[str, str]] = []
    warnings: list[str] = []
    history_start = datetime.now() - timedelta(days=INDEX_HISTORY_YEARS * 366)
    history_start_text = history_start.strftime("%Y%m%d")
    history_end_text = datetime.now().strftime("%Y%m%d")

    _call_data_source(
        "[1/5] 调用 AKShare: stock_zh_index_spot_sina()",
        lambda: ak.stock_zh_index_spot_sina(),
        "index_quotes",
        raw_results,
        call_errors,
    )
    _wait_between_calls()

    for index_number, case in enumerate(INDEX_CASES, start=2):
        _call_data_source(
            f"[{index_number}/5] 调用 AKShare: stock_zh_index_hist_csindex("
            f"symbol='{case['code']}', start_date='{history_start_text}', "
            f"end_date='{history_end_text}')",
            lambda case=case: ak.stock_zh_index_hist_csindex(
                symbol=case["code"],
                start_date=history_start_text,
                end_date=history_end_text,
            ),
            f"history_{case['code']}",
            raw_results,
            call_errors,
        )
        _wait_between_calls()

    _call_data_source(
        "[5/5] 调用 AKShare: stock_index_pb_lg(symbol='沪深300')",
        lambda: ak.stock_index_pb_lg(symbol="沪深300"),
        "hs300_pb",
        raw_results,
        call_errors,
    )

    metrics: list[tuple[str, dict[str, Any]]] = []

    for case in INDEX_CASES:
        history_key = f"history_{case['code']}"
        if history_key not in raw_results:
            continue

        quote: dict[str, Any] | None = None
        if "index_quotes" in raw_results:
            try:
                quote = _extract_index_quote(
                    pd,
                    raw_results["index_quotes"],
                    case["quote_code"],
                    case["quote_name"],
                    "AKShare / stock_zh_index_spot_sina",
                )
            except Exception:
                quote = None

        if quote is None:
            try:
                quote = _extract_index_quote_from_history(
                    pd,
                    raw_results[history_key],
                    case["code"],
                    case["name"],
                    "AKShare / stock_zh_index_hist_csindex 后备行情",
                )
                warnings.append(
                    f"{case['name']} 未被 stock_zh_index_spot_sina 收录，"
                    "已使用 stock_zh_index_hist_csindex 最新记录作为行情后备"
                )
            except Exception as exc:
                call_errors.append((f"{case['name']} 指数行情提取", str(exc)))
                continue

        try:
            pb_raw = raw_results.get("hs300_pb") if case["code"] == "000300" else None
            item = _build_index_metrics(
                pd,
                raw_results[history_key],
                quote,
                case["name"],
                pb_raw=pb_raw,
            )
            metrics.append((case["name"], item))
            if item["pe_percentile_5y_daily"] is None:
                warnings.append(
                    f"{case['name']} 逐日历史不足，PE 百分位不可用: "
                    f"{item['pe_percentile_status']}"
                )
            if item["pb"] is None:
                warnings.append(
                    f"{case['name']} PB 不可用: {item['pb_status']}"
                )
        except Exception as exc:
            call_errors.append((f"{case['name']} 指标计算", str(exc)))

    print()
    print("=" * 88)
    for title, item in metrics:
        print(f"{title}:")
        for key, value in item.items():
            print(f"  {key}: {value}")
        print()

    if call_errors:
        print("失败项:")
        for title, message in call_errors:
            print(f"  {title}: {message}")
        print()

    if warnings:
        print("验证警告:")
        for message in warnings:
            print(f"  {message}")
        print()

    elapsed = (datetime.now() - started).total_seconds()
    print(f"本次运行耗时: {elapsed:.2f} 秒")
    print("重要: 不要连续运行本脚本，失败后也不要立即重试。")
    if call_errors:
        return 1
    return 2 if warnings else 0


if __name__ == "__main__":
    sys.exit(main())
