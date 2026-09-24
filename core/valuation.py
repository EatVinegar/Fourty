"""Valuation orchestration and dashboard data access."""

from __future__ import annotations

import time
import json
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

import pandas as pd
import yaml

from core import datasources, lake, strategy_ey, strategy_pct
from core.logging_utils import get_logger, log_exception
from db import store


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = ROOT / "config.yaml"
INTRADAY_QUOTES_PATH = ROOT / "data" / "lake" / "meta" / "intraday_quotes.json"
LOGGER = get_logger("fourty.valuation")


def load_config(path: str | Path | None = None) -> dict[str, Any]:
    """Load the project YAML configuration."""
    config_path = Path(path) if path is not None else DEFAULT_CONFIG_PATH
    with config_path.open("r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    if not isinstance(config, dict):
        raise RuntimeError("config.yaml 顶层必须是对象")
    return config


def _db_path(config: Mapping[str, Any]) -> Path:
    path = Path(str(config["app"]["db_path"]))
    return path if path.is_absolute() else ROOT / path


def initialize_database(config: Mapping[str, Any] | None = None) -> Path:
    """Create the SQLite schema and return its path."""
    active_config = config or load_config()
    db_path = _db_path(active_config)
    store.init_db(db_path)
    return db_path


def get_data_lake_status() -> dict[str, Any]:
    """Return local data-lake initialization and freshness status."""
    return lake.lake_status()


def _write_intraday_quotes(payload: dict[str, Any]) -> None:
    path = INTRADAY_QUOTES_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _read_intraday_quotes() -> dict[str, Any]:
    path = INTRADAY_QUOTES_PATH
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    return payload if isinstance(payload, dict) else {}


def refresh_intraday_quotes(
    config: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Fetch intraday quotes without modifying curated daily history."""
    active_config = config or load_config()
    quote_frames: list[pd.DataFrame] = []
    errors: dict[str, str] = {}
    for source_name, fetch in (
        ("CN", datasources.fetch_index_quotes),
        ("HK", datasources.fetch_hk_index_quotes),
    ):
        try:
            quote_frames.append(fetch())
        except Exception as exc:
            errors[source_name] = str(exc)
            log_exception(LOGGER, f"intraday quote failed source={source_name}", exc)

    if not quote_frames:
        return {"quotes": {}, "errors": errors}

    quotes = pd.concat(quote_frames, ignore_index=True)
    captured_at = datetime.now().isoformat(timespec="seconds")
    result: dict[str, Any] = {}
    for index_key in active_config["indexes"]:
        quote = datasources.match_index_quote(index_key, quotes)
        if quote is None:
            continue
        result[index_key] = {
            "price": float(quote["price"]),
            "change_pct": (
                None
                if pd.isna(quote["change_pct"])
                else float(quote["change_pct"])
            ),
            "quote_code": str(quote["quote_code"]),
            "quote_name": str(quote["quote_name"]),
        }

    payload = {
        "captured_at": captured_at,
        "trade_date": datetime.now().date().isoformat(),
        "quotes": result,
    }
    _write_intraday_quotes(payload)
    return {"quotes": result, "errors": errors, **payload}


def refresh_all_data(
    config: Mapping[str, Any] | None = None,
    mode: str = "incremental",
    index_keys: list[str] | None = None,
    progress_callback: Any | None = None,
) -> dict[str, Any]:
    """Build the local data lake and refresh the 10-year bond yield."""
    active_config = config or load_config()
    db_path = initialize_database(active_config)
    interval = float(active_config["app"].get("request_interval_seconds", 5))
    errors: dict[str, str] = {}
    saved: dict[str, Any] = {}

    def forward_progress(event: dict[str, Any]) -> None:
        if progress_callback is None:
            return
        forwarded = dict(event)
        forwarded["fraction"] = min(0.9, float(event["fraction"]) * 0.9)
        progress_callback(forwarded)

    LOGGER.info("refresh start mode=%s index_keys=%s", mode, index_keys)
    lake_result = lake.build_lake(
        mode=mode,
        index_keys=index_keys,
        progress_callback=forward_progress,
    )
    saved["lake"] = lake_result

    try:
        if interval > 0:
            time.sleep(interval)
        if progress_callback is not None:
            progress_callback(
                {
                    "phase": "bond",
                    "completed": 0,
                    "total": 1,
                    "fraction": 0.95,
                    "message": "正在更新 10 年期国债收益率",
                    "index_key": "bond_10y",
                    "rows": None,
                    "error": None,
                }
            )
        bond_frame = datasources.fetch_cn_10y_yield()
        store.save_bond_yield(bond_frame, db_path)
        saved["bond_10y"] = {
            "trade_date": pd.Timestamp(
                bond_frame.iloc[-1]["trade_date"]
            ).date().isoformat(),
            "yield_10y": float(bond_frame.iloc[-1]["yield_10y"]),
            "source": "AKShare / bond_gb_zh_sina",
        }
    except Exception as exc:
        errors["bond_10y"] = str(exc)
        log_exception(LOGGER, "bond yield refresh failed", exc)

    try:
        if progress_callback is not None:
            progress_callback(
                {
                    "phase": "intraday",
                    "completed": 0,
                    "total": 1,
                    "fraction": 0.98,
                    "message": "正在更新盘中行情",
                    "index_key": None,
                    "rows": None,
                    "error": None,
                }
            )
        intraday = refresh_intraday_quotes(active_config)
        saved["intraday"] = intraday
        errors.update(intraday.get("errors", {}))
    except Exception as exc:
        errors["intraday"] = str(exc)
        log_exception(LOGGER, "intraday quote refresh failed", exc)

    store.set_meta(
        "last_refresh",
        datetime.now().isoformat(timespec="seconds"),
        db_path,
    )
    if progress_callback is not None:
        progress_callback(
            {
                "phase": "done",
                "completed": 1,
                "total": 1,
                "fraction": 1.0,
                "message": "数据更新完成",
                "index_key": None,
                "rows": None,
                "error": None,
            }
        )
    LOGGER.info("refresh finish errors=%s", errors)
    return {
        "db_path": str(db_path),
        "saved": saved,
        "errors": errors,
    }


def load_dashboard_data(
    config: Mapping[str, Any] | None = None,
) -> tuple[pd.DataFrame, dict[str, Any] | None, str | None]:
    """Load dashboard data from the local lake and latest bond yield."""
    active_config = config or load_config()
    db_path = initialize_database(active_config)
    if not lake.is_initialized():
        return pd.DataFrame(), store.load_latest_bond_yield(db_path), store.get_meta(
            "last_refresh", db_path
        )

    snapshots, history_map = _build_lake_snapshots(active_config, db_path)
    snapshots = _enrich_snapshots(
        snapshots,
        history_map,
        active_config,
        db_path,
    )
    bond = store.load_latest_bond_yield(db_path)
    last_refresh = store.get_meta("last_refresh", db_path)
    return snapshots, bond, last_refresh


def load_index_detail(
    index_key: str,
    config: Mapping[str, Any] | None = None,
) -> tuple[dict[str, Any] | None, pd.DataFrame]:
    """Load one lake-backed index snapshot and full daily history."""
    active_config = config or load_config()
    db_path = initialize_database(active_config)
    if not lake.is_initialized():
        return None, pd.DataFrame()

    snapshots, history_map = _build_lake_snapshots(active_config, db_path)
    snapshots = _enrich_snapshots(
        snapshots,
        history_map,
        active_config,
        db_path,
    )
    snapshot = None
    if not snapshots.empty:
        matched = snapshots.loc[snapshots["index_key"] == index_key]
        if not matched.empty:
            snapshot = matched.iloc[0].to_dict()
    return snapshot, history_map.get(index_key, pd.DataFrame())


def _settings_map(settings: pd.DataFrame) -> dict[str, dict[str, Any]]:
    if settings.empty:
        return {}
    return {
        str(row.index_key): row._asdict()
        for row in settings.itertuples(index=False)
    }


def _build_lake_snapshots(
    config: Mapping[str, Any],
    db_path: Path,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    """Build dashboard snapshots directly from curated lake data."""
    ui_keys = list(config["indexes"])
    daily = lake.load_daily(ui_keys)
    history_map: dict[str, pd.DataFrame] = {}
    rows: list[dict[str, Any]] = []

    for index_key in ui_keys:
        group = daily.loc[daily["index_key"] == index_key].copy()
        if group.empty:
            continue

        group["trade_date"] = pd.to_datetime(group["trade_date"])
        for column in (
            "open",
            "high",
            "low",
            "close",
            "change_pct",
            "volume",
            "amount",
            "sample_count",
            "pe_ttm",
        ):
            if column in group.columns:
                group[column] = pd.to_numeric(group[column], errors="coerce")
        group = group.sort_values("trade_date").reset_index(drop=True)
        history_map[index_key] = group

        latest = group.iloc[-1]
        index_config = config["indexes"][index_key]
        has_pe = bool(index_config.get("has_pe", True))
        pe_ttm = None
        earnings_yield = None
        pe_latest_date = None
        pe_windows: dict[str, Any] = {}

        if has_pe:
            valid_pe = group.loc[group["pe_ttm"] > 0].copy()
            if not valid_pe.empty:
                latest_pe = valid_pe.iloc[-1]
                pe_ttm = float(latest_pe["pe_ttm"])
                earnings_yield = strategy_ey.calculate_earnings_yield(pe_ttm)
                pe_latest_date = latest_pe["trade_date"].date().isoformat()
                pe_windows = strategy_pct.calculate_percentile_windows(
                    valid_pe,
                    windows=(5, 10),
                    min_samples=int(
                        config["valuation"]["min_percentile_samples"]
                    ),
                )

        rows.append(
            {
                "index_key": index_key,
                "index_name": index_config["name"],
                "csi_symbol": index_config["csi_symbol"],
                "history_date": latest["trade_date"].date().isoformat(),
                "history_close": float(latest["close"]),
                "history_change_pct": (
                    None
                    if pd.isna(latest["change_pct"])
                    else float(latest["change_pct"])
                ),
                "price": float(latest["close"]),
                "change_pct": (
                    None
                    if pd.isna(latest["change_pct"])
                    else float(latest["change_pct"])
                ),
                "quote_source": "data_lake/curated/index_daily",
                "quote_time": None,
                "pe_ttm": pe_ttm,
                "earnings_yield_pct": earnings_yield,
                "pe_percentile_5y": pe_windows.get("pe_percentile_5y"),
                "pe_window_5y_samples": pe_windows.get(
                    "pe_window_5y_samples",
                    0,
                ),
                "pe_window_5y_start": pe_windows.get("pe_window_5y_start"),
                "pe_percentile_10y": pe_windows.get("pe_percentile_10y"),
                "pe_window_10y_samples": pe_windows.get(
                    "pe_window_10y_samples",
                    0,
                ),
                "pe_window_10y_start": pe_windows.get(
                    "pe_window_10y_start"
                ),
                "pe_latest_date": pe_latest_date,
                "source": "AKShare/data_lake",
            }
        )

    snapshots = pd.DataFrame(rows)
    overlay = _read_intraday_quotes()
    if not snapshots.empty and overlay:
        overlay_date = pd.Timestamp(
            overlay.get("trade_date") or overlay.get("captured_at")
        ).normalize()
        overlay_quotes = overlay.get("quotes", {})
        for position, row in snapshots.iterrows():
            quote = overlay_quotes.get(str(row["index_key"]))
            if not quote:
                continue
            history_date = pd.Timestamp(row["history_date"]).normalize()
            if history_date >= overlay_date:
                continue
            snapshots.at[position, "price"] = float(quote["price"])
            snapshots.at[position, "change_pct"] = quote["change_pct"]
            snapshots.at[position, "quote_source"] = "intraday"
            snapshots.at[position, "quote_time"] = overlay.get("captured_at")
    return snapshots, history_map


def _enrich_snapshots(
    snapshots: pd.DataFrame,
    history_map: Mapping[str, pd.DataFrame],
    config: Mapping[str, Any],
    db_path: Path,
) -> pd.DataFrame:
    """Add current strategy states and theoretical amounts to snapshots."""
    if snapshots.empty:
        return snapshots

    settings_map = _settings_map(store.load_strategy_settings(db_path))
    strategy_config = config["strategies"]
    valuation_config = strategy_config["valuation_percentile"]
    deviation_config = strategy_config["ma_deviation"]
    rows: list[dict[str, Any]] = []

    for snapshot in snapshots.to_dict(orient="records"):
        index_key = str(snapshot["index_key"])
        history = history_map[index_key]
        settings = settings_map.get(index_key)
        enriched = dict(snapshot)

        initial_amount = (
            None if settings is None else float(settings["initial_amount"])
        )
        initial_index_price = (
            None
            if settings is None
            else float(settings["initial_index_price"])
        )
        strategy_start_date = (
            None
            if settings is None
            else str(settings["strategy_start_date"])
        )
        months_elapsed = (
            strategy_ey.calculate_months_elapsed(strategy_start_date)
            if strategy_start_date
            else None
        )

        has_pe = bool(config["indexes"][index_key].get("has_pe", True))
        if has_pe:
            pe_window = strategy_pct.calculate_percentile_windows(
                history,
                windows=(5,),
                min_samples=int(
                    config["valuation"]["min_percentile_samples"]
                ),
            )
            pe_percentile_5y = pe_window.get("pe_percentile_5y")
            valuation_amount = (
                strategy_ey.calculate_valuation_percentile_amount(
                    initial_amount=initial_amount,
                    pe_ttm=snapshot.get("pe_ttm"),
                    pe_percentile=pe_percentile_5y,
                    month_index=months_elapsed or 0,
                    low_percentile=float(
                        valuation_config["low_percentile"]
                    ),
                    high_percentile=float(
                        valuation_config["high_percentile"]
                    ),
                    low_multiplier=float(
                        valuation_config["low_multiplier"]
                    ),
                    middle_multiplier=float(
                        valuation_config["middle_multiplier"]
                    ),
                    inflation_rate=float(
                        valuation_config["inflation_rate"]
                    ),
                )
            )
        else:
            pe_percentile_5y = None
            valuation_amount = {
                "state": "不适用",
                "amplifier": None,
                "inflation_factor": None,
                "amount": None,
            }

        deviation_history = strategy_pct.calculate_ma_deviation_series(
            history,
            window=int(deviation_config["ma_window"]),
        )
        deviation_metrics = (
            strategy_pct.calculate_latest_deviation_percentile(
                deviation_history,
                years=int(deviation_config["percentile_window_years"]),
                min_samples=int(
                    config["valuation"]["min_percentile_samples"]
                ),
            )
        )
        volatility_10 = strategy_pct.calculate_volatility_10(
            history["close"],
            window=int(deviation_config["volatility_window"]),
            annualization_factor=int(
                deviation_config["annualization_factor"]
            ),
        )
        ma_amount = strategy_pct.calculate_ma_deviation_amount(
            initial_amount=initial_amount,
            deviation_percentile=deviation_metrics[
                "deviation_percentile"
            ],
            volatility_pct=volatility_10,
            volatility_tiers=deviation_config["volatility_discounts"],
            low_percentile=float(deviation_config["low_percentile"]),
            high_percentile=float(deviation_config["high_percentile"]),
            low_multiplier=float(deviation_config["low_multiplier"]),
            middle_multiplier=float(
                deviation_config["middle_multiplier"]
            ),
        )

        enriched.update(
            {
                "initial_amount": initial_amount,
                "initial_index_price": initial_index_price,
                "strategy_start_date": strategy_start_date,
                "months_elapsed": months_elapsed,
                "pe_percentile_5y_strategy": pe_percentile_5y,
                "valuation_state": valuation_amount["state"],
                "valuation_amplifier": valuation_amount["amplifier"],
                "valuation_inflation_factor": valuation_amount[
                    "inflation_factor"
                ],
                "valuation_amount": valuation_amount["amount"],
                "ma250": (
                    float(deviation_history["ma"].iloc[-1])
                    if not deviation_history.empty
                    and pd.notna(deviation_history["ma"].iloc[-1])
                    else None
                ),
                "ma_deviation_pct": deviation_metrics["deviation_pct"],
                "ma_deviation_percentile": deviation_metrics[
                    "deviation_percentile"
                ],
                "ma_deviation_samples": deviation_metrics[
                    "deviation_samples"
                ],
                "volatility_10": (
                    round(float(volatility_10), 4)
                    if volatility_10 is not None
                    else None
                ),
                "ma_state": ma_amount["state"],
                "ma_base_multiplier": ma_amount["base_multiplier"],
                "ma_volatility_discount": ma_amount[
                    "volatility_discount"
                ],
                "ma_final_multiplier": ma_amount["final_multiplier"],
                "ma_amount": ma_amount["amount"],
            }
        )
        rows.append(enriched)

    return pd.DataFrame(rows)


def save_index_settings(
    index_key: str,
    initial_amount: float,
    strategy_start_date: str,
    config: Mapping[str, Any] | None = None,
) -> None:
    """Save settings for one index."""
    active_config = config or load_config()
    if index_key not in active_config["indexes"]:
        raise ValueError(f"未知指数: {index_key}")
    if initial_amount <= 0:
        raise ValueError("初始金额必须大于 0")

    db_path = initialize_database(active_config)
    initial_index_price, _ = resolve_strategy_start_price(
        index_key,
        strategy_start_date,
        active_config,
    )
    store.save_strategy_settings(
        index_key=index_key,
        initial_amount=initial_amount,
        initial_index_price=initial_index_price,
        strategy_start_date=strategy_start_date,
        db_path=db_path,
    )


def resolve_strategy_start_price(
    index_key: str,
    strategy_start_date: str,
    config: Mapping[str, Any] | None = None,
) -> tuple[float, str]:
    """Resolve the latest stored close on or before strategy start date."""
    active_config = config or load_config()
    if index_key not in active_config["indexes"]:
        raise ValueError(f"未知指数: {index_key}")

    initialize_database(active_config)
    if not lake.is_initialized():
        raise RuntimeError("数据湖尚未初始化，无法确定初始价格")
    history = lake.load_daily([index_key])
    if history.empty:
        raise RuntimeError(f"{index_key} 数据湖中没有历史数据")

    history["trade_date"] = pd.to_datetime(history["trade_date"])
    requested = pd.Timestamp(strategy_start_date).normalize()
    eligible = history.loc[history["trade_date"] <= requested].sort_values(
        "trade_date"
    )
    if eligible.empty:
        earliest = history["trade_date"].min().date().isoformat()
        raise ValueError(
            f"策略起始日期早于该指数最早历史日期 {earliest}"
        )

    row = eligible.iloc[-1]
    return float(row["close"]), row["trade_date"].date().isoformat()


def load_all_index_settings(
    config: Mapping[str, Any] | None = None,
) -> pd.DataFrame:
    """Load settings for all indices in configured display order."""
    active_config = config or load_config()
    db_path = initialize_database(active_config)
    settings = store.load_strategy_settings(db_path)
    order = {key: position for position, key in enumerate(active_config["indexes"])}
    if not settings.empty:
        settings["_order"] = settings["index_key"].map(order).fillna(999)
        settings = settings.sort_values("_order").drop(columns="_order")
    return settings
