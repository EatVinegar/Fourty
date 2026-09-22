"""SQLite persistence helpers."""

from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

import pandas as pd


def _resolve_path(db_path: str | Path) -> Path:
    return Path(db_path).expanduser().resolve()


def connect(db_path: str | Path) -> sqlite3.Connection:
    """Open one SQLite connection with local-app-safe pragmas."""
    path = _resolve_path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=5.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout = 5000")
    connection.execute("PRAGMA journal_mode = WAL")
    connection.execute("PRAGMA synchronous = NORMAL")
    return connection


def init_db(db_path: str | Path) -> None:
    """Create the schema if it does not exist."""
    schema_path = Path(__file__).with_name("schema.sql")
    schema = schema_path.read_text(encoding="utf-8")
    with connect(db_path) as connection:
        connection.executescript(schema)


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def save_index_history(
    index_key: str,
    frame: pd.DataFrame,
    db_path: str | Path,
) -> int:
    """Upsert normalized daily index history."""
    if frame.empty:
        return 0

    fetched_at = _now()
    rows = [
        (
            index_key,
            pd.Timestamp(row.trade_date).date().isoformat(),
            None if pd.isna(row.close) else float(row.close),
            None if pd.isna(row.change_pct) else float(row.change_pct),
            None if pd.isna(row.pe_ttm) else float(row.pe_ttm),
            "stock_zh_index_hist_csindex",
            fetched_at,
        )
        for row in frame.itertuples(index=False)
    ]
    with connect(db_path) as connection:
        connection.executemany(
            """
            INSERT INTO index_history (
                index_key, trade_date, close, change_pct, pe_ttm,
                source, fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(index_key, trade_date) DO UPDATE SET
                close = excluded.close,
                change_pct = excluded.change_pct,
                pe_ttm = excluded.pe_ttm,
                source = excluded.source,
                fetched_at = excluded.fetched_at
            """,
            rows,
        )
    return len(rows)


def load_index_history(
    index_key: str,
    db_path: str | Path,
) -> pd.DataFrame:
    """Load one index history ordered by date."""
    with connect(db_path) as connection:
        frame = pd.read_sql_query(
            """
            SELECT trade_date, close, change_pct, pe_ttm, source
            FROM index_history
            WHERE index_key = ?
            ORDER BY trade_date
            """,
            connection,
            params=(index_key,),
        )
    if not frame.empty:
        frame["trade_date"] = pd.to_datetime(frame["trade_date"])
    return frame


def save_index_snapshot(
    snapshot: Mapping[str, Any],
    db_path: str | Path,
) -> None:
    """Upsert the latest dashboard record for one index."""
    fields = (
        "index_key",
        "index_name",
        "csi_symbol",
        "history_date",
        "history_close",
        "history_change_pct",
        "price",
        "change_pct",
        "quote_source",
        "pe_ttm",
        "earnings_yield_pct",
        "pe_percentile_5y",
        "pe_window_5y_samples",
        "pe_window_5y_start",
        "pe_percentile_10y",
        "pe_window_10y_samples",
        "pe_window_10y_start",
        "pe_latest_date",
        "source",
    )
    values = [snapshot.get(field) for field in fields]
    values.append(_now())
    placeholders = ", ".join("?" for _ in range(len(fields) + 1))
    columns = ", ".join((*fields, "fetched_at"))
    updates = ", ".join(
        f"{field} = excluded.{field}"
        for field in fields
        if field != "index_key"
    )
    with connect(db_path) as connection:
        connection.execute(
            f"""
            INSERT INTO index_latest ({columns})
            VALUES ({placeholders})
            ON CONFLICT(index_key) DO UPDATE SET
                {updates},
                fetched_at = excluded.fetched_at
            """,
            values,
        )


def load_index_snapshots(db_path: str | Path) -> pd.DataFrame:
    """Load all latest index snapshots."""
    with connect(db_path) as connection:
        return pd.read_sql_query(
            """
            SELECT *
            FROM index_latest
            ORDER BY CASE index_key
                WHEN 'hs300' THEN 1
                WHEN 'dividend_lv' THEN 2
                WHEN 'csi_dividend' THEN 3
                WHEN 'sse50' THEN 4
                ELSE 99
            END
            """,
            connection,
        )


def save_bond_yield(
    frame: pd.DataFrame,
    db_path: str | Path,
) -> int:
    """Upsert daily China 10-year government bond yields."""
    if frame.empty:
        return 0

    fetched_at = _now()
    rows = [
        (
            pd.Timestamp(row.trade_date).date().isoformat(),
            float(row.yield_10y),
            "stock_bond_gb_zh_sina",
            fetched_at,
        )
        for row in frame.itertuples(index=False)
    ]
    with connect(db_path) as connection:
        connection.executemany(
            """
            INSERT INTO bond_yield_daily (
                trade_date, yield_10y, source, fetched_at
            ) VALUES (?, ?, ?, ?)
            ON CONFLICT(trade_date) DO UPDATE SET
                yield_10y = excluded.yield_10y,
                source = excluded.source,
                fetched_at = excluded.fetched_at
            """,
            rows,
        )
    return len(rows)


def load_latest_bond_yield(db_path: str | Path) -> dict[str, Any] | None:
    """Return the latest stored 10-year bond yield."""
    with connect(db_path) as connection:
        row = connection.execute(
            """
            SELECT trade_date, yield_10y, source, fetched_at
            FROM bond_yield_daily
            ORDER BY trade_date DESC
            LIMIT 1
            """
        ).fetchone()
    return dict(row) if row is not None else None


def set_meta(key: str, value: str, db_path: str | Path) -> None:
    """Upsert one metadata value."""
    with connect(db_path) as connection:
        connection.execute(
            """
            INSERT INTO app_meta (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )


def get_meta(key: str, db_path: str | Path) -> str | None:
    """Read one metadata value."""
    with connect(db_path) as connection:
        row = connection.execute(
            "SELECT value FROM app_meta WHERE key = ?",
            (key,),
        ).fetchone()
    return str(row["value"]) if row is not None else None


def save_strategy_settings(
    index_key: str,
    initial_amount: float,
    initial_index_price: float,
    strategy_start_date: str,
    db_path: str | Path,
) -> None:
    """Save per-index strategy settings."""
    with connect(db_path) as connection:
        connection.execute(
            """
            INSERT INTO index_strategy_settings (
                index_key, initial_amount, initial_index_price,
                strategy_start_date, updated_at
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(index_key) DO UPDATE SET
                initial_amount = excluded.initial_amount,
                initial_index_price = excluded.initial_index_price,
                strategy_start_date = excluded.strategy_start_date,
                updated_at = excluded.updated_at
            """,
            (
                index_key,
                float(initial_amount),
                float(initial_index_price),
                strategy_start_date,
                _now(),
            ),
        )


def load_strategy_settings(db_path: str | Path) -> pd.DataFrame:
    """Load per-index strategy settings."""
    with connect(db_path) as connection:
        return pd.read_sql_query(
            """
            SELECT index_key, initial_amount, initial_index_price,
                   strategy_start_date, updated_at
            FROM index_strategy_settings
            ORDER BY index_key
            """,
            connection,
        )
