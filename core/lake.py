r"""Personal index-fund data-lake demo.

The implementation borrows the following ideas from CNEquity:

- staging -> curated -> derived -> meta
- one canonical row per primary key
- source/data_version/fetched_at provenance
- atomic Parquet writes and run manifests
- quality findings separate from curated data

It deliberately does not import or run CNEquity and does not require its
dependency stack.
"""

from __future__ import annotations

import hashlib
import argparse
import json
import os
import shutil
import sqlite3
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from core import datasources


ROOT = Path(__file__).resolve().parents[1]
LAKE = ROOT / "data" / "lake"
STAGING = LAKE / "staging"
CURATED = LAKE / "curated"
DERIVED = LAKE / "derived"
RAW = LAKE / "raw"
META = LAKE / "meta"
MANIFEST_DB = META / "manifest.db"
CATALOG_DB = LAKE / "catalog.db"
STATE_DIR = META / "state"
QUALITY_DIR = META / "quality"
REQUEST_INTERVAL_SECONDS = 5
START_DATE = "2016-01-01"
DATA_VERSION = f"akshare-{datasources.AKSHARE_VERSION}"
DAILY_COLUMNS = [
    "index_key",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "change_pct",
    "volume",
    "amount",
    "sample_count",
    "pe_ttm",
    "source",
    "data_version",
    "fetched_at",
    "run_id",
]

INDEXES = (
    {
        "index_key": "sse50",
        "symbol": "000016",
        "name": "上证50",
        "market": "CN",
        "category": "broad",
        "source": "stock_zh_index_hist_csindex",
        "fetcher": "csindex",
        "has_pe": True,
    },
    {
        "index_key": "hs300",
        "symbol": "000300",
        "name": "沪深300",
        "market": "CN",
        "category": "broad",
        "source": "stock_zh_index_hist_csindex",
        "fetcher": "csindex",
        "has_pe": True,
    },
    {
        "index_key": "csi_a50",
        "symbol": "930050",
        "name": "中证A50",
        "market": "CN",
        "category": "broad",
        "source": "stock_zh_index_hist_csindex",
        "fetcher": "csindex",
        "has_pe": True,
    },
    {
        "index_key": "csi_a500",
        "symbol": "000510",
        "name": "中证A500",
        "market": "CN",
        "category": "broad",
        "source": "stock_zh_index_hist_csindex",
        "fetcher": "csindex",
        "has_pe": True,
    },
    {
        "index_key": "csi100",
        "symbol": "000903",
        "name": "中证100",
        "market": "CN",
        "category": "broad",
        "source": "stock_zh_index_hist_csindex",
        "fetcher": "csindex",
        "has_pe": True,
    },
    {
        "index_key": "csi800",
        "symbol": "000906",
        "name": "中证800",
        "market": "CN",
        "category": "broad",
        "source": "stock_zh_index_hist_csindex",
        "fetcher": "csindex",
        "has_pe": True,
    },
    {
        "index_key": "csi500",
        "symbol": "000905",
        "name": "中证500",
        "market": "CN",
        "category": "broad",
        "source": "stock_zh_index_hist_csindex",
        "fetcher": "csindex",
        "has_pe": True,
    },
    {
        "index_key": "csi1000",
        "symbol": "000852",
        "name": "中证1000",
        "market": "CN",
        "category": "broad",
        "source": "stock_zh_index_hist_csindex",
        "fetcher": "csindex",
        "has_pe": True,
    },
    {
        "index_key": "csi2000",
        "symbol": "932000",
        "name": "中证2000",
        "market": "CN",
        "category": "broad",
        "source": "stock_zh_index_hist_csindex",
        "fetcher": "csindex",
        "has_pe": True,
    },
    {
        "index_key": "star50",
        "symbol": "000688",
        "name": "科创50",
        "market": "CN",
        "category": "broad",
        "source": "stock_zh_index_hist_csindex",
        "fetcher": "csindex",
        "has_pe": True,
    },
    {
        "index_key": "dividend_lv",
        "symbol": "H30269",
        "name": "红利低波",
        "market": "CN",
        "category": "strategy",
        "source": "stock_zh_index_hist_csindex",
        "fetcher": "csindex",
        "has_pe": True,
    },
    {
        "index_key": "csi_dividend",
        "symbol": "000922",
        "name": "红利指数",
        "market": "CN",
        "category": "strategy",
        "source": "stock_zh_index_hist_csindex",
        "fetcher": "csindex",
        "has_pe": True,
    },
    {
        "index_key": "hsi",
        "symbol": "HSI",
        "name": "恒生指数",
        "market": "HK",
        "category": "broad",
        "source": "stock_hk_index_daily_sina",
        "fetcher": "hk_sina",
        "has_pe": False,
    },
    {
        "index_key": "hstech",
        "symbol": "HSTECH",
        "name": "恒生科技",
        "market": "HK",
        "category": "strategy",
        "source": "stock_hk_index_daily_sina",
        "fetcher": "hk_sina",
        "has_pe": False,
    },
)


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _ensure_layout() -> None:
    for path in (
        STAGING,
        CURATED,
        DERIVED,
        RAW,
        META,
        STATE_DIR,
        QUALITY_DIR,
    ):
        path.mkdir(parents=True, exist_ok=True)


def _atomic_write_parquet(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    frame.to_parquet(temporary, index=False, compression="zstd")
    os.replace(temporary, path)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _dataset_sha256(path: Path) -> str:
    """Hash a file or a deterministic directory manifest."""
    if path.is_file():
        return _sha256(path)
    digest = hashlib.sha256()
    for file_path in sorted(path.rglob("*.parquet")):
        relative = file_path.relative_to(path).as_posix()
        digest.update(relative.encode("utf-8"))
        digest.update(bytes.fromhex(_sha256(file_path)))
    return digest.hexdigest()


def _init_manifest() -> None:
    META.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(MANIFEST_DB) as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS ingestion_runs (
                run_id TEXT PRIMARY KEY,
                mode TEXT NOT NULL DEFAULT 'full',
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                error_message TEXT
            );
            CREATE TABLE IF NOT EXISTS revisions (
                dataset TEXT NOT NULL,
                revision_id TEXT NOT NULL,
                path TEXT NOT NULL,
                sha256 TEXT NOT NULL,
                row_count INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (dataset, revision_id)
            );
            """
        )
        columns = {
            row[1]
            for row in connection.execute(
                "PRAGMA table_info(ingestion_runs)"
            ).fetchall()
        }
        if "mode" not in columns:
            connection.execute(
                "ALTER TABLE ingestion_runs ADD COLUMN mode TEXT NOT NULL DEFAULT 'full'"
            )


def _fetch_csindex(
    case: dict[str, Any],
    run_id: str,
    start_date: str,
) -> pd.DataFrame:
    try:
        raw = datasources.fetch_csindex_history_raw(
            symbol=case["symbol"],
            start_date=start_date.replace("-", ""),
            end_date=datetime.now().strftime("%Y%m%d"),
        )
    except ValueError as exc:
        if "length mismatch" in str(exc).lower():
            return pd.DataFrame(columns=DAILY_COLUMNS)
        raise
    if raw.empty:
        return pd.DataFrame(columns=DAILY_COLUMNS)
    frame = raw.copy()
    frame["_trade_date"] = pd.to_datetime(frame["日期"], errors="coerce")
    frame["_open"] = pd.to_numeric(frame["开盘"], errors="coerce")
    frame["_high"] = pd.to_numeric(frame["最高"], errors="coerce")
    frame["_low"] = pd.to_numeric(frame["最低"], errors="coerce")
    frame["_close"] = pd.to_numeric(frame["收盘"], errors="coerce")
    frame["_change_pct"] = pd.to_numeric(frame["涨跌幅"], errors="coerce")
    frame["_volume"] = pd.to_numeric(frame["成交量"], errors="coerce")
    frame["_amount"] = pd.to_numeric(frame["成交金额"], errors="coerce")
    frame["_sample_count"] = pd.to_numeric(frame["样本数量"], errors="coerce")
    frame["_pe_ttm"] = pd.to_numeric(frame["滚动市盈率"], errors="coerce")
    return _normalize_daily(frame, case, run_id)


def _fetch_hk_sina(
    case: dict[str, Any],
    run_id: str,
    start_date: str,
) -> pd.DataFrame:
    raw = datasources.fetch_hk_index_daily_raw(symbol=case["symbol"])
    if raw.empty:
        raise RuntimeError(f"{case['name']} 返回空数据")
    frame = raw.copy()
    frame["_trade_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["_open"] = pd.to_numeric(frame["open"], errors="coerce")
    frame["_high"] = pd.to_numeric(frame["high"], errors="coerce")
    frame["_low"] = pd.to_numeric(frame["low"], errors="coerce")
    frame["_close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame = frame.sort_values("_trade_date")
    frame["_change_pct"] = frame["_close"].pct_change() * 100.0
    frame = frame.loc[
        frame["_trade_date"] >= pd.Timestamp(start_date)
    ].copy()
    frame["_volume"] = pd.to_numeric(frame["volume"], errors="coerce")
    frame["_amount"] = pd.to_numeric(frame["amount"], errors="coerce")
    frame["_sample_count"] = float("nan")
    frame["_pe_ttm"] = float("nan")
    return _normalize_daily(frame, case, run_id)


def _normalize_daily(
    frame: pd.DataFrame,
    case: dict[str, Any],
    run_id: str,
) -> pd.DataFrame:
    result = pd.DataFrame(
        {
            "index_key": case["index_key"],
            "trade_date": frame["_trade_date"].dt.date.astype("string"),
            "open": frame["_open"],
            "high": frame["_high"],
            "low": frame["_low"],
            "close": frame["_close"],
            "change_pct": frame["_change_pct"],
            "volume": frame["_volume"],
            "amount": frame["_amount"],
            "sample_count": frame["_sample_count"],
            "pe_ttm": frame["_pe_ttm"],
            "source": case["source"],
            "data_version": DATA_VERSION,
            "fetched_at": _now(),
            "run_id": run_id,
        }
    )
    result = result.dropna(subset=["trade_date"]).sort_values("trade_date")
    result = result.drop_duplicates(["index_key", "trade_date"], keep="last")
    return result.reset_index(drop=True)


def _build_master(case: dict[str, Any], run_id: str) -> dict[str, Any]:
    return {
        "index_key": case["index_key"],
        "symbol": case["symbol"],
        "index_name": case["name"],
        "market": case["market"],
        "category": case["category"],
        "has_pe": int(case["has_pe"]),
        "source": case["source"],
        "data_version": DATA_VERSION,
        "fetched_at": _now(),
        "run_id": run_id,
    }


def _write_staging(
    dataset: str,
    run_id: str,
    case_key: str,
    frame: pd.DataFrame,
) -> Path:
    path = (
        STAGING
        / dataset
        / f"run_id={run_id}"
        / f"part-{case_key}.parquet"
    )
    _atomic_write_parquet(frame, path)
    return path


def _compact_unpartitioned(
    dataset: str,
    run_id: str,
    primary_key: list[str],
) -> Path:
    staged_dir = STAGING / dataset / f"run_id={run_id}"
    staged_files = sorted(staged_dir.glob("*.parquet"))
    if not staged_files:
        raise RuntimeError(f"{dataset} 没有 staging 文件")
    staged = pd.concat(
        [pd.read_parquet(path) for path in staged_files],
        ignore_index=True,
    )
    output = CURATED / dataset / "part-merged.parquet"
    if output.exists():
        staged = pd.concat(
            [pd.read_parquet(output), staged],
            ignore_index=True,
        )
    staged = (
        staged.sort_values("fetched_at")
        .drop_duplicates(primary_key, keep="last")
        .reset_index(drop=True)
    )
    _atomic_write_parquet(staged, output)
    shutil.rmtree(staged_dir, ignore_errors=True)
    return output


def _compact_daily(run_id: str) -> Path:
    dataset = "index_daily"
    staged_dir = STAGING / dataset / f"run_id={run_id}"
    staged_files = sorted(staged_dir.glob("*.parquet"))
    if not staged_files:
        raise RuntimeError("index_daily 没有 staging 文件")
    staged = pd.concat(
        [pd.read_parquet(path) for path in staged_files],
        ignore_index=True,
    )
    staged["_year"] = pd.to_datetime(staged["trade_date"]).dt.year

    written: list[Path] = []
    for year, group in staged.groupby("_year", sort=True):
        output = (
            CURATED
            / dataset
            / f"year={int(year)}"
            / "part-merged.parquet"
        )
        combined = group.drop(columns="_year")
        if output.exists():
            combined = pd.concat(
                [pd.read_parquet(output), combined],
                ignore_index=True,
            )
        combined = (
            combined.sort_values("fetched_at")
            .drop_duplicates(["index_key", "trade_date"], keep="last")
            .reset_index(drop=True)
        )
        _atomic_write_parquet(combined, output)
        written.append(output)
    shutil.rmtree(staged_dir, ignore_errors=True)
    return CURATED / dataset


def _load_curated(dataset: str) -> pd.DataFrame:
    files = sorted((CURATED / dataset).rglob("*.parquet"))
    if not files:
        raise RuntimeError(f"curated/{dataset} 为空")
    return pd.concat([pd.read_parquet(path) for path in files], ignore_index=True)


def is_initialized() -> bool:
    """Return whether the curated daily dataset has been initialized."""
    return bool(sorted((CURATED / "index_daily").rglob("*.parquet")))


def load_index_master() -> pd.DataFrame:
    """Load curated index metadata."""
    return _load_curated("index_master")


def load_daily(index_keys: list[str] | None = None) -> pd.DataFrame:
    """Load curated daily data, optionally filtered by index keys."""
    frame = _load_curated("index_daily")
    if index_keys is not None:
        frame = frame.loc[frame["index_key"].isin(index_keys)].copy()
    frame = frame.sort_values(["index_key", "trade_date"]).reset_index(drop=True)

    # Repair empty incremental change values for HK indices from adjacent
    # curated closes without requiring another full Sina download.
    hk_mask = frame["source"].eq("stock_hk_index_daily_sina")
    if hk_mask.any():
        for _, indexes in frame.loc[hk_mask].groupby("index_key").groups.items():
            group = frame.loc[indexes]
            calculated = group["close"].pct_change() * 100.0
            frame.loc[indexes, "change_pct"] = group["change_pct"].fillna(
                calculated
            )
    return frame


def load_metrics() -> pd.DataFrame:
    """Load the latest derived metrics."""
    path = DERIVED / "index_metrics" / "part-merged.parquet"
    if not path.is_file():
        return pd.DataFrame()
    return pd.read_parquet(path)


def lake_status() -> dict[str, Any]:
    """Return a compact data-lake status summary."""
    initialized = is_initialized()
    status: dict[str, Any] = {
        "initialized": initialized,
        "lake_root": str(LAKE),
        "index_count": 0,
        "row_count": 0,
        "latest_trade_date": None,
        "last_run_id": None,
        "last_run_status": None,
    }
    if not initialized:
        return status

    daily = load_daily()
    status["index_count"] = int(daily["index_key"].nunique())
    status["row_count"] = int(len(daily))
    status["latest_trade_date"] = str(
        pd.to_datetime(daily["trade_date"]).max().date()
    )
    if MANIFEST_DB.is_file():
        with sqlite3.connect(MANIFEST_DB) as connection:
            row = connection.execute(
                """
                SELECT run_id, status
                FROM ingestion_runs
                ORDER BY started_at DESC
                LIMIT 1
                """
            ).fetchone()
        if row is not None:
            status["last_run_id"] = row[0]
            status["last_run_status"] = row[1]
    return status


def _build_derived() -> pd.DataFrame:
    daily = _load_curated("index_daily")
    rows: list[dict[str, Any]] = []
    for index_key, group in daily.groupby("index_key", sort=False):
        frame = group.sort_values("trade_date").copy()
        frame["ma250"] = frame["close"].rolling(250).mean()
        frame["deviation_pct"] = (
            (frame["close"] - frame["ma250"]) / frame["ma250"] * 100.0
        )
        latest = frame.iloc[-1]
        pe_numbers = pd.to_numeric(frame["pe_ttm"], errors="coerce")
        pe_valid = pe_numbers.loc[pe_numbers > 0].dropna()
        pe_percentile = None
        if (
            not pe_valid.empty
            and pd.notna(latest["pe_ttm"])
            and float(latest["pe_ttm"]) > 0
        ):
            window = pe_valid.tail(1250)
            pe_percentile = float(
                (window < float(latest["pe_ttm"])).mean() * 100.0
            )
        rows.append(
            {
                "index_key": index_key,
                "trade_date": latest["trade_date"],
                "close": latest["close"],
                "pe_ttm": latest["pe_ttm"],
                "pe_percentile_5y": pe_percentile,
                "ma250": latest["ma250"],
                "deviation_pct": latest["deviation_pct"],
                "vol10": (
                    frame["close"].pct_change().tail(10).std(ddof=1)
                    * (252**0.5)
                    * 100.0
                ),
                "has_pe": int(pd.notna(latest["pe_ttm"])),
                "source": "derived",
                "data_version": DATA_VERSION,
                "fetched_at": _now(),
            }
        )
    result = pd.DataFrame(rows)
    output = DERIVED / "index_metrics" / "part-merged.parquet"
    _atomic_write_parquet(result, output)
    return result


def _write_raw(
    case: dict[str, Any],
    run_id: str,
    frame: pd.DataFrame,
) -> Path:
    path = (
        RAW
        / f"source={case['source']}"
        / f"index_key={case['index_key']}"
        / f"run_id={run_id}.parquet"
    )
    _atomic_write_parquet(frame, path)
    return path


def _sync_catalog() -> None:
    master = _load_curated("index_master")
    daily = _load_curated("index_daily")
    derived = pd.read_parquet(
        DERIVED / "index_metrics" / "part-merged.parquet"
    )
    latest = (
        daily.sort_values("trade_date")
        .groupby("index_key", as_index=False)
        .tail(1)
    )
    with sqlite3.connect(CATALOG_DB) as connection:
        master.to_sql("index_master", connection, if_exists="replace", index=False)
        latest.to_sql("index_daily_latest", connection, if_exists="replace", index=False)
        derived.to_sql("index_metrics", connection, if_exists="replace", index=False)
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_daily_latest_key "
            "ON index_daily_latest(index_key)"
        )


def _quality_check(
    run_id: str,
    fetched: dict[str, pd.DataFrame],
    cases: tuple[dict[str, Any], ...],
) -> dict[str, Any]:
    findings: list[dict[str, Any]] = []
    non_empty = {
        key: frame for key, frame in fetched.items() if not frame.empty
    }
    if not non_empty:
        payload = {
            "run_id": run_id,
            "created_at": _now(),
            "findings": [
                {
                    "check": "incremental_window",
                    "severity": "info",
                    "message": "没有发现新的交易日数据",
                }
            ],
        }
        (QUALITY_DIR / f"{run_id}.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return payload

    all_daily = pd.concat(non_empty.values(), ignore_index=True)
    duplicates = int(
        all_daily.duplicated(["index_key", "trade_date"], keep=False).sum()
    )
    findings.append(
        {
            "check": "primary_key_unique",
            "severity": "error" if duplicates else "info",
            "message": f"duplicate rows: {duplicates}",
        }
    )
    for case in cases:
        frame = non_empty.get(case["index_key"])
        if frame is None:
            continue
        missing_pe = int(frame["pe_ttm"].isna().sum())
        if not case["has_pe"]:
            findings.append(
                {
                    "check": f"{case['index_key']}_pe_coverage",
                    "severity": "warning",
                    "message": (
                        "Sina 港股指数历史接口不提供 PE，"
                        f"missing rows: {missing_pe}"
                    ),
                }
            )
        elif missing_pe:
            ordered = frame.sort_values("trade_date")
            latest_pe = pd.to_numeric(
                pd.Series([ordered.iloc[-1]["pe_ttm"]]),
                errors="coerce",
            ).iloc[0]
            valid = pd.to_numeric(frame["pe_ttm"], errors="coerce")
            valid_dates = frame.loc[valid > 0, "trade_date"]
            coverage_start = (
                valid_dates.min() if not valid_dates.empty else None
            )
            findings.append(
                {
                    "check": f"{case['index_key']}_pe_coverage",
                    "severity": "warning" if pd.notna(latest_pe) else "error",
                    "message": (
                        f"PE missing rows: {missing_pe}; "
                        f"first valid PE date: {coverage_start}"
                    ),
                }
            )
    payload = {
        "run_id": run_id,
        "created_at": _now(),
        "findings": findings,
    }
    (QUALITY_DIR / f"{run_id}.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return payload


def _record_revision(dataset: str, path: Path, row_count: int) -> None:
    revision_id = uuid.uuid4().hex
    with sqlite3.connect(MANIFEST_DB) as connection:
        connection.execute(
            """
            INSERT INTO revisions (
                dataset, revision_id, path, sha256, row_count, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                dataset,
                revision_id,
                str(path),
                _dataset_sha256(path),
                int(row_count),
                _now(),
            ),
        )


def _read_state() -> dict[str, Any]:
    path = STATE_DIR / "index_daily.json"
    if not path.exists():
        return {"indexes": {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {"indexes": {}}
    indexes = payload.get("indexes")
    if not isinstance(indexes, dict):
        payload["indexes"] = {}
    return payload


def run(
    mode: str = "full",
    index_keys: list[str] | None = None,
) -> dict[str, Any]:
    if mode not in {"full", "incremental"}:
        raise ValueError("mode must be full or incremental")
    _ensure_layout()
    _init_manifest()
    run_id = uuid.uuid4().hex
    started_at = _now()
    previous_state = _read_state()
    selected_cases = tuple(
        case
        for case in INDEXES
        if index_keys is None or case["index_key"] in index_keys
    )
    if not selected_cases:
        raise ValueError("没有匹配的指数")
    with sqlite3.connect(MANIFEST_DB) as connection:
        connection.execute(
            """
            INSERT INTO ingestion_runs (run_id, mode, status, started_at)
            VALUES (?, ?, 'running', ?)
            """,
            (run_id, mode, started_at),
        )

    fetched: dict[str, pd.DataFrame] = {}
    master_rows: list[dict[str, Any]] = []
    errors: dict[str, str] = {}
    try:
        for position, case in enumerate(selected_cases):
            if position:
                time.sleep(REQUEST_INTERVAL_SECONDS)
            index_key = case["index_key"]
            last_date = previous_state["indexes"].get(index_key)
            if mode == "incremental" and last_date:
                start_date = (
                    pd.Timestamp(last_date) + pd.Timedelta(days=1)
                ).date().isoformat()
            else:
                start_date = START_DATE
            print(
                f"fetch {case['name']} ({case['source']}) "
                f"from {start_date}"
            )
            try:
                if case["fetcher"] == "csindex":
                    frame = _fetch_csindex(case, run_id, start_date)
                else:
                    frame = _fetch_hk_sina(case, run_id, start_date)
                fetched[index_key] = frame
                _write_raw(case, run_id, frame)
                if not frame.empty:
                    _write_staging(
                        "index_daily",
                        run_id,
                        index_key,
                        frame,
                    )
                master_rows.append(_build_master(case, run_id))
                print(f"  rows={len(frame)}")
            except Exception as exc:
                errors[index_key] = str(exc)
                print(f"  failed={type(exc).__name__}: {exc}")

        if not fetched:
            raise RuntimeError("所有指数抓取均失败")

        master = pd.DataFrame(master_rows)
        _write_staging("index_master", run_id, "all", master)
        if any(not frame.empty for frame in fetched.values()):
            _compact_daily(run_id)
        master_path = _compact_unpartitioned(
            "index_master",
            run_id,
            ["index_key"],
        )
        metrics = _build_derived()
        _sync_catalog()
        non_empty_fetched = {
            key: frame for key, frame in fetched.items() if not frame.empty
        }
        quality = _quality_check(
            run_id,
            non_empty_fetched or fetched,
            selected_cases,
        )

        daily_path = CURATED / "index_daily"
        _record_revision("index_master", master_path, len(master))
        _record_revision(
            "index_daily",
            daily_path,
            int(len(_load_curated("index_daily"))),
        )
        _record_revision(
            "index_metrics",
            DERIVED / "index_metrics" / "part-merged.parquet",
            len(metrics),
        )
        state_indexes = dict(previous_state.get("indexes", {}))
        for index_key, frame in fetched.items():
            if frame.empty:
                continue
            state_indexes[index_key] = str(frame["trade_date"].max())
        state = {
            "run_id": run_id,
            "indexes": state_indexes,
            "last_success_date": (
                max(state_indexes.values()) if state_indexes else None
            ),
            "updated_at": _now(),
        }
        (STATE_DIR / "index_daily.json").write_text(
            json.dumps(state, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        has_quality_warning = any(
            finding["severity"] == "warning"
            for finding in quality["findings"]
        )
        status = "warning" if errors or has_quality_warning else "success"
        with sqlite3.connect(MANIFEST_DB) as connection:
            connection.execute(
                """
                UPDATE ingestion_runs
                SET status = ?, finished_at = ?, error_message = ?
                WHERE run_id = ?
                """,
                (
                    status,
                    _now(),
                    json.dumps(errors, ensure_ascii=False) if errors else None,
                    run_id,
                ),
            )
        return {
            "run_id": run_id,
            "mode": mode,
            "status": status,
            "indexes": list(fetched),
            "rows": {key: len(frame) for key, frame in fetched.items()},
            "errors": errors,
            "metrics": metrics.to_dict(orient="records"),
            "quality": quality,
            "catalog_db": str(CATALOG_DB),
        }
    except Exception as exc:
        with sqlite3.connect(MANIFEST_DB) as connection:
            connection.execute(
                """
                UPDATE ingestion_runs
                SET status = 'failed', finished_at = ?, error_message = ?
                WHERE run_id = ?
                """,
                (_now(), str(exc), run_id),
            )
        raise


def list_indexes() -> tuple[dict[str, Any], ...]:
    """Return the configured data-lake index definitions."""
    return INDEXES


def build_lake(
    mode: str = "incremental",
    index_keys: list[str] | None = None,
) -> dict[str, Any]:
    """Build or incrementally update the local data lake."""
    return run(mode=mode, index_keys=index_keys)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=("full", "incremental"),
        default="incremental",
        help="full 重建历史，incremental 只请求已有数据之后的新日期",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="列出 demo 配置的指数",
    )
    parser.add_argument(
        "--indexes",
        default="",
        help="逗号分隔的 index_key，只处理指定指数",
    )
    args = parser.parse_args()
    if args.list:
        for case in INDEXES:
            print(
                case["index_key"],
                case["symbol"],
                case["name"],
                case["market"],
                case["source"],
            )
        return 0

    selected = (
        [item.strip() for item in args.indexes.split(",") if item.strip()]
        if args.indexes
        else None
    )
    result = run(args.mode, selected)
    print("\nDATA LAKE DEMO RESULT")
    print(f"run_id={result['run_id']}")
    print(f"mode={result['mode']}")
    print(f"status={result['status']}")
    print(f"rows={result['rows']}")
    if result["errors"]:
        print(f"errors={result['errors']}")
    print(f"catalog={result['catalog_db']}")
    for item in result["metrics"]:
        print(
            item["index_key"],
            "date=", item["trade_date"],
            "close=", item["close"],
            "pe=", item["pe_ttm"],
            "pe_pct_5y=", item["pe_percentile_5y"],
            "ma250=", item["ma250"],
            "deviation=", item["deviation_pct"],
            "vol10=", item["vol10"],
        )
    warnings = [
        finding
        for finding in result["quality"]["findings"]
        if finding["severity"] != "info"
    ]
    for warning in warnings:
        print("QUALITY", warning["severity"], warning["check"], warning["message"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
