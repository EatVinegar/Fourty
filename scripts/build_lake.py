r"""CLI entry point for building the local index data lake.

Examples:

    D:\code\Fourty\.venv\python.exe scripts\build_lake.py --mode full
    D:\code\Fourty\.venv\python.exe scripts\build_lake.py --mode incremental
    D:\code\Fourty\.venv\python.exe scripts\build_lake.py --mode incremental --indexes hs300,csi500
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core import lake


def main() -> int:
    parser = argparse.ArgumentParser(description="构建本地指数数据湖")
    parser.add_argument(
        "--mode",
        choices=("full", "incremental"),
        default="incremental",
        help="full 初始化完整历史，incremental 只更新新增日期",
    )
    parser.add_argument(
        "--indexes",
        default="",
        help="逗号分隔的 index_key，留空表示全部",
    )
    parser.add_argument("--list", action="store_true", help="列出可用指数")
    args = parser.parse_args()

    if args.list:
        for item in lake.list_indexes():
            print(
                f"{item['index_key']}\t{item['symbol']}\t"
                f"{item['name']}\t{item['market']}\t{item['source']}"
            )
        return 0

    selected = (
        [item.strip() for item in args.indexes.split(",") if item.strip()]
        if args.indexes
        else None
    )
    result = lake.build_lake(mode=args.mode, index_keys=selected)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
