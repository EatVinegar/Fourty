"""Strategy settings page."""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pandas as pd
import streamlit as st


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.valuation import (
    load_all_index_settings,
    load_config,
    resolve_strategy_start_price,
    save_index_settings,
)
from app.ui import apply_global_style, format_number


st.set_page_config(
    page_title="设置",
    page_icon="⚙️",
    layout="wide",
)
apply_global_style()

config = load_config()
indexes = config["indexes"]
settings = load_all_index_settings(config)
settings_map = (
    {
        str(row.index_key): row._asdict()
        for row in settings.itertuples(index=False)
    }
    if not settings.empty
    else {}
)

st.markdown("## 策略设置")
st.caption("初始指数价格由策略起始日期自动匹配。")

selected_key = st.sidebar.selectbox(
    "选择指数",
    options=list(indexes),
    format_func=lambda key: indexes[key]["name"],
)
existing = settings_map.get(selected_key, {})

initial_amount = st.number_input(
    "初始金额（元）",
    min_value=1.0,
    value=float(existing.get("initial_amount", 1000.0)),
    step=100.0,
)
existing_start = existing.get("strategy_start_date")
start_date = st.date_input(
    "策略起始日期",
    value=(
        pd.Timestamp(existing_start).date()
        if existing_start
        else date.today()
    ),
)

resolved_price = None
resolved_price_date = None
try:
    resolved_price, resolved_price_date = resolve_strategy_start_price(
        selected_key,
        start_date.isoformat(),
        config,
    )
    preview_columns = st.columns(2)
    preview_columns[0].metric(
        "自动初始指数价格",
        format_number(resolved_price, 4),
    )
    preview_columns[1].metric(
        "对应交易日期",
        resolved_price_date,
    )
except Exception as exc:
    st.warning(str(exc))

submitted = st.button("保存设置", width="stretch")

if submitted:
    try:
        save_index_settings(
            selected_key,
            initial_amount=float(initial_amount),
            strategy_start_date=start_date.isoformat(),
            config=config,
        )
        st.success(f"{indexes[selected_key]['name']} 设置已保存")
        settings = load_all_index_settings(config)
        settings_map = {
            str(row.index_key): row._asdict()
            for row in settings.itertuples(index=False)
        }
    except Exception as exc:
        st.error(str(exc))

st.subheader("当前设置")
if settings.empty:
    st.info("尚未保存任何指数设置。")
else:
    display = settings.rename(
        columns={
            "index_key": "内部键",
            "initial_amount": "初始金额",
            "initial_index_price": "初始指数价格",
            "strategy_start_date": "策略起始日期",
            "updated_at": "更新时间",
        }
    )
    display.insert(
        1,
        "指数名称",
        display["内部键"].map(
            lambda key: indexes.get(key, {}).get("name", key)
        ),
    )
    st.dataframe(display, width="stretch", hide_index=True)

st.divider()
st.caption(
    "初始指数价格自动取不晚于策略起始日期的最近一个有效交易日收盘价，"
    "用于记录策略起点。"
)
