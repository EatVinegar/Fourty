"""Index detail page."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.valuation import (
    get_data_lake_status,
    load_config,
    load_index_detail,
    refresh_all_data,
)
from app.ui import apply_global_style, format_number, format_valuation_state


st.set_page_config(
    page_title="指数详情",
    page_icon="📊",
    layout="wide",
)
apply_global_style()


config = load_config()
lake_status = get_data_lake_status()
indexes = config["indexes"]
index_key = st.sidebar.selectbox(
    "选择指数",
    options=list(indexes),
    format_func=lambda key: indexes[key]["name"],
)

build_label = (
    "初始化数据湖"
    if not lake_status["initialized"]
    else "增量更新数据"
)
build_mode = "full" if not lake_status["initialized"] else "incremental"
if st.sidebar.button(build_label, width="stretch"):
    with st.spinner(f"{build_label}..."):
        result = refresh_all_data(config, mode=build_mode)
    if result["errors"]:
        st.sidebar.error("部分数据更新失败")
    else:
        st.sidebar.success("数据已更新")
    st.rerun()

snapshot, history = load_index_detail(index_key, config)
if snapshot is None or history.empty:
    st.markdown(f"## {indexes[index_key]['name']}")
    st.warning("暂无本地数据，请先初始化数据湖。")
    st.stop()

st.markdown(f"## {snapshot['index_name']} ({snapshot['csi_symbol']})")
st.caption(
    f"行情来源：{snapshot['quote_source']} · "
    f"估值来源：{snapshot['source']} · "
    f"PE日期：{snapshot['pe_latest_date']}"
)

st.markdown("### 行情与估值")
metric_columns = st.columns(6)
metric_columns[0].metric("最新价格", format_number(snapshot["price"], 4))
metric_columns[1].metric(
    "涨跌幅",
    format_number(snapshot["change_pct"], 3, "%"),
)
metric_columns[2].metric("PE(TTM)", format_number(snapshot["pe_ttm"], 2))
metric_columns[3].metric(
    "盈利收益率",
    format_number(snapshot["earnings_yield_pct"], 4, "%"),
)
metric_columns[4].metric(
    "5年PE百分位",
    format_number(snapshot["pe_percentile_5y"], 2, "%"),
)
metric_columns[5].metric(
    "10年PE百分位",
    format_number(snapshot["pe_percentile_10y"], 2, "%"),
)

st.markdown("### 策略状态与本月金额")
strategy_columns = st.columns(4)
strategy_columns[0].metric(
    "估值分位法状态",
    format_valuation_state(snapshot.get("valuation_state")),
)
strategy_columns[1].metric(
    "估值分位法金额",
    format_number(snapshot.get("valuation_amount"), 2, " 元"),
)
strategy_columns[2].metric(
    "均线偏离法状态",
    format_valuation_state(snapshot.get("ma_state")),
)
strategy_columns[3].metric(
    "均线偏离法金额",
    format_number(snapshot.get("ma_amount"), 2, " 元"),
)

st.markdown("### 均线模型")
strategy_detail_columns = st.columns(4)
strategy_detail_columns[0].metric(
    "MA250",
    format_number(snapshot.get("ma250"), 4),
)
strategy_detail_columns[1].metric(
    "年线偏离度",
    format_number(snapshot.get("ma_deviation_pct"), 3, "%"),
)
strategy_detail_columns[2].metric(
    "10日年化波动率",
    format_number(snapshot.get("volatility_10"), 3, "%"),
)
strategy_detail_columns[3].metric(
    "波动率折扣",
    format_number(snapshot.get("ma_volatility_discount"), 3),
)

initial_amount = snapshot.get("initial_amount")
if initial_amount is None or pd.isna(initial_amount):
    st.info("该指数尚未设置初始金额和初始指数价格，金额暂不可计算。")
else:
    months_elapsed = snapshot.get("months_elapsed")
    months_text = (
        "—"
        if months_elapsed is None or pd.isna(months_elapsed)
        else str(int(months_elapsed))
    )
    st.caption(
        f"初始金额：{format_number(initial_amount, 2, ' 元')} · "
        f"初始指数价格：{format_number(snapshot.get('initial_index_price'), 4)} · "
        f"策略起始日期：{snapshot.get('strategy_start_date') or '—'} · "
        f"已运行月数：{months_text}"
    )

range_label = st.radio(
    "展示区间",
    ("近1年", "近3年", "近5年", "近10年"),
    horizontal=True,
)
year_count = {
    "近1年": 1,
    "近3年": 3,
    "近5年": 5,
    "近10年": 10,
}[range_label]

history = history.copy()
history["trade_date"] = pd.to_datetime(history["trade_date"])
start_date = history["trade_date"].max() - pd.DateOffset(years=year_count)
window = history.loc[history["trade_date"] >= start_date].copy()

close_tab, pe_tab = st.tabs(["收盘价格", "PE(TTM)"])
with close_tab:
    close_chart = window.set_index("trade_date")[["close"]].rename(
        columns={"close": "收盘价"}
    )
    st.line_chart(close_chart, width="stretch")

with pe_tab:
    pe_chart = (
        window.loc[window["pe_ttm"] > 0]
        .set_index("trade_date")[["pe_ttm"]]
        .rename(columns={"pe_ttm": "PE(TTM)"})
    )
    st.line_chart(pe_chart, width="stretch")

st.caption(
    "指数数据只用于行情和历史估值展示。页面不生成买入、卖出或定投决策。"
)
