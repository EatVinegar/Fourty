"""Index screening page."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.ui import apply_global_style, format_number, format_valuation_state
from core.logging_utils import configure_logging
from core.valuation import load_config, load_screening_data


st.set_page_config(
    page_title="指数筛选",
    page_icon="🔎",
    layout="wide",
)
apply_global_style()
configure_logging()


def _filter_state(
    frame,
    state_column: str,
    percentile_column: str,
    state: str,
):
    filtered = frame.loc[frame[percentile_column].notna()].copy()
    if state != "全部":
        filtered = filtered.loc[filtered[state_column] == state]
    ascending = state != "高估"
    return filtered.sort_values(
        percentile_column,
        ascending=ascending,
    )


config = load_config()
screening = load_screening_data(config)
if screening.empty:
    st.markdown("## 指数筛选")
    st.warning("请先初始化数据湖。")
    st.stop()

st.markdown("## 指数筛选")
st.caption("两套策略独立筛选，不进行合并。")

valuation_tab, deviation_tab = st.tabs(
    ["估值分位法", "均线偏离法"]
)

with valuation_tab:
    valuation_state = st.radio(
        "估值状态",
        ("低估", "高估", "全部"),
        horizontal=True,
        key="valuation_filter_state",
    )
    valuation = _filter_state(
        screening,
        "valuation_state",
        "pe_percentile_5y_strategy",
        valuation_state,
    )
    st.metric("筛选结果", len(valuation))
    if valuation.empty:
        st.info("没有符合条件的指数。")
    else:
        display = valuation.loc[
            :,
            [
                "index_name",
                "csi_symbol",
                "category",
                "history_date",
                "pe_ttm",
                "pe_percentile_5y_strategy",
                "pe_percentile_5y_strategy_samples",
                "valuation_state",
            ],
        ].rename(
            columns={
                "index_name": "指数",
                "csi_symbol": "代码",
                "category": "类型",
                "history_date": "日期",
                "pe_ttm": "PE",
                "pe_percentile_5y_strategy": "5年PE百分位",
                "pe_percentile_5y_strategy_samples": "PE样本数",
                "valuation_state": "状态",
            }
        )
        display["PE"] = display["PE"].map(
            lambda value: format_number(value, 2)
        )
        display["5年PE百分位"] = display["5年PE百分位"].map(
            lambda value: format_number(value, 2, "%")
        )
        display["状态"] = display["状态"].map(format_valuation_state)
        st.dataframe(display, width="stretch", hide_index=True)

with deviation_tab:
    deviation_state = st.radio(
        "偏离状态",
        ("低估", "高估", "全部"),
        horizontal=True,
        key="deviation_filter_state",
    )
    deviation = _filter_state(
        screening,
        "ma_state",
        "ma_deviation_percentile",
        deviation_state,
    )
    st.metric("筛选结果", len(deviation))
    if deviation.empty:
        st.info("没有符合条件的指数。")
    else:
        display = deviation.loc[
            :,
            [
                "index_name",
                "csi_symbol",
                "category",
                "history_date",
                "ma_deviation_pct",
                "ma_deviation_percentile",
                "ma_deviation_samples",
                "volatility_10",
                "ma_state",
            ],
        ].rename(
            columns={
                "index_name": "指数",
                "csi_symbol": "代码",
                "category": "类型",
                "history_date": "日期",
                "ma_deviation_pct": "年线偏离度",
                "ma_deviation_percentile": "偏离度百分位",
                "ma_deviation_samples": "偏离样本数",
                "volatility_10": "10日波动率",
                "ma_state": "状态",
            }
        )
        for column in ("年线偏离度", "偏离度百分位", "10日波动率"):
            display[column] = display[column].map(
                lambda value: format_number(value, 2, "%")
            )
        display["状态"] = display["状态"].map(format_valuation_state)
        st.dataframe(display, width="stretch", hide_index=True)
