"""Today's market dashboard."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.valuation import (
    get_data_lake_status,
    load_config,
    load_dashboard_data,
    refresh_all_data,
)
from app.ui import apply_global_style, format_number, format_valuation_state


st.set_page_config(
    page_title="今日看板",
    page_icon="📈",
    layout="wide",
)
apply_global_style()


def _refresh(config: dict, mode: str) -> dict:
    action = "初始化数据湖" if mode == "full" else "增量更新数据"
    with st.spinner(f"{action}，请稍候..."):
        return refresh_all_data(config, mode=mode)


config = load_config()
lake_status = get_data_lake_status()
stored_snapshots, bond, last_refresh = load_dashboard_data(config)

st.markdown("## 今日看板")
st.caption("只展示指数行情和历史估值指标，不生成买入或卖出指令。")
if not lake_status["initialized"]:
    st.info("本地数据湖尚未初始化。首次建湖会拉取全部指数历史数据。")
    build_label = "初始化数据湖"
    build_mode = "full"
else:
    build_label = "增量更新数据"
    build_mode = "incremental"

if st.button(build_label, width="content"):
    refresh_result = _refresh(config, mode=build_mode)
    if refresh_result["errors"]:
        st.error("部分数据更新失败：" + "；".join(refresh_result["errors"].values()))
    stored_snapshots, bond, last_refresh = load_dashboard_data(config)
    st.rerun()

top_left, top_right = st.columns([1, 3])
with top_left:
    st.metric(
        "中国10年期国债收益率",
        format_number(bond["yield_10y"] if bond else None, 4, "%"),
        help="来源：AKShare bond_gb_zh_sina",
    )
with top_right:
    st.caption(
        "最近更新："
        + (last_refresh or "尚未更新")
        + (
            f" · 数据湖指数：{lake_status['index_count']} · "
            f"数据日期：{lake_status['latest_trade_date']}"
            if lake_status["initialized"]
            else " · 数据湖未初始化"
        )
        + (
            f" · 国债数据日期：{bond['trade_date']}"
            if bond
            else " · 暂无国债数据"
        )
    )

if stored_snapshots.empty:
    st.warning("本地数据湖还没有可用于展示的指数数据。")
    st.stop()

for row in stored_snapshots.itertuples(index=False):
    with st.container(border=True):
        title_col, source_col = st.columns([3, 2])
        with title_col:
            st.markdown(f"### {row.index_name} ({row.csi_symbol})")
        with source_col:
            st.caption(
                f"行情来源：{row.quote_source or '—'} · "
                f"估值来源：{row.source or 'AKShare'}"
            )

        st.markdown("#### 行情与估值")
        metric_columns = st.columns(6)
        metric_columns[0].metric("最新价格", format_number(row.price, 4))
        metric_columns[1].metric(
            "涨跌幅",
            format_number(row.change_pct, 3, "%"),
        )
        metric_columns[2].metric("PE(TTM)", format_number(row.pe_ttm, 2))
        metric_columns[3].metric(
            "盈利收益率",
            format_number(row.earnings_yield_pct, 4, "%"),
        )
        metric_columns[4].metric(
            "5年PE百分位",
            format_number(row.pe_percentile_5y, 2, "%"),
        )
        metric_columns[5].metric(
            "10年PE百分位",
            format_number(row.pe_percentile_10y, 2, "%"),
        )

        st.markdown("#### 策略状态")
        state_columns = st.columns(4)
        state_columns[0].metric(
            "PE估值状态",
            format_valuation_state(row.valuation_state),
        )
        state_columns[1].metric(
            "PE分位",
            format_number(row.pe_percentile_5y_strategy, 2, "%"),
        )
        state_columns[2].metric(
            "均线偏离状态",
            format_valuation_state(row.ma_state),
        )
        state_columns[3].metric(
            "偏离度分位",
            format_number(row.ma_deviation_percentile, 2, "%"),
        )

        st.caption(
            f"PE日期：{row.pe_latest_date or '—'} · "
            f"5年样本：{int(row.pe_window_5y_samples or 0)} · "
            f"10年样本：{int(row.pe_window_10y_samples or 0)} · "
            f"MA250偏离：{format_number(row.ma_deviation_pct, 2, '%')} · "
            f"10日波动率：{format_number(row.volatility_10, 2, '%')}"
        )

st.divider()
st.caption(
    "数据来自 AKShare 封装的中证指数与新浪行情接口。"
    "页面仅用于数据展示，不构成投资建议。"
)
