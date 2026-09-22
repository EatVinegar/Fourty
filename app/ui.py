"""Shared Streamlit UI helpers."""

from __future__ import annotations

import pandas as pd
import streamlit as st


def apply_global_style() -> None:
    """Apply compact typography and consistent spacing."""
    st.markdown(
        """
        <style>
            .block-container {
                max-width: 1450px;
                padding-top: 1.4rem;
                padding-bottom: 2rem;
            }

            h1 {
                font-size: 1.75rem !important;
                line-height: 2.1rem !important;
                margin-bottom: 0.2rem !important;
            }

            h2 {
                font-size: 1.25rem !important;
                line-height: 1.65rem !important;
                margin-top: 0.4rem !important;
                margin-bottom: 0.4rem !important;
            }

            h3 {
                font-size: 1.05rem !important;
                line-height: 1.45rem !important;
                margin-top: 0.2rem !important;
                margin-bottom: 0.2rem !important;
            }

            [data-testid="stMetric"] {
                min-height: 78px;
                padding: 0.55rem 0.65rem;
                border: 1px solid rgba(128, 128, 128, 0.22);
                border-radius: 8px;
                background: transparent;
            }

            [data-testid="stMetricLabel"] {
                font-size: 0.75rem;
                line-height: 1rem;
            }

            [data-testid="stMetricValue"] {
                font-size: 1.15rem;
                line-height: 1.35rem;
            }

            [data-testid="stMetricDelta"] {
                font-size: 0.72rem;
            }

            [data-testid="stCaptionContainer"] {
                font-size: 0.72rem;
            }

            div[data-testid="stVerticalBlockBorderWrapper"] {
                border-radius: 8px;
                padding: 0.65rem 0.8rem;
            }

            [data-testid="stHorizontalBlock"] {
                gap: 0.55rem;
            }

            hr {
                margin: 0.9rem 0 !important;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def format_number(
    value: object,
    digits: int = 2,
    suffix: str = "",
) -> str:
    """Format dashboard values without raising on missing data."""
    if value is None or pd.isna(value):
        return "—"
    return f"{float(value):,.{digits}f}{suffix}"


def format_valuation_state(state: object) -> str:
    """Add a visual icon to valuation status labels."""
    text = str(state) if state is not None and not pd.isna(state) else "不可用"
    icons = {
        "低估": "🟢",
        "适中": "🟡",
        "高估": "🔴",
        "不适用": "⚪",
        "不可用": "⚪",
    }
    return f"{icons.get(text, '⚪')} {text}"
