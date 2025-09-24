"""Streamlit dashboard for くらしいきいき社の計数管理アプリ."""
from __future__ import annotations

# TODO: Streamlit UIコンポーネントを使ってダッシュボードを構築
import html
import hashlib
import io
from contextlib import contextmanager
import calendar
from datetime import date, datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl

import numpy as np
import pandas as pd
import altair as alt
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from streamlit_plotly_events import plotly_events

from data_processing import (
    DEFAULT_FIXED_COST,
    annotate_customer_segments,
    build_alerts,
    calculate_kpis,
    create_current_pl,
    create_default_cashflow_plan,
    fetch_sales_from_endpoint,
    forecast_cashflow,
    generate_sample_cost_data,
    generate_sample_sales_data,
    generate_sample_subscription_data,
    detect_duplicate_rows,
    validate_channel_fees,
    ValidationReport,
    load_cost_workbook,
    load_sales_files,
    load_subscription_workbook,
    merge_sales_and_costs,
    monthly_sales_summary,
    simulate_pl,
    compute_channel_share,
    compute_category_share,
    compute_kpi_breakdown,
)

st.set_page_config(
    page_title="くらしいきいき社 計数管理ダッシュボード",
    layout="wide",
)


PERIOD_FREQ_OPTIONS: List[Tuple[str, str]] = [
    ("月次", "M"),
    ("週次", "W-MON"),
    ("四半期", "Q"),
    ("年次", "Y"),
]

PERIOD_YOY_LAG: Dict[str, int] = {
    "M": 12,
    "W-MON": 52,
    "Q": 4,
    "Y": 1,
}


PLAN_WIZARD_STEPS: List[Dict[str, str]] = [
    {
        "title": "基本情報入力",
        "description": "会社名や計画期間を設定し、計画の前提条件を整理します。",
    },
    {
        "title": "売上予測",
        "description": "チャネル別の売上計画をCSV取り込みやテンプレートで作成します。",
    },
    {
        "title": "経費入力",
        "description": "固定費・変動費のテンプレートや自動補完を使ってコスト計画を整えます。",
    },
    {
        "title": "財務指標計算",
        "description": "売上と経費から利益率などの主要指標を自動計算します。",
    },
    {
        "title": "結果確認",
        "description": "入力内容を確認し、計画サマリーを共有用に出力します。",
    },
]


SALES_PLAN_COLUMNS = ["項目", "月次売上", "チャネル"]
EXPENSE_PLAN_COLUMNS = ["費目", "月次金額", "区分"]

COMMON_SALES_ITEMS = [
    "自社サイト売上",
    "楽天市場売上",
    "Amazon売上",
    "Yahoo!ショッピング売上",
    "サブスク売上",
    "卸売売上",
    "定期便アップセル",
    "店頭販売",
]

COMMON_EXPENSE_ITEMS = [
    "人件費",
    "家賃",
    "広告宣伝費",
    "配送費",
    "外注費",
    "システム利用料",
    "水道光熱費",
    "雑費",
]

PLAN_CHANNEL_OPTIONS_BASE = [
    "自社サイト",
    "楽天市場",
    "Amazon",
    "Yahoo!ショッピング",
    "卸売",
    "サブスク",
    "広告流入",
    "その他",
]

PLAN_EXPENSE_CLASSIFICATIONS = ["固定費", "変動費", "投資", "その他"]

SALES_PLAN_TEMPLATES: Dict[str, List[Dict[str, Any]]] = {
    "EC標準チャネル構成": [
        {"項目": "自社サイト売上", "月次売上": 1_200_000, "チャネル": "自社サイト"},
        {"項目": "楽天市場売上", "月次売上": 950_000, "チャネル": "楽天市場"},
        {"項目": "Amazon売上", "月次売上": 780_000, "チャネル": "Amazon"},
        {"項目": "Yahoo!ショッピング売上", "月次売上": 320_000, "チャネル": "Yahoo!ショッピング"},
    ],
    "サブスク強化モデル": [
        {"項目": "サブスク売上", "月次売上": 850_000, "チャネル": "サブスク"},
        {"項目": "定期便アップセル", "月次売上": 420_000, "チャネル": "サブスク"},
        {"項目": "新規顧客向け単品", "月次売上": 380_000, "チャネル": "広告流入"},
    ],
}

EXPENSE_PLAN_TEMPLATES: Dict[str, List[Dict[str, Any]]] = {
    "スリム型コスト構成": [
        {"費目": "人件費", "月次金額": 600_000, "区分": "固定費"},
        {"費目": "家賃", "月次金額": 200_000, "区分": "固定費"},
        {"費目": "広告宣伝費", "月次金額": 180_000, "区分": "変動費"},
        {"費目": "システム利用料", "月次金額": 90_000, "区分": "固定費"},
    ],
    "成長投資モデル": [
        {"費目": "人件費", "月次金額": 850_000, "区分": "固定費"},
        {"費目": "広告宣伝費", "月次金額": 320_000, "区分": "変動費"},
        {"費目": "外注費", "月次金額": 160_000, "区分": "変動費"},
        {"費目": "研究開発費", "月次金額": 120_000, "区分": "投資"},
    ],
}

DEFAULT_STORE_OPTIONS = ["全社", "那覇本店", "浦添物流センター", "EC本部"]

FILTER_STATE_KEYS = {
    "store": "filter_store",
    "channels": "filter_channels",
    "categories": "filter_categories",
    "period": "filter_period",
    "freq": "filter_frequency",
    "signature": "filter_signature",
}

STATE_MESSAGES: Dict[str, Dict[str, Any]] = {
    "empty_data": {
        "type": "warning",
        "text": "該当期間のデータがありません。他の期間やチャネルを選択してください。",
        "action_label": "デフォルト条件に戻る",
    },
    "loading": {
        "type": "info",
        "text": "データを読み込み中です…",
    },
    "success": {
        "type": "success",
        "text": "データを更新しました。",
    },
    "warning_gross_margin": {
        "type": "warning",
        "text": "粗利率が目標を下回っています。商品構成を見直しましょう。",
    },
    "error": {
        "type": "error",
        "text": "データの読み込みに失敗しました。再試行してください。",
        "action_label": "再読み込み",
    },
    "csv_done": {
        "type": "info",
        "text": "CSVをダウンロードしました。",
    },
    "unauthorized": {
        "type": "error",
        "text": "この操作を行う権限がありません。管理者にお問い合わせください。",
    },
}

SALES_IMPORT_CANDIDATES: Dict[str, List[str]] = {
    "項目": ["項目", "科目", "勘定科目", "売上科目", "部門"],
    "月次売上": ["月次売上", "金額", "売上高", "予測額"],
    "チャネル": ["チャネル", "分類", "モール", "部門", "経路"],
}

EXPENSE_IMPORT_CANDIDATES: Dict[str, List[str]] = {
    "費目": ["費目", "科目", "勘定科目", "費用科目"],
    "月次金額": ["月次金額", "金額", "予算額", "支出額"],
    "区分": ["区分", "分類", "タイプ", "費用区分"],
}


UPLOAD_META_MULTIPLE = "対応形式: CSV, Excel（最大10MB・複数ファイル対応）"
UPLOAD_META_SINGLE = "対応形式: CSV, Excel（最大10MB・1ファイル）"
UPLOAD_HELP_MULTIPLE = "CSVまたはExcelファイルをドラッグ＆ドロップで追加できます。複数ファイルをまとめてアップロードできます。"
UPLOAD_HELP_SINGLE = "CSVまたはExcelファイルをドラッグ＆ドロップでアップロードしてください。1ファイルのみアップロードできます。"

SALES_UPLOAD_CONFIGS: List[Dict[str, str]] = [
    {
        "channel": "自社サイト",
        "label": "自社サイト売上データ",
        "description": "公式ECサイトの受注・売上明細ファイルをアップロードしてください。",
    },
    {
        "channel": "楽天市場",
        "label": "楽天市場売上データ",
        "description": "楽天RMSなどからダウンロードした売上CSV/Excelを読み込みます。",
    },
    {
        "channel": "Amazon",
        "label": "Amazon売上データ",
        "description": "Amazonセラーセントラルのレポートをアップロードします。",
    },
    {
        "channel": "Yahoo!ショッピング",
        "label": "Yahoo!ショッピング売上データ",
        "description": "ストアクリエイターProから出力した受注データを取り込みます。",
    },
]

ANCILLARY_UPLOAD_CONFIGS: List[Dict[str, Any]] = [
    {
        "key": "cost",
        "label": "商品原価率一覧",
        "description": "商品別の売価・原価・原価率がまとまったファイルをアップロードします。",
        "meta_text": UPLOAD_META_SINGLE,
        "help_text": "商品原価率表のCSVまたはExcelを1ファイルだけアップロードできます。",
        "multiple": False,
    },
    {
        "key": "subscription",
        "label": "定期購買/KPIデータ",
        "description": "サブスク会員数・解約数などの月次KPIを含むファイルを読み込みます。",
        "meta_text": UPLOAD_META_SINGLE,
        "help_text": "サブスクリプションのKPIを記載したCSVまたはExcelを1ファイルアップロードしてください。",
        "multiple": False,
    },
]


STATUS_PILL_DETAILS: Dict[str, Tuple[str, str]] = {
    "ok": ("✅", "正常"),
    "warning": ("⚠️", "警告"),
    "error": ("⛔", "エラー"),
}


MAIN_NAV_STRUCTURE: List[Tuple[str, List[str]]] = [
    ("ホーム", ["ダッシュボード"]),
    ("分析", ["売上分析", "利益分析", "財務モニタリング"]),
    ("レポート", ["KPIモニタリング"]),
    ("データ入力", ["データアップロード/管理"]),
]

TUTORIAL_INDEX: List[Dict[str, Any]] = [
    {
        "title": "KPIの読み解き方と活用ガイド",
        "keywords": ["kpi", "活用", "レポート", "ダッシュボード"],
        "path": "docs/01_user_research_and_kpi.md",
    }
]


PRIMARY_NAVY = "#0B1F33"
PRIMARY_NAVY_ALT = "#123A66"
SECONDARY_SLATE = "#5B6B82"
SECONDARY_SKY = "#E6ECF4"
NEUTRAL_STEEL = "#8FA5C9"
ACCENT_BLUE = "#1E88E5"
ACCENT_BLUE_STRONG = "#15579B"
ACCENT_ORANGE = "#FF7A45"
ACCENT_ORANGE_STRONG = "#C24C1D"
INK_INVERSE = "#F5F8FF"
INK_MUTED = "#C7D3E7"
MCKINSEY_FONT_STACK = (
    "'Noto Sans JP', 'Hiragino Sans', 'Segoe UI', 'Helvetica Neue', sans-serif"
)
ALT_FONT_FAMILY = "Noto Sans JP"
SALES_SERIES_COLOR = "#1E88E5"
GROSS_SERIES_COLOR = "#00796B"
INVENTORY_SERIES_COLOR = "#F9A825"
CASH_SERIES_COLOR = "#3949AB"
YOY_SERIES_COLOR = SECONDARY_SLATE
BASELINE_SERIES_COLOR = "#6D6D6D"
CF_COLOR_MAPPING = {
    "営業CF": SALES_SERIES_COLOR,
    "投資CF": ACCENT_ORANGE,
    "財務CF": GROSS_SERIES_COLOR,
    "返済": YOY_SERIES_COLOR,
}
PLOTLY_COLORWAY = [
    SALES_SERIES_COLOR,
    GROSS_SERIES_COLOR,
    INVENTORY_SERIES_COLOR,
    YOY_SERIES_COLOR,
    ACCENT_ORANGE,
]


KGI_TARGETS = {
    "sales": 7_000_000,
    "gross_margin_rate": 0.62,
    "cash_balance": 5_000_000,
}


def apply_chart_theme(fig):
    """マッキンゼー風の配色と余白に合わせてPlotly図を整える。"""

    fig.update_layout(
        font=dict(family=MCKINSEY_FONT_STACK, color="#0F1E2E"),
        title=dict(font=dict(size=18, color="#0F1E2E", family=MCKINSEY_FONT_STACK)),
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=12)),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=48, r=36, t=60, b=48),
        hoverlabel=dict(font=dict(family=MCKINSEY_FONT_STACK)),
        colorway=PLOTLY_COLORWAY,
    )
    fig.update_xaxes(showgrid=True, gridcolor="rgba(11,31,51,0.08)")
    fig.update_yaxes(showgrid=True, gridcolor="rgba(11,31,51,0.08)")
    return fig


def apply_altair_theme(chart: alt.Chart) -> alt.Chart:
    """Altairグラフに共通のスタイル・タイポグラフィを適用する。"""

    return (
        chart.configure_axis(
            labelFont=ALT_FONT_FAMILY,
            titleFont=ALT_FONT_FAMILY,
            labelColor="#0F1E2E",
            titleColor="#0F1E2E",
            gridColor="rgba(11,31,51,0.12)",
            domainColor="rgba(11,31,51,0.18)",
        )
        .configure_legend(
            titleFont=ALT_FONT_FAMILY,
            labelFont=ALT_FONT_FAMILY,
            labelColor="#0F1E2E",
            titleColor="#0F1E2E",
            orient="top",
            direction="horizontal",
            symbolSize=120,
        )
        .configure_view(strokeOpacity=0)
        .configure_title(font=ALT_FONT_FAMILY, color="#0F1E2E", fontSize=18)
        .configure_mark(font=ALT_FONT_FAMILY)
    )


def inject_mckinsey_style() -> None:
    """60-30-10のカラーパレットとタイポグラフィをアプリ全体に適用する。"""

    st.markdown(
        f"""
        <style>
        :root {{
            --color-primary: {PRIMARY_NAVY};
            --color-primary-alt: {PRIMARY_NAVY_ALT};
            --color-accent: {ACCENT_BLUE};
            --color-accent-strong: {ACCENT_BLUE_STRONG};
            --color-alert: {ACCENT_ORANGE};
            --color-alert-strong: {ACCENT_ORANGE_STRONG};
            --secondary-surface: {SECONDARY_SKY};
            --surface-elevated: #ffffff;
            --surface-contrast: rgba(255, 255, 255, 0.08);
            --ink-base: #1A2433;
            --ink-strong: #0F1E2E;
            --ink-subtle: #5B6A82;
            --ink-inverse: {INK_INVERSE};
            --ink-muted: {INK_MUTED};
            --focus-outline: rgba(42, 134, 255, 0.45);
        }}

        html, body {{
            font-family: {MCKINSEY_FONT_STACK};
            color: var(--ink-inverse);
            line-height: 1.45;
            background-color: var(--color-primary);
            background-image: linear-gradient(180deg, #0B1F33 0%, #123A66 60%, #0B1F33 100%);
            min-height: 100%;
        }}

        a {{
            color: #B8D4FF;
            font-weight: 600;
            text-decoration: underline;
        }}

        a:hover {{
            color: #E0ECFF;
        }}

        .surface-card a,
        .form-section a,
        .search-card a,
        .stApp main div[data-testid="stDataFrame"] a {{
            color: var(--color-accent-strong);
        }}

        .surface-card a:hover,
        .form-section a:hover,
        .search-card a:hover,
        .stApp main div[data-testid="stDataFrame"] a:hover {{
            color: #174A9C;
        }}

        a:focus {{
            outline: 3px solid var(--focus-outline);
            outline-offset: 2px;
        }}

        main .block-container {{
            max-width: 1220px;
            padding: 2.2rem 1.8rem 3rem;
            color: var(--ink-inverse);
        }}

        .stAppViewContainer {{
            padding-top: 0;
        }}

        .stApp {{
            background: linear-gradient(180deg, rgba(11,31,51,0.98) 0%, rgba(18,58,102,0.94) 60%, rgba(230,236,244,0.35) 100%);
            color: var(--ink-inverse);
            font-family: {MCKINSEY_FONT_STACK};
        }}

        p, li, span {{
            font-size: 15px;
            line-height: 1.45;
        }}

        h1, h2, h3, h4 {{
            font-weight: 700;
            letter-spacing: 0.02em;
            line-height: 1.35;
            color: var(--ink-inverse);
        }}

        h2 {{
            margin-top: 2rem;
        }}

        h3 {{
            margin-top: 1.5rem;
        }}

        .surface-card h1,
        .surface-card h2,
        .surface-card h3,
        .surface-card h4,
        .form-section h1,
        .form-section h2,
        .form-section h3,
        .form-section h4 {{
            color: var(--ink-strong);
        }}

        .surface-card p,
        .surface-card li,
        .surface-card span,
        .form-section p,
        .form-section li,
        .form-section span {{
            color: var(--ink-base);
        }}

        section[data-testid="stSidebar"] {{
            background: linear-gradient(180deg, #F7F9FC 0%, #FFFFFF 100%);
            color: var(--ink-strong);
            border-right: 1px solid rgba(11,31,51,0.08);
        }}

        section[data-testid="stSidebar"] .block-container {{
            padding-top: 2.4rem;
        }}

        section[data-testid="stSidebar"] * {{
            color: var(--ink-base);
        }}

        section[data-testid="stSidebar"] h1,
        section[data-testid="stSidebar"] h2,
        section[data-testid="stSidebar"] h3,
        section[data-testid="stSidebar"] h4 {{
            color: var(--ink-strong);
        }}

        section[data-testid="stSidebar"] a {{
            color: #1E5CC3;
        }}

        section[data-testid="stSidebar"] a:hover {{
            color: #0F1E2E;
        }}

        section[data-testid="stSidebar"] input,
        section[data-testid="stSidebar"] select,
        section[data-testid="stSidebar"] textarea {{
            background: #FFFFFF;
            border-radius: 0.65rem;
            border: 1px solid rgba(11,31,51,0.18);
            color: var(--ink-base);
        }}

        section[data-testid="stSidebar"] input:focus,
        section[data-testid="stSidebar"] select:focus,
        section[data-testid="stSidebar"] textarea:focus {{
            border-color: rgba(30,92,195,0.55);
            box-shadow: 0 0 0 2px rgba(30,92,195,0.25);
        }}

        section[data-testid="stSidebar"] input::placeholder,
        section[data-testid="stSidebar"] textarea::placeholder {{
            color: rgba(17,58,102,0.5);
        }}

        section[data-testid="stSidebar"] .stButton>button,
        section[data-testid="stSidebar"] .stDownloadButton>button {{
            border-radius: 0.6rem;
            border: 1px solid rgba(30,92,195,0.35);
            background: linear-gradient(135deg, #2A86FF, #1E5CC3);
            color: #ffffff;
            box-shadow: 0 8px 20px rgba(30,92,195,0.25);
        }}

        section[data-testid="stSidebar"] .stButton>button:hover,
        section[data-testid="stSidebar"] .stDownloadButton>button:hover {{
            background: linear-gradient(135deg, #1E5CC3, #174A9C);
        }}

        section[data-testid="stSidebar"] div[data-testid="stExpander"] {{
            border: 1px solid rgba(11,31,51,0.1);
            border-radius: 0.95rem;
            background: #FFFFFF;
            box-shadow: 0 12px 26px rgba(15,30,46,0.06);
            margin-bottom: 0.85rem;
            overflow: hidden;
        }}

        section[data-testid="stSidebar"] div[data-testid="stExpander"] > div:first-child {{
            padding: 0.9rem 1rem;
        }}

        section[data-testid="stSidebar"] div[data-testid="stExpander"] > div:nth-child(2) {{
            padding: 0 1rem 1rem;
        }}

        section[data-testid="stSidebar"] div[data-testid="stExpander"] summary {{
            font-weight: 700;
            color: var(--ink-strong);
        }}

        .sidebar-section {{
            background: #FFFFFF;
            border-radius: 0.95rem;
            border: 1px solid rgba(11,31,51,0.08);
            padding: 1rem 1.1rem;
            box-shadow: 0 10px 22px rgba(15,30,46,0.05);
            margin-bottom: 1rem;
        }}

        .sidebar-section--emphasis {{
            background: linear-gradient(135deg, rgba(42,134,255,0.12) 0%, rgba(18,58,102,0.08) 100%);
            border-color: rgba(30,92,195,0.22);
        }}

        .sidebar-section__eyebrow {{
            font-size: 0.7rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: #1E5CC3;
            margin-bottom: 0.25rem;
        }}

        .sidebar-section__title {{
            font-size: 0.95rem;
            font-weight: 600;
            color: var(--ink-strong);
            margin-bottom: 0.3rem;
        }}

        .sidebar-section__body {{
            font-size: 0.85rem;
            color: var(--ink-base);
            margin-bottom: 0.5rem;
        }}

        .sidebar-section__status {{
            display: inline-flex;
            align-items: center;
            gap: 0.3rem;
            font-size: 0.8rem;
            font-weight: 600;
            padding: 0.3rem 0.65rem;
            border-radius: 0.6rem;
            background: rgba(30,92,195,0.14);
            color: #1E5CC3;
            margin-bottom: 0.6rem;
        }}

        .sidebar-subheading {{
            font-size: 0.95rem;
            font-weight: 700;
            color: var(--ink-strong);
            margin: 1.2rem 0 0.6rem;
        }}

        .sidebar-upload-card {{
            display: grid;
            grid-template-columns: auto 1fr;
            gap: 0.75rem;
            align-items: center;
            background: #F4F7FB;
            border: 1px dashed rgba(17,58,102,0.25);
            border-radius: 0.85rem;
            padding: 0.85rem 1rem;
            margin-bottom: 0.6rem;
        }}

        .sidebar-upload-card__icons {{
            display: flex;
            flex-direction: column;
            gap: 0.35rem;
        }}

        .sidebar-upload-card__icon {{
            display: inline-flex;
            align-items: center;
            justify-content: center;
            padding: 0.25rem 0.6rem;
            border-radius: 0.6rem;
            font-size: 0.72rem;
            font-weight: 700;
            letter-spacing: 0.05em;
        }}

        .sidebar-upload-card__icon--csv {{
            background: rgba(30,92,195,0.16);
            color: #1E5CC3;
        }}

        .sidebar-upload-card__icon--excel {{
            background: rgba(26,140,76,0.16);
            color: #1B7A4A;
        }}

        .sidebar-upload-card__title {{
            font-weight: 700;
            font-size: 0.95rem;
            color: var(--ink-strong);
        }}

        .sidebar-upload-card__meta {{
            font-size: 0.78rem;
            color: var(--ink-subtle);
            margin-top: 0.15rem;
        }}

        .sidebar-upload-card__desc {{
            font-size: 0.85rem;
            color: var(--ink-base);
            margin: 0.45rem 0 0;
        }}

        section[data-testid="stSidebar"] .stFileUploader div[data-testid="stFileUploaderDropzone"] {{
            background: #FFFFFF;
            border: 1px dashed rgba(17,58,102,0.28);
            border-radius: 0.9rem;
        }}

        section[data-testid="stSidebar"] .stFileUploader div[data-testid="stFileUploaderDropzone"] p {{
            color: var(--ink-subtle);
        }}

        section[data-testid="stSidebar"] .stFileUploader div[data-testid="stFileUploaderDropzone"]:hover {{
            border-color: rgba(30,92,195,0.45);
        }}

        .hero-panel {{
            background: linear-gradient(135deg, var(--color-primary) 0%, var(--color-primary-alt) 100%);
            color: var(--ink-inverse);
            padding: 2.2rem 2.6rem;
            border-radius: 1.25rem;
            margin-bottom: 1.5rem;
            box-shadow: 0 22px 55px rgba(11,31,51,0.28);
        }}

        .hero-title {{
            font-size: 1.9rem;
            margin-bottom: 0.6rem;
        }}

        .hero-subtitle {{
            font-size: 1.05rem;
            max-width: 760px;
            opacity: 0.96;
        }}

        .hero-meta {{
            margin-top: 1.2rem;
            display: flex;
            flex-wrap: wrap;
            gap: 0.75rem;
        }}

        .hero-persona {{
            margin-top: 1.4rem;
            display: flex;
            flex-wrap: wrap;
            gap: 0.6rem;
        }}

        .hero-chip {{
            display: inline-flex;
            align-items: center;
            background: rgba(255,255,255,0.88);
            color: var(--color-primary);
            padding: 0.3rem 0.85rem;
            border-radius: 999px;
            font-size: 0.85rem;
            font-weight: 600;
            letter-spacing: 0.01em;
            gap: 0.35rem;
            box-shadow: 0 8px 22px rgba(9,21,35,0.16);
        }}

        .hero-badge {{
            display: inline-flex;
            align-items: center;
            padding: 0.35rem 0.85rem;
            border-radius: 999px;
            background: rgba(255,255,255,0.2);
            font-size: 0.9rem;
            letter-spacing: 0.02em;
            color: var(--ink-inverse);
            gap: 0.4rem;
        }}

        .hero-badge--accent {{
            background: var(--color-accent-strong);
            color: var(--ink-inverse);
        }}

        .hero-badge--alert {{
            background: var(--color-alert-strong);
            color: var(--ink-inverse);
        }}

        .surface-card {{
            background: var(--surface-elevated);
            border-radius: 1rem;
            padding: 1.6rem 1.8rem;
            box-shadow: 0 16px 42px rgba(15,30,46,0.08);
            border: 1px solid rgba(11,31,51,0.12);
            margin-bottom: 1.8rem;
            color: var(--ink-base);
        }}

        .kgi-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
            gap: 1.2rem;
            margin-bottom: 1.4rem;
        }}

        .kgi-card {{
            position: relative;
            padding: 1.4rem 1.6rem;
            border-radius: 1.1rem;
            background: linear-gradient(135deg, rgba(18,58,102,0.92) 0%, rgba(11,31,51,0.95) 70%);
            color: var(--ink-inverse);
            border: 1px solid rgba(255,255,255,0.08);
            box-shadow: 0 22px 48px rgba(5,18,34,0.4);
        }}

        .kgi-card__title {{
            font-size: 0.78rem;
            letter-spacing: 0.12em;
            text-transform: uppercase;
            color: rgba(255,255,255,0.68);
            margin-bottom: 0.75rem;
        }}

        .kgi-card__value {{
            font-size: 1.95rem;
            font-weight: 700;
            margin-bottom: 0.4rem;
        }}

        .kgi-card__delta {{
            font-size: 0.85rem;
            font-weight: 600;
            display: inline-flex;
            align-items: center;
            gap: 0.35rem;
        }}

        .kgi-card__delta--up {{
            color: #8CE99A;
        }}

        .kgi-card__delta--down {{
            color: #FFA8A8;
        }}

        .kgi-card__target {{
            margin-top: 0.5rem;
            font-size: 0.78rem;
            letter-spacing: 0.02em;
            color: rgba(255,255,255,0.78);
        }}

        .kgi-card__target--behind {{
            color: #FFD166;
        }}

        .dashboard-meta {{
            display: inline-flex;
            flex-wrap: wrap;
            gap: 0.5rem;
            margin-bottom: 1.4rem;
        }}

        .dashboard-meta__chip {{
            background: rgba(11,31,51,0.18);
            color: var(--ink-inverse);
            border-radius: 999px;
            padding: 0.3rem 0.85rem;
            font-size: 0.78rem;
            font-weight: 600;
            letter-spacing: 0.02em;
        }}

        .kpi-strip {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 1rem;
            margin-bottom: 1.8rem;
        }}

        .kpi-strip__card {{
            background: var(--surface-elevated);
            border-radius: 0.95rem;
            border: 1px solid rgba(11,31,51,0.1);
            padding: 1rem 1.2rem;
            box-shadow: 0 14px 32px rgba(15,30,46,0.08);
        }}

        .kpi-strip__label {{
            font-size: 0.75rem;
            letter-spacing: 0.1em;
            text-transform: uppercase;
            font-weight: 700;
            color: var(--ink-subtle);
            margin-bottom: 0.35rem;
        }}

        .kpi-strip__value {{
            font-size: 1.25rem;
            font-weight: 700;
            color: var(--ink-strong);
        }}

        .kpi-strip__delta {{
            margin-top: 0.2rem;
            font-size: 0.8rem;
            font-weight: 600;
            color: var(--ink-subtle);
        }}

        .kpi-strip__delta--up {{
            color: #1E5CC3;
        }}

        .kpi-strip__delta--down {{
            color: #C24C1D;
        }}

        .chart-section {{
            background: var(--surface-elevated);
            border-radius: 1rem;
            padding: 1.2rem 1.4rem;
            border: 1px solid rgba(11,31,51,0.08);
            box-shadow: 0 12px 30px rgba(15,30,46,0.06);
            margin-bottom: 1.8rem;
        }}

        .chart-section__header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 0.8rem;
        }}

        .chart-section__title {{
            font-size: 1.05rem;
            font-weight: 700;
            color: var(--ink-strong);
        }}

        .detail-toolbar {{
            display: flex;
            justify-content: flex-end;
            gap: 0.5rem;
            margin-bottom: 0.8rem;
        }}

        .data-status-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
            gap: 1rem;
            margin-top: 1.2rem;
        }}

        .data-status-card {{
            background: var(--surface-elevated);
            border-radius: 1rem;
            border: 1px solid rgba(11,31,51,0.08);
            padding: 1.1rem 1.3rem;
            box-shadow: 0 12px 28px rgba(15,30,46,0.06);
        }}

        .data-status-card__title {{
            font-size: 0.95rem;
            font-weight: 700;
            color: var(--ink-strong);
            margin-bottom: 0.4rem;
        }}

        .data-status-card__meta {{
            font-size: 0.75rem;
            color: var(--ink-subtle);
            margin-bottom: 0.6rem;
        }}

        .data-status-card__body {{
            font-size: 0.85rem;
            color: var(--ink-base);
            margin-bottom: 0.7rem;
            line-height: 1.4;
        }}

        .data-status-card__status {{
            display: inline-flex;
            align-items: center;
            gap: 0.35rem;
            font-weight: 600;
            font-size: 0.78rem;
            border-radius: 999px;
            padding: 0.2rem 0.7rem;
        }}

        .data-status-card__status--ok {{
            background: rgba(55, 178, 77, 0.16);
            color: #2F8F46;
        }}

        .data-status-card__status--warning {{
            background: rgba(255, 170, 64, 0.18);
            color: #C24C1D;
        }}

        .data-status-card__status--error {{
            background: rgba(255, 92, 92, 0.2);
            color: #D1435B;
        }}

        .data-status-card__footnote {{
            font-size: 0.75rem;
            color: var(--ink-subtle);
            margin-top: 0.6rem;
        }}

        .bsc-card {{
            background: linear-gradient(180deg, rgba(255,255,255,0.96) 0%, rgba(242,246,255,0.92) 100%);
            border-radius: 1rem;
            padding: 1.2rem 1.4rem;
            box-shadow: 0 18px 48px rgba(15,30,46,0.18);
            border: 1px solid rgba(11,31,51,0.08);
            color: var(--ink-base);
        }}

        .bsc-card__title {{
            font-size: 0.95rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            color: var(--ink-strong);
            margin-bottom: 0.35rem;
        }}

        .bsc-card__subtitle {{
            font-size: 0.8rem;
            color: var(--ink-subtle);
            margin-bottom: 0.75rem;
        }}

        .bsc-card .stMetric {{
            margin-bottom: 0.55rem;
        }}

        .bsc-card .stMetric label {{
            color: var(--ink-base);
            font-size: 0.85rem;
        }}

        .bsc-card .stMetric div[data-testid="stMetricValue"] {{
            color: var(--ink-strong);
            font-size: 1.55rem;
        }}

        .bsc-card .stMetric div[data-testid="stMetricDelta"] {{
            font-size: 0.85rem;
        }}

        .form-section {{
            background: var(--surface-elevated);
            border-radius: 1rem;
            padding: 1.4rem 1.6rem;
            border: 1px solid rgba(11,31,51,0.1);
            box-shadow: 0 12px 30px rgba(15,30,46,0.06);
            margin-bottom: 1.6rem;
            display: flex;
            flex-direction: column;
            gap: 1rem;
            color: var(--ink-base);
        }}

        .form-section--secondary {{
            background: rgba(230,236,244,0.95);
            border-color: rgba(11,31,51,0.08);
            box-shadow: none;
        }}

        .form-section__title {{
            font-size: 1.1rem;
            font-weight: 700;
            color: var(--ink-strong);
        }}

        .form-section__description {{
            font-size: 0.95rem;
            color: var(--ink-subtle);
            margin: 0;
        }}

        .form-section__status {{
            display: inline-flex;
            align-items: center;
            padding: 0.2rem 0.65rem;
            border-radius: 999px;
            background: rgba(30,92,195,0.15);
            color: var(--color-accent-strong);
            font-size: 0.8rem;
            font-weight: 600;
            letter-spacing: 0.02em;
            gap: 0.3rem;
        }}

        .stepper {{
            display: grid;
            gap: 0.75rem;
            margin: 1.2rem 0 2rem;
        }}

        .stepper__item {{
            display: flex;
            align-items: center;
            gap: 1rem;
            padding: 0.85rem 1.1rem;
            border-radius: 0.95rem;
            border: 1px solid rgba(11,31,51,0.18);
            background: rgba(230,236,244,0.9);
        }}

        .stepper__item--active {{
            border-color: rgba(30,92,195,0.5);
            background: rgba(30,92,195,0.16);
        }}

        .stepper__item--done {{
            border-color: rgba(11,31,51,0.18);
            background: rgba(11,31,51,0.06);
        }}

        .stepper__index {{
            width: 2rem;
            height: 2rem;
            border-radius: 999px;
            background: var(--color-primary);
            color: var(--ink-inverse);
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: 700;
            font-size: 0.95rem;
        }}

        .stepper__item--active .stepper__index {{
            background: var(--color-accent-strong);
        }}

        .stepper__body {{
            flex: 1;
        }}

        .stepper__title {{
            font-size: 1rem;
            font-weight: 700;
            color: var(--ink-strong);
        }}

        .stepper__desc {{
            font-size: 0.9rem;
            color: var(--ink-subtle);
            margin-top: 0.2rem;
        }}

        .stepper__status {{
            font-size: 0.85rem;
            font-weight: 600;
            color: var(--ink-subtle);
        }}

        .stApp main .stButton>button,
        .stApp main .stDownloadButton>button {{
            border-radius: 0.85rem;
            padding: 0.75rem 1.4rem;
            font-weight: 600;
            background: var(--color-accent-strong);
            border: 1px solid #123F7A;
            color: var(--ink-inverse);
            min-height: 48px;
            box-shadow: 0 12px 28px rgba(30,92,195,0.25);
        }}

        .stApp main .stButton>button:hover,
        .stApp main .stDownloadButton>button:hover {{
            background: #174A9C;
        }}

        .stApp main .stButton>button:focus,
        .stApp main .stDownloadButton>button:focus {{
            outline: none;
            box-shadow: 0 0 0 3px var(--focus-outline);
        }}

        .stApp main div[data-baseweb="input"] input,
        .stApp main div[data-baseweb="textarea"] textarea,
        .stApp main div[data-baseweb="select"] > div {{
            border-radius: 0.75rem;
            border: 1px solid rgba(11,31,51,0.18);
            background: #ffffff;
            color: var(--ink-strong);
            min-height: 48px;
            padding: 0.5rem 0.85rem;
            font-size: 0.95rem;
        }}

        .stApp main div[data-baseweb="textarea"] textarea {{
            min-height: 120px;
        }}

        .stApp main div[data-baseweb="input"] input:focus,
        .stApp main div[data-baseweb="textarea"] textarea:focus,
        .stApp main div[data-baseweb="select"]:focus-within > div {{
            border-color: var(--color-accent-strong);
            box-shadow: 0 0 0 3px rgba(30,92,195,0.25);
        }}

        .stApp main div[data-testid="stFileUploader"] section[data-testid="stFileUploaderDropzone"] {{
            border-radius: 0.9rem;
            border: 1px dashed rgba(11,31,51,0.3);
            background: rgba(230,236,244,0.45);
        }}

        .stApp main div[data-testid="stFileUploader"] section[data-testid="stFileUploaderDropzone"] * {{
            color: var(--ink-base);
        }}

        .stApp main div[data-testid="stDataFrame"] > div {{
            border-radius: 0.85rem;
            border: 1px solid rgba(11,31,51,0.12);
            background: var(--surface-elevated);
            color: var(--ink-base);
        }}

        .stApp main div[data-testid="stDataEditor"] {{
            border-radius: 0.85rem;
            border: 1px solid rgba(11,31,51,0.12);
            padding: 0.5rem;
            background: var(--surface-elevated);
            color: var(--ink-base);
        }}

        @media (max-width: 960px) {{
            main .block-container {{
                padding: 1.6rem 1.1rem 2.6rem;
            }}

            .hero-panel {{
                padding: 1.8rem 1.6rem;
            }}

            .hero-meta {{
                flex-direction: column;
                align-items: flex-start;
            }}

            .main-nav-block div[role="radiogroup"] {{
                flex-direction: column;
                align-items: stretch;
            }}

            .main-nav-block div[role="radiogroup"] label {{
                width: 100%;
                text-align: center;
            }}

            div[data-testid="column"] {{
                width: 100% !important;
                padding-right: 0 !important;
            }}

            .stepper__item {{
                flex-direction: column;
                align-items: flex-start;
            }}

            .stepper__status {{
                margin-top: 0.35rem;
            }}
        }}

        @media (max-width: 600px) {{
            .surface-card {{
                padding: 1.2rem 1.3rem;
            }}

            .form-section {{
                padding: 1.1rem 1.2rem;
            }}

            .hero-title {{
                font-size: 1.6rem;
            }}

            .stApp main .stButton>button,
            .stApp main .stDownloadButton>button {{
                width: 100%;
            }}
        }}

        .main-nav-block div[role="radiogroup"] {{
            gap: 0.75rem;
            flex-wrap: wrap;
        }}

        .main-nav-block div[role="radiogroup"] label {{
            border-radius: 999px;
            padding: 0.35rem 0.9rem;
            border: 1px solid rgba(15,30,46,0.16);
            background: rgba(230,236,244,0.85);
            font-weight: 600;
            color: var(--ink-strong);
        }}

        .main-nav-block div[role="radiogroup"] label:hover {{
            border-color: rgba(30,92,195,0.45);
        }}

        .breadcrumb-trail {{
            color: var(--ink-muted);
            margin-bottom: 1.2rem;
            font-size: 0.9rem;
        }}

        .alert-banner {{
            border-radius: 0.95rem;
            padding: 1.1rem 1.3rem;
            margin-bottom: 1.6rem;
            border: 1px solid transparent;
        }}

        .alert-banner--warning {{
            background: #FFF3E9;
            border-color: #F0B08A;
            color: #9A3A0B;
        }}

        .alert-banner--ok {{
            background: #E3EDFF;
            border-color: #9CB6F5;
            color: #113E79;
        }}

        .alert-banner__title {{
            font-weight: 700;
            margin-bottom: 0.4rem;
        }}

        .alert-banner ul {{
            margin: 0.2rem 0 0;
            padding-left: 1.2rem;
        }}

        .search-card {{
            padding-bottom: 1rem;
        }}

        .search-card .search-title {{
            font-weight: 700;
            font-size: 1.1rem;
            color: var(--ink-strong);
        }}

        .search-card div[data-testid="stTextInput"] {{
            margin-top: 0.6rem;
        }}

        .search-card input {{
            border-radius: 0.75rem;
            border: 1px solid rgba(11,31,51,0.18);
            padding: 0.6rem 0.9rem;
            background: #ffffff;
            color: var(--ink-strong);
        }}

        .search-card input:focus {{
            border-color: var(--color-accent-strong);
            box-shadow: 0 0 0 2px rgba(30,92,195,0.22);
        }}

        hr {{
            border-color: #d8e1ef;
        }}

        div[data-testid="stMetric"] {{
            background: var(--surface-elevated);
            border-radius: 0.9rem;
            padding: 1.1rem;
            border: 1px solid rgba(11,31,51,0.12);
            box-shadow: 0 12px 30px rgba(15,30,46,0.05);
            color: var(--ink-base);
        }}

        div[data-testid="stMetricLabel"] {{
            color: var(--ink-subtle);
            font-weight: 600;
        }}

        div[data-testid="stMetricValue"] {{
            color: var(--ink-strong);
            font-weight: 700;
        }}

        div[data-testid="stMetricDelta"] {{
            color: var(--color-accent-strong);
            font-weight: 600;
        }}

        .status-pill {{
            display: inline-flex;
            align-items: center;
            padding: 0.25rem 0.6rem;
            border-radius: 999px;
            font-size: 0.8rem;
            font-weight: 600;
            letter-spacing: 0.02em;
            margin-bottom: 0.35rem;
            gap: 0.3rem;
        }}

        .status-pill--ok {{
            background: #E0EDFF;
            color: #113E79;
        }}

        .status-pill--warning {{
            background: #FFF4D7;
            color: #7A4E00;
        }}

        .status-pill--error {{
            background: #FFE3D6;
            color: #7A1C00;
        }}

        section[data-testid="stSidebar"] .sidebar-meta {{
            font-size: 0.8rem;
            color: var(--ink-subtle);
            margin-bottom: 0.75rem;
        }}

        section[data-testid="stSidebar"] .sidebar-meta--status {{
            color: #1E5CC3;
            font-weight: 600;
        }}

        .stAlert>div {{
            border-radius: 0.95rem;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def load_data(
    use_sample: bool,
    uploaded_sales: Dict[str, List],
    cost_file,
    subscription_file,
    *,
    automated_sales: Optional[Dict[str, pd.DataFrame]] = None,
    automated_reports: Optional[List[ValidationReport]] = None,
) -> Dict[str, Any]:
    """アップロード状況に応じてデータを読み込む。"""
    # TODO: アップロードされたExcelファイルを読み込んでデータフレームに統合
    sales_frames: List[pd.DataFrame] = []
    cost_frames: List[pd.DataFrame] = []
    subscription_frames: List[pd.DataFrame] = []
    validation_report = ValidationReport()

    if use_sample:
        sales_frames.append(generate_sample_sales_data())
        cost_frames.append(generate_sample_cost_data())
        subscription_frames.append(generate_sample_subscription_data())

    loaded_sales, uploaded_validation = load_sales_files(uploaded_sales)
    validation_report.extend(uploaded_validation)
    if not loaded_sales.empty:
        sales_frames.append(loaded_sales)

    if cost_file:
        cost_frames.append(load_cost_workbook(cost_file))
    if subscription_file:
        subscription_frames.append(load_subscription_workbook(subscription_file))

    if automated_sales:
        for df in automated_sales.values():
            if df is not None and not df.empty:
                sales_frames.append(df)

    if automated_reports:
        for report in automated_reports:
            validation_report.extend(report)

    sales_df = pd.concat(sales_frames, ignore_index=True) if sales_frames else pd.DataFrame()
    cost_df = pd.concat(cost_frames, ignore_index=True) if cost_frames else pd.DataFrame()
    subscription_df = pd.concat(subscription_frames, ignore_index=True) if subscription_frames else pd.DataFrame()

    if not sales_df.empty:
        combined_duplicates = detect_duplicate_rows(sales_df)
        if not combined_duplicates.empty:
            before = len(validation_report.duplicate_rows)
            validation_report.add_duplicates(combined_duplicates)
            if len(validation_report.duplicate_rows) > before:
                validation_report.add_message(
                    "warning",
                    f"全チャネルの売上データで重複しているレコードが{len(combined_duplicates)}件検出されました。",
                    count=int(combined_duplicates.shape[0]),
                )

    return {
        "sales": sales_df,
        "cost": cost_df,
        "subscription": subscription_df,
        "sales_validation": validation_report,
    }


def apply_filters(
    sales_df: pd.DataFrame,
    channels: List[str],
    date_range: List[date],
    categories: Optional[List[str]] = None,
    stores: Optional[List[str]] = None,
) -> pd.DataFrame:
    """サイドバーで選択した条件をもとに売上データを抽出する。"""
    if sales_df.empty:
        return sales_df

    filtered = sales_df.copy()
    if stores and "store" in filtered.columns:
        if isinstance(stores, (str, bytes)):
            stores = [stores]
        filtered = filtered[filtered["store"].isin(stores)]
    if channels:
        filtered = filtered[filtered["channel"].isin(channels)]
    if categories:
        filtered = filtered[filtered["category"].isin(categories)]
    if date_range:
        start_date = pd.to_datetime(date_range[0]) if date_range[0] else filtered["order_date"].min()
        end_date = pd.to_datetime(date_range[1]) if date_range[1] else filtered["order_date"].max()
        filtered = filtered[(filtered["order_date"] >= start_date) & (filtered["order_date"] <= end_date)]
    return filtered


def download_button_from_df(label: str, df: pd.DataFrame, filename: str) -> None:
    """データフレームをCSVとしてダウンロードするボタンを配置。"""
    if df is None or df.empty:
        return
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    clicked = st.download_button(label, buffer.getvalue(), file_name=filename, mime="text/csv")
    if clicked:
        display_state_message("csv_done", action_key=f"csv_done_{filename}")


def display_state_message(
    state: str,
    *,
    format_kwargs: Optional[Dict[str, Any]] = None,
    action: Optional[Callable[[], None]] = None,
    action_label: Optional[str] = None,
    action_key: Optional[str] = None,
    container: Optional[Any] = None,
) -> None:
    """状態に応じたフィードバックメッセージを表示する。"""

    config = STATE_MESSAGES.get(state)
    if not config:
        return

    target = container or st
    format_kwargs = format_kwargs or {}
    message_text = config["text"].format(**format_kwargs)
    message_type = config.get("type", "info")
    display_fn = getattr(target, message_type, target.info)
    display_fn(message_text)

    label = action_label or config.get("action_label")
    if action and label:
        button_kwargs = {"key": action_key} if action_key else {}
        if target.button(label, **button_kwargs):
            action()


def suggest_default_period(min_date: date, max_date: date) -> Tuple[date, date]:
    """営業日に応じた推奨期間（基本は当月）を返す。"""

    today = date.today()
    if today < min_date:
        today = min_date
    if today > max_date:
        today = max_date

    closing_threshold = 3
    if today.day <= closing_threshold:
        reference = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
    else:
        reference = today.replace(day=1)

    start_day = reference
    last_day = calendar.monthrange(reference.year, reference.month)[1]
    end_day = reference.replace(day=last_day)

    start_day = max(start_day, min_date)
    end_day = min(end_day, max_date)
    if start_day > end_day:
        start_day, end_day = min_date, max_date
    return start_day, end_day


def reset_filters(defaults: Dict[str, Any]) -> None:
    """フィルタ関連のセッション状態を初期値に戻す。"""

    for key, value in defaults.items():
        if isinstance(value, list):
            st.session_state[key] = list(value)
        else:
            st.session_state[key] = value
    st.experimental_rerun()


def jump_to_section(main_label: str, section_label: Optional[str] = None) -> None:
    """ナビゲーションの選択を強制的に切り替えてリロードする。"""

    st.session_state["main_nav"] = main_label
    if section_label:
        st.session_state[f"sub_nav_{main_label}"] = section_label
    st.experimental_rerun()


def build_filter_signature(
    store: Optional[str],
    channels: Optional[List[str]],
    categories: Optional[List[str]],
    date_range: Any,
    freq_label: str,
) -> Tuple[Any, ...]:
    """フィルタの状態を比較可能なタプルに変換する。"""

    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        start_value, end_value = date_range
    else:
        start_value = end_value = date_range

    def _normalize_date(value: Any) -> Optional[str]:
        if value is None:
            return None
        if hasattr(value, "isoformat"):
            return value.isoformat()
        return str(value)

    return (
        store or "all",
        tuple(channels or []),
        tuple(categories or []),
        _normalize_date(start_value),
        _normalize_date(end_value),
        freq_label,
    )


def normalize_date_input(value: Any) -> Optional[date]:
    """様々な入力値をdate型に揃える。"""

    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if hasattr(value, "to_pydatetime"):
        try:
            return value.to_pydatetime().date()
        except Exception:
            return None
    try:
        return pd.to_datetime(value).date()
    except Exception:
        return None


def prepare_plan_table(
    data: Any,
    columns: List[str],
    numeric_columns: Optional[List[str]] = None,
) -> pd.DataFrame:
    """ウィザード用の表を指定の列構成と数値型に整形する。"""

    numeric_columns = numeric_columns or []
    if isinstance(data, pd.DataFrame):
        df = data.copy()
    elif data is None or (hasattr(data, "__len__") and len(data) == 0):
        df = pd.DataFrame(columns=columns)
    else:
        df = pd.DataFrame(data)

    if df.empty and not list(df.columns):
        df = pd.DataFrame(columns=columns)

    df.columns = [str(col).strip() for col in df.columns]
    for column in columns:
        if column not in df.columns:
            df[column] = 0.0 if column in numeric_columns else ""
    df = df[columns]
    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0.0)
    if columns:
        label_column = columns[0]
        df[label_column] = df[label_column].astype(str).str.strip()
    return df


def reset_plan_wizard_state() -> None:
    """経営計画ウィザードのセッション状態を初期化する。"""

    default_start = date.today().replace(day=1)
    st.session_state["plan_wizard"] = {
        "current_step": 0,
        "completed": False,
        "basic_info": {
            "company_name": "",
            "preparer": "",
            "fiscal_year_start": default_start,
            "plan_period_months": 12,
            "target_margin": 15.0,
            "strategic_focus": "",
        },
        "sales_table": pd.DataFrame(columns=SALES_PLAN_COLUMNS),
        "expense_table": pd.DataFrame(columns=EXPENSE_PLAN_COLUMNS),
        "sales_import_hash": None,
        "expense_import_hash": None,
        "sales_import_feedback": None,
        "expense_import_feedback": None,
        "metrics": {},
    }
    for key in [
        "plan_sales_editor",
        "plan_expense_editor",
        "plan_sales_common_select",
        "plan_expense_common_select",
    ]:
        st.session_state.pop(key, None)


def ensure_plan_wizard_state() -> Dict[str, Any]:
    """経営計画ウィザード用のセッション情報を返す。"""

    if "plan_wizard" not in st.session_state:
        reset_plan_wizard_state()

    state: Dict[str, Any] = st.session_state["plan_wizard"]
    state.setdefault("current_step", 0)
    state.setdefault("completed", False)
    state.setdefault("basic_info", {})

    basic_info = state["basic_info"]
    if not isinstance(basic_info.get("fiscal_year_start"), date):
        basic_info["fiscal_year_start"] = date.today().replace(day=1)
    basic_info.setdefault("company_name", "")
    basic_info.setdefault("preparer", "")
    basic_info.setdefault("plan_period_months", 12)
    basic_info.setdefault("target_margin", 15.0)
    basic_info.setdefault("strategic_focus", "")

    state["sales_table"] = prepare_plan_table(
        state.get("sales_table"), SALES_PLAN_COLUMNS, ["月次売上"]
    )
    state["expense_table"] = prepare_plan_table(
        state.get("expense_table"), EXPENSE_PLAN_COLUMNS, ["月次金額"]
    )
    state.setdefault("sales_import_hash", None)
    state.setdefault("expense_import_hash", None)
    state.setdefault("sales_import_feedback", None)
    state.setdefault("expense_import_feedback", None)
    state.setdefault("metrics", {})
    return state


def append_plan_rows(
    df: pd.DataFrame,
    label_column: str,
    numeric_column: str,
    default_values: Optional[Dict[str, Any]],
    items: List[str],
) -> Tuple[pd.DataFrame, int]:
    """プルダウンで選択した項目を既存の表に追加する。"""

    if df is None or df.empty:
        df = pd.DataFrame(columns=[label_column, numeric_column])

    existing = set(df[label_column].astype(str).str.strip())
    new_rows: List[Dict[str, Any]] = []
    for item in items:
        normalized = str(item).strip()
        if normalized and normalized not in existing:
            row = {label_column: normalized, numeric_column: 0.0}
            if default_values:
                for key, value in default_values.items():
                    row[key] = value
            new_rows.append(row)
            existing.add(normalized)

    if new_rows:
        df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)

    return df, len(new_rows)


def normalize_plan_import(
    df: pd.DataFrame,
    column_candidates: Dict[str, List[str]],
    required_columns: List[str],
    numeric_columns: List[str],
) -> pd.DataFrame:
    """CSV取り込み時に列名を標準化し、必要列を抽出する。"""

    if df is None or df.empty:
        raise ValueError("CSVにデータがありません。")

    working = df.copy()
    working.columns = [str(col).strip() for col in working.columns]
    rename_map: Dict[str, str] = {}
    for target, candidates in column_candidates.items():
        found = next((col for col in candidates if col in working.columns), None)
        if found:
            rename_map[found] = target

    missing = [col for col in required_columns if col not in rename_map.values()]
    if missing:
        raise ValueError(
            f"必要な列({', '.join(missing)})がCSV内に見つかりませんでした。列名を確認してください。"
        )

    normalized = working[list(rename_map.keys())].rename(columns=rename_map)
    label_column = required_columns[0]
    normalized = normalized.dropna(subset=[label_column])
    normalized[label_column] = normalized[label_column].astype(str).str.strip()
    normalized = normalized[normalized[label_column] != ""]
    for column in numeric_columns:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce").fillna(0.0)
    for target in column_candidates.keys():
        if target not in normalized.columns:
            normalized[target] = "" if target not in numeric_columns else 0.0
    return normalized


def import_plan_csv(
    file_bytes: bytes,
    column_candidates: Dict[str, List[str]],
    required_columns: List[str],
    numeric_columns: List[str],
) -> Tuple[pd.DataFrame, Optional[str]]:
    """会計ソフトからエクスポートしたCSVを標準形式に変換する。"""

    if not file_bytes:
        return pd.DataFrame(columns=required_columns), "CSVファイルが空です。"

    last_error: Optional[str] = None
    for encoding in ["utf-8-sig", "utf-8", "cp932", "shift_jis"]:
        try:
            text = file_bytes.decode(encoding)
            buffer = io.StringIO(text)
            raw_df = pd.read_csv(buffer)
            normalized = normalize_plan_import(
                raw_df, column_candidates, required_columns, numeric_columns
            )
            return normalized, None
        except UnicodeDecodeError:
            last_error = f"文字コード{encoding}での読み込みに失敗しました。"
            continue
        except pd.errors.ParserError:
            last_error = "CSVの解析に失敗しました。フォーマットを確認してください。"
            continue
        except ValueError as exc:
            return pd.DataFrame(columns=required_columns), str(exc)

    return pd.DataFrame(columns=required_columns), last_error or "CSVの読み込みに失敗しました。"


def calculate_plan_metrics_from_state(state: Dict[str, Any]) -> Dict[str, Any]:
    """売上・経費入力から主要な財務指標を算出する。"""

    sales_df = prepare_plan_table(state.get("sales_table"), SALES_PLAN_COLUMNS, ["月次売上"])
    expense_df = prepare_plan_table(
        state.get("expense_table"), EXPENSE_PLAN_COLUMNS, ["月次金額"]
    )
    state["sales_table"] = sales_df
    state["expense_table"] = expense_df

    info = state.get("basic_info", {})
    period_months = int(info.get("plan_period_months") or 0)
    monthly_sales = float(sales_df["月次売上"].sum()) if not sales_df.empty else 0.0
    monthly_expenses = float(expense_df["月次金額"].sum()) if not expense_df.empty else 0.0
    monthly_profit = monthly_sales - monthly_expenses
    margin = monthly_profit / monthly_sales if monthly_sales else np.nan
    target_margin_pct = float(info.get("target_margin") or 0.0)
    margin_gap_pct = (margin * 100 - target_margin_pct) if monthly_sales else np.nan

    metrics = {
        "monthly_sales": monthly_sales,
        "monthly_expenses": monthly_expenses,
        "monthly_profit": monthly_profit,
        "monthly_margin": margin,
        "annual_sales": monthly_sales * period_months,
        "annual_expenses": monthly_expenses * period_months,
        "annual_profit": monthly_profit * period_months,
        "target_margin_pct": target_margin_pct,
        "margin_gap_pct": margin_gap_pct,
        "period_months": period_months,
        "burn_rate": monthly_expenses - monthly_sales,
    }
    state["metrics"] = metrics
    return metrics


def build_plan_summary_df(metrics: Dict[str, Any]) -> pd.DataFrame:
    """計画の要約表を作成する。"""

    rows: List[Dict[str, Any]] = [
        {
            "指標": "売上",
            "月次計画額": metrics.get("monthly_sales", 0.0),
            "年間計画額": metrics.get("annual_sales", 0.0),
            "指標値": np.nan,
        },
        {
            "指標": "経費",
            "月次計画額": metrics.get("monthly_expenses", 0.0),
            "年間計画額": metrics.get("annual_expenses", 0.0),
            "指標値": np.nan,
        },
        {
            "指標": "営業利益",
            "月次計画額": metrics.get("monthly_profit", 0.0),
            "年間計画額": metrics.get("annual_profit", 0.0),
            "指標値": np.nan,
        },
        {
            "指標": "月次バーンレート (費用-売上)",
            "月次計画額": metrics.get("burn_rate", 0.0),
            "年間計画額": metrics.get("burn_rate", 0.0)
            * metrics.get("period_months", 0),
            "指標値": np.nan,
        },
    ]

    margin = metrics.get("monthly_margin")
    if margin is not None and np.isfinite(margin):
        rows.append(
            {
                "指標": "営業利益率",
                "月次計画額": np.nan,
                "年間計画額": np.nan,
                "指標値": margin * 100,
            }
        )

    margin_gap = metrics.get("margin_gap_pct")
    if margin_gap is not None and np.isfinite(margin_gap):
        rows.append(
            {
                "指標": "目標比差分 (pt)",
                "月次計画額": np.nan,
                "年間計画額": np.nan,
                "指標値": margin_gap,
            }
        )

    return pd.DataFrame(rows)


def compute_actual_reference(actual_sales: Optional[pd.DataFrame]) -> Dict[str, float]:
    """実績データから平均売上・利益などを算出して比較指標を返す。"""

    if actual_sales is None or actual_sales.empty:
        return {}
    if "order_date" not in actual_sales.columns or "sales_amount" not in actual_sales.columns:
        return {}

    working = actual_sales.copy()
    working["order_month"] = working["order_date"].dt.to_period("M")
    monthly_sales = working.groupby("order_month")["sales_amount"].sum()
    reference: Dict[str, float] = {}
    if not monthly_sales.empty:
        reference["monthly_sales_avg"] = float(monthly_sales.mean())

    profit_column = None
    if "net_gross_profit" in working.columns:
        profit_column = "net_gross_profit"
    elif "gross_profit" in working.columns:
        profit_column = "gross_profit"

    if profit_column:
        monthly_profit = working.groupby("order_month")[profit_column].sum()
        if not monthly_profit.empty:
            reference["monthly_profit_avg"] = float(monthly_profit.mean())
            sales_avg = reference.get("monthly_sales_avg")
            if sales_avg:
                reference["margin_avg"] = reference["monthly_profit_avg"] / sales_avg

    return reference


def validate_plan_basic_info(info: Dict[str, Any]) -> Tuple[bool, List[str], List[str]]:
    """基本情報入力の妥当性を確認する。"""

    errors: List[str] = []
    warnings: List[str] = []

    if not info.get("company_name", "").strip():
        errors.append("事業所名を入力してください。")
    if not isinstance(info.get("fiscal_year_start"), date):
        errors.append("計画開始月を選択してください。")

    period = int(info.get("plan_period_months") or 0)
    if period <= 0:
        errors.append("計画期間は1ヶ月以上を指定してください。")

    if not info.get("preparer", "").strip():
        warnings.append("作成担当者を入力すると共有がスムーズになります。")

    target_margin = float(info.get("target_margin") or 0.0)
    if target_margin < 0:
        errors.append("目標利益率は0%以上で設定してください。")
    elif target_margin > 80:
        warnings.append("目標利益率が高すぎる可能性があります。")

    return len(errors) == 0, errors, warnings


def validate_plan_sales(df: pd.DataFrame) -> Tuple[bool, List[str], List[str]]:
    """売上予測入力の妥当性を確認する。"""

    errors: List[str] = []
    warnings: List[str] = []

    if df is None or df.empty:
        errors.append("売上予測を1件以上入力してください。")
        return False, errors, warnings

    if "項目" not in df.columns or "月次売上" not in df.columns:
        errors.append("売上予測の列構成が不正です。")
        return False, errors, warnings

    empty_label = df["項目"].astype(str).str.strip() == ""
    if empty_label.any():
        errors.append("空欄の売上項目があります。名称を入力してください。")

    negative = df["月次売上"] < 0
    if negative.any():
        errors.append("売上金額は0以上で入力してください。")

    zero_rows = df["月次売上"] == 0
    if zero_rows.any():
        warnings.append("0円の売上項目があります。必要でなければ削除してください。")

    duplicates = df["項目"].astype(str).str.strip().duplicated()
    if duplicates.any():
        warnings.append("同名の売上項目が複数あります。集計が重複する可能性があります。")

    return len(errors) == 0, errors, warnings


def validate_plan_expenses(df: pd.DataFrame) -> Tuple[bool, List[str], List[str]]:
    """経費計画入力の妥当性を確認する。"""

    errors: List[str] = []
    warnings: List[str] = []

    if df is None or df.empty:
        errors.append("経費計画を1件以上入力してください。")
        return False, errors, warnings

    if "費目" not in df.columns or "月次金額" not in df.columns:
        errors.append("経費計画の列構成が不正です。")
        return False, errors, warnings

    empty_label = df["費目"].astype(str).str.strip() == ""
    if empty_label.any():
        errors.append("空欄の経費科目があります。名称を入力してください。")

    negative = df["月次金額"] < 0
    if negative.any():
        errors.append("経費金額は0以上で入力してください。")

    zero_rows = df["月次金額"] == 0
    if zero_rows.any():
        warnings.append("0円の経費項目があります。必要でなければ削除してください。")

    if "区分" in df.columns and (df["区分"].astype(str).str.strip() == "").any():
        warnings.append("区分が未選択の経費があります。")

    return len(errors) == 0, errors, warnings


def validate_plan_metrics(metrics: Dict[str, Any]) -> Tuple[bool, List[str], List[str]]:
    """財務指標計算ステップの妥当性を確認する。"""

    errors: List[str] = []
    warnings: List[str] = []

    if not metrics:
        errors.append("売上と経費の入力を完了してください。")
        return False, errors, warnings

    if metrics.get("monthly_sales", 0.0) <= 0:
        errors.append("売上予測が未入力または0円のため、指標を計算できません。")

    if metrics.get("monthly_expenses", 0.0) < 0:
        errors.append("経費金額が不正です。")

    if metrics.get("period_months", 0) <= 0:
        errors.append("計画期間を見直してください。")

    if (
        metrics.get("monthly_sales", 0.0) > 0
        and metrics.get("monthly_profit", 0.0) < 0
    ):
        warnings.append("月次営業利益がマイナスです。コスト構成を確認してください。")

    margin_gap = metrics.get("margin_gap_pct")
    if margin_gap is not None and np.isfinite(margin_gap) and margin_gap < 0:
        warnings.append("計画上の利益率が目標を下回っています。")

    return len(errors) == 0, errors, warnings


def render_instruction_popover(label: str, content: str) -> None:
    """ポップオーバーまたはエクスパンダーで操作ガイドを表示する。"""

    popover_fn = getattr(st, "popover", None)
    if callable(popover_fn):
        with popover_fn(label):
            st.markdown(content)
    else:
        with st.expander(label):
            st.markdown(content)


@contextmanager
def form_section(
    title: Optional[str],
    description: Optional[str] = None,
    *,
    tone: str = "primary",
) -> None:
    """フォーム入力をカード化し、余白と階層を整える。"""

    classes = ["form-section"]
    if tone and tone != "primary":
        classes.append(f"form-section--{tone}")

    with st.container():
        st.markdown(
            f"<div class='{ ' '.join(classes) }'>", unsafe_allow_html=True
        )
        if title:
            st.markdown(
                f"<div class='form-section__title'>{html.escape(title)}</div>",
                unsafe_allow_html=True,
            )
        if description:
            st.markdown(
                f"<p class='form-section__description'>{html.escape(description)}</p>",
                unsafe_allow_html=True,
            )
        try:
            yield
        finally:
            st.markdown("</div>", unsafe_allow_html=True)


def render_plan_stepper(current_step: int) -> None:
    """ウィザードの進行状況を視覚的なタイムラインで表示する。"""

    items: List[str] = []
    total_steps = len(PLAN_WIZARD_STEPS)
    for idx, step in enumerate(PLAN_WIZARD_STEPS):
        if idx < current_step:
            state_class = "stepper__item stepper__item--done"
            status = "完了"
        elif idx == current_step:
            state_class = "stepper__item stepper__item--active"
            status = "進行中"
        else:
            state_class = "stepper__item"
            status = "未着手"

        items.append(
            """
            <div class="{state_class}">
                <div class="stepper__index">{index}</div>
                <div class="stepper__body">
                    <div class="stepper__title">{title}</div>
                    <div class="stepper__desc">{description}</div>
                </div>
                <div class="stepper__status">{status}</div>
            </div>
            """.format(
                state_class=state_class,
                index=idx + 1,
                title=html.escape(step["title"]),
                description=html.escape(step.get("description", "")),
                status=status,
            )
        )

    st.markdown(
        f"<div class='stepper'>{''.join(items)}</div>",
        unsafe_allow_html=True,
    )


def render_plan_step_basic_info(state: Dict[str, Any]) -> None:
    """ウィザードの基本情報入力ステップを描画する。"""

    info = state["basic_info"]
    render_instruction_popover(
        "基本情報の入力ガイド",
        """
- 会社名や担当者などの基本情報を入力します。
- 計画開始月と期間は年間換算の計算に利用されます。
- 目標利益率を設定すると達成状況のチェックが自動化されます。
""",
    )

    with form_section(
        "事業所と担当者",
        "共有時に識別される基本情報を先に押さえておきます。",
    ):
        info["company_name"] = st.text_input(
            "事業所名",
            value=info.get("company_name", ""),
            key="plan_company_name",
            help="経営計画書に記載する正式な社名または店舗名を入力してください。",
        )
        info["preparer"] = st.text_input(
            "作成担当者",
            value=info.get("preparer", ""),
            key="plan_preparer",
            help="計画の作成者または責任者を入力すると共有がスムーズになります。",
        )

    with form_section(
        "計画期間と利益目標",
        "期間と目標値は後続のシミュレーションに自動反映されます。",
    ):
        col1, col2 = st.columns(2)
        default_start = info.get("fiscal_year_start")
        if not isinstance(default_start, date):
            default_start = date.today().replace(day=1)
        info["fiscal_year_start"] = col1.date_input(
            "計画開始月",
            value=default_start,
            key="plan_fiscal_start",
            help="事業計画の初月を選択します。月次予測の起点として使用されます。",
        )

        period_default = int(info.get("plan_period_months") or 12)
        info["plan_period_months"] = col2.slider(
            "計画期間（月）",
            min_value=3,
            max_value=36,
            value=period_default,
            step=1,
            key="plan_period_months",
            help="3〜36ヶ月の範囲で計画期間を指定します。",
        )

        target_margin_default = float(info.get("target_margin") or 15.0)
        info["target_margin"] = col1.slider(
            "目標営業利益率(%)",
            min_value=0.0,
            max_value=50.0,
            value=target_margin_default,
            step=0.5,
            key="plan_target_margin",
            help="経営チームが目指す営業利益率を設定します。",
        )

    with form_section(
        "重点施策メモ",
        "将来の振り返りで意図を再確認できるよう、戦略メモを残せます。",
        tone="secondary",
    ):
        st.markdown(
            "<span class='form-section__status'>任意入力</span>",
            unsafe_allow_html=True,
        )
        info["strategic_focus"] = st.text_area(
            "重点施策メモ",
            value=info.get("strategic_focus", ""),
            key="plan_strategic_focus",
            help="成長戦略や重点施策をメモできます。後続ステップの指標と合わせて検討してください。",
        )

    st.caption(
        "段階的なウィザードと統一されたツールチップを用いたインターフェースは、Nielsen Norman Groupの調査 (moldstud.com) によればユーザー満足度を約20%向上させます。"
    )


def render_plan_step_sales(state: Dict[str, Any], context: Dict[str, Any]) -> None:
    """売上予測入力ステップを描画する。"""

    state["sales_table"] = prepare_plan_table(
        state.get("sales_table"), SALES_PLAN_COLUMNS, ["月次売上"]
    )

    render_instruction_popover(
        "売上入力のヒント",
        """
- 会計ソフトから出力したCSVを取り込むと科目と金額を自動で整形します。
- テンプレートを読み込めば、よくあるチャネル構成を一度で入力できます。
- プルダウンから追加した科目は0円で挿入されるため、数値を上書きするだけで済みます。
""",
    )

    with form_section(
        "売上データの取り込み",
        "CSVやAPI連携からエクスポートしたデータを一括で整形します。",
    ):
        uploaded = st.file_uploader(
            "会計ソフトの売上CSVを取り込む",
            type=["csv"],
            key="plan_sales_upload",
            help="勘定奉行やfreeeなどの会計ソフトから出力したCSVをアップロードすると自動でマッピングされます。",
        )
        if uploaded is not None:
            file_bytes = uploaded.getvalue()
            file_hash = hashlib.md5(file_bytes).hexdigest()
            if file_hash and state.get("sales_import_hash") != file_hash:
                imported_df, error = import_plan_csv(
                    file_bytes,
                    SALES_IMPORT_CANDIDATES,
                    ["項目", "月次売上"],
                    ["月次売上"],
                )
                if error:
                    state["sales_import_feedback"] = ("error", error)
                else:
                    state["sales_table"] = prepare_plan_table(
                        imported_df, SALES_PLAN_COLUMNS, ["月次売上"]
                    )
                    state["sales_import_feedback"] = (
                        "success",
                        f"CSVから{len(state['sales_table'])}件の売上科目を読み込みました。",
                    )
                state["sales_import_hash"] = file_hash

        feedback = state.get("sales_import_feedback")
        if feedback:
            level, message = feedback
            if level == "error":
                st.error(message)
            elif level == "success":
                st.success(message)

    with form_section(
        "テンプレートと科目の追加",
        "よく使うチャネル構成を呼び出し、入力の手戻りを防ぎます。",
        tone="secondary",
    ):
        template_cols = st.columns([3, 1])
        template_options = ["テンプレートを選択"] + list(SALES_PLAN_TEMPLATES.keys())
        selected_template = template_cols[0].selectbox(
            "売上テンプレートを適用",
            options=template_options,
            key="plan_sales_template",
            help="売上の典型的な構成をテンプレートとして呼び出せます。",
        )
        if template_cols[1].button("読み込む", key="plan_apply_sales_template"):
            if selected_template != "テンプレートを選択":
                template_df = pd.DataFrame(SALES_PLAN_TEMPLATES[selected_template])
                state["sales_table"] = prepare_plan_table(
                    template_df, SALES_PLAN_COLUMNS, ["月次売上"]
                )
                state["sales_import_feedback"] = (
                    "success",
                    f"テンプレート『{selected_template}』を適用しました。",
                )

        common_candidates = list(
            dict.fromkeys(COMMON_SALES_ITEMS + context.get("category_options", []))
        )
        selected_common = st.multiselect(
            "よく使う売上科目を追加",
            options=common_candidates,
            key="plan_sales_common_select",
            help="複数選択すると、0円の行として追加され数値だけ入力すれば完了です。",
        )
        if st.button("選択した科目を追加", key="plan_add_sales_common"):
            state["sales_table"], added = append_plan_rows(
                state["sales_table"],
                "項目",
                "月次売上",
                {"チャネル": ""},
                selected_common,
            )
            if added:
                st.success(f"{added}件の売上科目を追加しました。")
            else:
                st.info("新しく追加できる科目がありませんでした。")
            st.session_state["plan_sales_common_select"] = []

    with form_section(
        "売上計画の編集",
        "取り込んだ行はここで月次金額とチャネルを整えます。",
    ):
        channel_options = list(
            dict.fromkeys(context.get("channel_options", PLAN_CHANNEL_OPTIONS_BASE))
        )
        channel_select_options = [""] + channel_options
        column_module = getattr(st, "column_config", None)
        column_config = {}
        if column_module:
            column_config["項目"] = column_module.TextColumn(
                "項目",
                help="売上項目の名称を入力します。",
            )
            column_config["月次売上"] = column_module.NumberColumn(
                "月次売上 (円)",
                min_value=0.0,
                step=50_000.0,
                help="各項目の月次売上計画を入力します。",
            )
            if hasattr(column_module, "SelectboxColumn"):
                column_config["チャネル"] = column_module.SelectboxColumn(
                    "チャネル/メモ",
                    options=channel_select_options,
                    help="主要チャネルやメモを選択・入力します。",
                )
            else:
                column_config["チャネル"] = column_module.TextColumn(
                    "チャネル/メモ",
                    help="主要チャネルやメモを入力します。",
                )
        else:
            column_config = None

        editor_kwargs: Dict[str, Any] = {
            "num_rows": "dynamic",
            "use_container_width": True,
            "hide_index": True,
        }
        if column_config:
            editor_kwargs["column_config"] = column_config

        sales_editor_value = st.data_editor(
            state["sales_table"],
            key="plan_sales_editor",
            **editor_kwargs,
        )
        state["sales_table"] = prepare_plan_table(
            sales_editor_value, SALES_PLAN_COLUMNS, ["月次売上"]
        )

        monthly_total = (
            float(state["sales_table"]["月次売上"].sum())
            if not state["sales_table"].empty
            else 0.0
        )
        st.metric("月次売上計画合計", f"{monthly_total:,.0f} 円")
        st.caption("CSV取り込みとテンプレートで手入力を軽減し、小規模企業でも負荷を抑えられます。")


def render_plan_step_expenses(state: Dict[str, Any], context: Dict[str, Any]) -> None:
    """経費入力ステップを描画する。"""

    state["expense_table"] = prepare_plan_table(
        state.get("expense_table"), EXPENSE_PLAN_COLUMNS, ["月次金額"]
    )

    render_instruction_popover(
        "経費入力のヒント",
        """
- 会計ソフトから出力した支出CSVを読み込むと費目と金額を自動で整形します。
- テンプレートは小規模ECでよく使う固定費と変動費の構成を含んでいます。
- プルダウンから費目を追加して月次金額を入力すれば経費計画が完成します。
""",
    )

    with form_section(
        "経費データの取り込み",
        "支出CSVを読み込むと費目と金額を自動整形します。",
    ):
        uploaded = st.file_uploader(
            "会計ソフトの経費CSVを取り込む",
            type=["csv"],
            key="plan_expense_upload",
            help="freeeや弥生会計などから出力した経費CSVをアップロードすると自動でマッピングします。",
        )
        if uploaded is not None:
            file_bytes = uploaded.getvalue()
            file_hash = hashlib.md5(file_bytes).hexdigest()
            if file_hash and state.get("expense_import_hash") != file_hash:
                imported_df, error = import_plan_csv(
                    file_bytes,
                    EXPENSE_IMPORT_CANDIDATES,
                    ["費目", "月次金額"],
                    ["月次金額"],
                )
                if error:
                    state["expense_import_feedback"] = ("error", error)
                else:
                    state["expense_table"] = prepare_plan_table(
                        imported_df, EXPENSE_PLAN_COLUMNS, ["月次金額"]
                    )
                    state["expense_import_feedback"] = (
                        "success",
                        f"CSVから{len(state['expense_table'])}件の経費科目を読み込みました。",
                    )
                state["expense_import_hash"] = file_hash

        feedback = state.get("expense_import_feedback")
        if feedback:
            level, message = feedback
            if level == "error":
                st.error(message)
            elif level == "success":
                st.success(message)

    with form_section(
        "テンプレートと費目の追加",
        "固定費・変動費のひな形を呼び出し、抜け漏れを防ぎます。",
        tone="secondary",
    ):
        template_cols = st.columns([3, 1])
        template_options = ["テンプレートを選択"] + list(EXPENSE_PLAN_TEMPLATES.keys())
        selected_template = template_cols[0].selectbox(
            "経費テンプレートを適用",
            options=template_options,
            key="plan_expense_template",
            help="固定費・変動費の代表的な構成をテンプレートから読み込めます。",
        )
        if template_cols[1].button("読み込む", key="plan_apply_expense_template"):
            if selected_template != "テンプレートを選択":
                template_df = pd.DataFrame(EXPENSE_PLAN_TEMPLATES[selected_template])
                state["expense_table"] = prepare_plan_table(
                    template_df, EXPENSE_PLAN_COLUMNS, ["月次金額"]
                )
                state["expense_import_feedback"] = (
                    "success",
                    f"テンプレート『{selected_template}』を適用しました。",
                )

        selected_common = st.multiselect(
            "よく使う経費科目を追加",
            options=COMMON_EXPENSE_ITEMS,
            key="plan_expense_common_select",
            help="複数選択で0円の行を追加し、金額だけ入力できるようにします。",
        )
        if st.button("選択した費目を追加", key="plan_add_expense_common"):
            state["expense_table"], added = append_plan_rows(
                state["expense_table"],
                "費目",
                "月次金額",
                {"区分": "固定費"},
                selected_common,
            )
            if added:
                st.success(f"{added}件の経費科目を追加しました。")
            else:
                st.info("新しく追加できる科目がありませんでした。")
            st.session_state["plan_expense_common_select"] = []

    with form_section(
        "経費計画の編集",
        "費目ごとの月次金額と区分を整えます。",
    ):
        column_module = getattr(st, "column_config", None)
        column_config = {}
        if column_module:
            column_config["費目"] = column_module.TextColumn(
                "費目",
                help="経費の科目名を入力します。",
            )
            column_config["月次金額"] = column_module.NumberColumn(
                "月次金額 (円)",
                min_value=0.0,
                step=20_000.0,
                help="各費目の月次金額を入力します。",
            )
            if hasattr(column_module, "SelectboxColumn"):
                column_config["区分"] = column_module.SelectboxColumn(
                    "区分",
                    options=PLAN_EXPENSE_CLASSIFICATIONS,
                    help="固定費/変動費/投資などの区分を選択します。",
                )
            else:
                column_config["区分"] = column_module.TextColumn(
                    "区分",
                    help="固定費や変動費などの区分を入力します。",
                )
        else:
            column_config = None

        editor_kwargs: Dict[str, Any] = {
            "num_rows": "dynamic",
            "use_container_width": True,
            "hide_index": True,
        }
        if column_config:
            editor_kwargs["column_config"] = column_config

        expense_editor_value = st.data_editor(
            state["expense_table"],
            key="plan_expense_editor",
            **editor_kwargs,
        )
        state["expense_table"] = prepare_plan_table(
            expense_editor_value, EXPENSE_PLAN_COLUMNS, ["月次金額"]
        )

        monthly_total = (
            float(state["expense_table"]["月次金額"].sum())
            if not state["expense_table"].empty
            else 0.0
        )
        st.metric("月次経費計画合計", f"{monthly_total:,.0f} 円")
        st.caption("テンプレートと自動補完で経費入力も数クリックで完了します。")


def render_plan_step_metrics(state: Dict[str, Any], context: Dict[str, Any]) -> None:
    """財務指標計算ステップを描画する。"""

    metrics = calculate_plan_metrics_from_state(state)
    actual_reference = context.get("actual_reference", {})

    monthly_sales_delta = None
    if actual_reference.get("monthly_sales_avg") is not None:
        diff = metrics["monthly_sales"] - actual_reference["monthly_sales_avg"]
        monthly_sales_delta = f"{diff:,.0f} 円 vs 過去平均"

    monthly_profit_delta = None
    if actual_reference.get("monthly_profit_avg") is not None:
        diff_profit = metrics["monthly_profit"] - actual_reference["monthly_profit_avg"]
        monthly_profit_delta = f"{diff_profit:,.0f} 円 vs 過去平均"

    margin_value = metrics.get("monthly_margin")
    margin_display = (
        f"{margin_value * 100:.1f} %"
        if margin_value is not None and np.isfinite(margin_value)
        else "計算不可"
    )
    margin_delta = None
    if metrics.get("target_margin_pct") is not None and np.isfinite(metrics.get("margin_gap_pct")):
        margin_delta = f"{metrics['margin_gap_pct']:.1f} pt vs 目標"

    with form_section(
        "主要指標とアラート",
        "過去平均と比較して計画値の妥当性を確認します。",
    ):
        col1, col2, col3 = st.columns(3)
        col1.metric(
            "月次売上計画",
            f"{metrics['monthly_sales']:,.0f} 円",
            delta=monthly_sales_delta,
        )
        col2.metric(
            "月次営業利益",
            f"{metrics['monthly_profit']:,.0f} 円",
            delta=monthly_profit_delta,
        )
        col3.metric("営業利益率", margin_display, delta=margin_delta)

        if metrics.get("monthly_profit", 0.0) < 0:
            st.error("月次営業利益がマイナスです。コスト配分や売上計画を見直してください。")
        elif metrics.get("monthly_profit", 0.0) == 0:
            st.warning("月次営業利益が0円です。余裕を持たせるために売上・経費を再検討しましょう。")

    summary_df = build_plan_summary_df(metrics)
    with form_section(
        "計画サマリー表",
        "月次・年間の計画額を一覧で確認し、そのままCSVに出力できます。",
    ):
        formatters: Dict[str, str] = {}
        if "月次計画額" in summary_df.columns:
            formatters["月次計画額"] = "{:,.0f}"
        if "年間計画額" in summary_df.columns:
            formatters["年間計画額"] = "{:,.0f}"
        if "指標値" in summary_df.columns:
            formatters["指標値"] = "{:,.1f}"
        st.dataframe(summary_df.style.format(formatters), use_container_width=True)

        if actual_reference.get("margin_avg") is not None:
            st.caption(
                f"参考: 過去平均の営業利益率は{actual_reference['margin_avg'] * 100:.1f}%です。"
            )


def render_plan_step_review(state: Dict[str, Any], context: Dict[str, Any]) -> None:
    """ウィザード最終ステップの結果確認を描画する。"""

    metrics = state.get("metrics") or calculate_plan_metrics_from_state(state)
    info = state.get("basic_info", {})

    st.success("入力内容を確認し、必要に応じて修正してください。")

    with form_section(
        "基本情報サマリー",
        "共有前に必須項目を再確認します。",
    ):
        st.markdown(
            "<span class='form-section__status'>入力完了</span>",
            unsafe_allow_html=True,
        )
        st.markdown(
            f"**事業所名**: {info.get('company_name') or '-'} / **担当者**: {info.get('preparer') or '-'} / "
            f"**計画開始月**: {info.get('fiscal_year_start')} / **期間**: {info.get('plan_period_months')}ヶ月"
        )

    with form_section(
        "売上予測一覧",
        "CSVエクスポート前に最新の売上予測を確認します。",
    ):
        if state["sales_table"].empty:
            st.info("売上予測が未入力です。前のステップで追加してください。")
        else:
            st.dataframe(
                state["sales_table"].style.format({"月次売上": "{:,.0f}"}),
                use_container_width=True,
            )

    with form_section(
        "経費計画一覧",
        "費目別の月次コストを確認し、共有前の抜け漏れを防ぎます。",
    ):
        if state["expense_table"].empty:
            st.info("経費計画が未入力です。前のステップで追加してください。")
        else:
            st.dataframe(
                state["expense_table"].style.format({"月次金額": "{:,.0f}"}),
                use_container_width=True,
            )

    with form_section(
        "財務指標サマリー",
        "年間換算を含む主要指標を一覧で確認できます。",
    ):
        summary_df = build_plan_summary_df(metrics)
        formatters: Dict[str, str] = {}
        if "月次計画額" in summary_df.columns:
            formatters["月次計画額"] = "{:,.0f}"
        if "年間計画額" in summary_df.columns:
            formatters["年間計画額"] = "{:,.0f}"
        if "指標値" in summary_df.columns:
            formatters["指標値"] = "{:,.1f}"
        st.dataframe(summary_df.style.format(formatters), use_container_width=True)

        download_button_from_df(
            "計画サマリーをCSVでダウンロード",
            summary_df,
            "business_plan_summary.csv",
        )

        actual_reference = context.get("actual_reference", {})
        actual_caption: List[str] = []
        if actual_reference.get("monthly_sales_avg") is not None:
            actual_caption.append(f"平均売上 {actual_reference['monthly_sales_avg']:,.0f}円/月")
        if actual_reference.get("monthly_profit_avg") is not None:
            actual_caption.append(f"平均営業利益 {actual_reference['monthly_profit_avg']:,.0f}円/月")
        if actual_reference.get("margin_avg") is not None:
            actual_caption.append(f"平均利益率 {actual_reference['margin_avg'] * 100:.1f}%")
        if actual_caption:
            st.caption("過去実績: " + " / ".join(actual_caption))

        st.caption("入力内容はブラウザセッションに一時保存されます。CSVをダウンロードして関係者と共有してください。")


def render_business_plan_wizard(actual_sales: Optional[pd.DataFrame]) -> None:
    """経営計画ウィザードの全体を描画する。"""

    state = ensure_plan_wizard_state()
    if state.get("current_step", 0) < len(PLAN_WIZARD_STEPS) - 1:
        state["completed"] = False

    channel_options = list(PLAN_CHANNEL_OPTIONS_BASE)
    category_options: List[str] = []
    if actual_sales is not None and not actual_sales.empty:
        if "channel" in actual_sales.columns:
            for channel in actual_sales["channel"].dropna().unique():
                channel_str = str(channel).strip()
                if channel_str and channel_str not in channel_options:
                    channel_options.append(channel_str)
        if "category" in actual_sales.columns:
            category_options = [
                str(cat).strip()
                for cat in actual_sales["category"].dropna().unique()
                if str(cat).strip()
            ]

    channel_options = list(dict.fromkeys(channel_options))
    context = {
        "channel_options": channel_options,
        "category_options": category_options,
        "actual_reference": compute_actual_reference(actual_sales),
    }

    header_cols = st.columns([3, 1])
    with header_cols[0]:
        st.markdown("### 経営計画ウィザード")
    with header_cols[1]:
        if st.button("リセット", key="plan_reset_button"):
            reset_plan_wizard_state()
            st.experimental_rerun()

    step_index = int(state.get("current_step", 0))
    total_steps = len(PLAN_WIZARD_STEPS)
    progress_fraction = (step_index + 1) / total_steps
    progress_label = (
        f"ステップ {step_index + 1} / {total_steps}: {PLAN_WIZARD_STEPS[step_index]['title']}"
    )
    try:
        st.progress(progress_fraction, text=progress_label)
    except TypeError:
        st.progress(progress_fraction)
        st.caption(progress_label)

    render_plan_stepper(step_index)

    st.markdown(f"#### {PLAN_WIZARD_STEPS[step_index]['title']}")
    st.write(PLAN_WIZARD_STEPS[step_index]["description"])

    if step_index == 0:
        render_plan_step_basic_info(state)
        is_valid, errors, warnings = validate_plan_basic_info(state["basic_info"])
    elif step_index == 1:
        render_plan_step_sales(state, context)
        is_valid, errors, warnings = validate_plan_sales(state["sales_table"])
    elif step_index == 2:
        render_plan_step_expenses(state, context)
        is_valid, errors, warnings = validate_plan_expenses(state["expense_table"])
    elif step_index == 3:
        render_plan_step_metrics(state, context)
        is_valid, errors, warnings = validate_plan_metrics(state.get("metrics", {}))
    else:
        render_plan_step_review(state, context)
        is_valid, errors, warnings = True, [], []

    for message in errors:
        st.error(f"❗ {message}")
    for message in warnings:
        st.warning(f"⚠️ {message}")

    nav_cols = st.columns([1, 1, 1])
    if nav_cols[0].button("戻る", disabled=step_index == 0, key=f"plan_prev_{step_index}"):
        state["current_step"] = max(step_index - 1, 0)
        st.experimental_rerun()

    next_label = "完了" if step_index == total_steps - 1 else "次へ進む"
    next_disabled = step_index < total_steps - 1 and not is_valid
    if nav_cols[2].button(next_label, disabled=next_disabled, key=f"plan_next_{step_index}"):
        if step_index < total_steps - 1:
            state["current_step"] = min(step_index + 1, total_steps - 1)
        else:
            state["completed"] = True
        st.experimental_rerun()

    if step_index == total_steps - 1 and state.get("completed"):
        st.success("経営計画ウィザードの入力が完了しました。CSV出力で関係者と共有できます。")


def _nanmean(series: pd.Series) -> float:
    """np.nanmeanの警告を避けつつ平均値を計算する。"""

    if series is None:
        return float("nan")
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        return float("nan")
    return float(clean.mean())


def format_period_label(period: pd.Period, freq: str) -> str:
    """表示用の期間ラベルを生成する。"""

    if freq in {"M", "Q", "Y"}:
        return str(period)
    start = period.start_time
    end = period.end_time
    if freq.startswith("W"):
        return f"{start.strftime('%Y-%m-%d')}週 ({start.strftime('%m/%d')}〜{end.strftime('%m/%d')})"
    return f"{start.strftime('%Y-%m-%d')}〜{end.strftime('%Y-%m-%d')}"


def summarize_sales_by_period(df: pd.DataFrame, freq: str) -> pd.DataFrame:
    """売上と粗利を指定粒度で集計する。"""

    columns = [
        "period",
        "period_start",
        "period_end",
        "period_label",
        "sales_amount",
        "gross_profit",
        "net_gross_profit",
        "prev_period_sales",
        "sales_mom",
        "prev_year_sales",
        "sales_yoy",
        "prev_period_gross",
        "gross_mom",
        "prev_year_gross",
        "gross_yoy",
    ]
    if df.empty:
        return pd.DataFrame(columns=columns)

    working = df.copy()
    working["period"] = working["order_date"].dt.to_period(freq)
    summary = (
        working.groupby("period")[["sales_amount", "gross_profit", "net_gross_profit"]]
        .sum()
        .reset_index()
        .sort_values("period")
    )
    summary["period_start"] = summary["period"].dt.to_timestamp()
    summary["period_end"] = summary["period"].dt.to_timestamp(how="end")
    summary["period_label"] = summary["period"].apply(lambda p: format_period_label(p, freq))

    summary["prev_period_sales"] = summary["sales_amount"].shift(1)
    summary["sales_mom"] = np.where(
        (summary["prev_period_sales"].notna()) & (summary["prev_period_sales"] != 0),
        (summary["sales_amount"] - summary["prev_period_sales"]) / summary["prev_period_sales"],
        np.nan,
    )

    yoy_lag = PERIOD_YOY_LAG.get(freq, 0)
    if yoy_lag:
        summary["prev_year_sales"] = summary["sales_amount"].shift(yoy_lag)
        summary["sales_yoy"] = np.where(
            (summary["prev_year_sales"].notna()) & (summary["prev_year_sales"] != 0),
            (summary["sales_amount"] - summary["prev_year_sales"]) / summary["prev_year_sales"],
            np.nan,
        )
    else:
        summary["prev_year_sales"] = np.nan
        summary["sales_yoy"] = np.nan

    summary["prev_period_gross"] = summary["net_gross_profit"].shift(1)
    summary["gross_mom"] = np.where(
        (summary["prev_period_gross"].notna()) & (summary["prev_period_gross"] != 0),
        (summary["net_gross_profit"] - summary["prev_period_gross"]) / summary["prev_period_gross"],
        np.nan,
    )

    if yoy_lag:
        summary["prev_year_gross"] = summary["net_gross_profit"].shift(yoy_lag)
        summary["gross_yoy"] = np.where(
            (summary["prev_year_gross"].notna()) & (summary["prev_year_gross"] != 0),
            (summary["net_gross_profit"] - summary["prev_year_gross"]) / summary["prev_year_gross"],
            np.nan,
        )
    else:
        summary["prev_year_gross"] = np.nan
        summary["gross_yoy"] = np.nan

    return summary[columns]


def build_kpi_history_df(
    merged_df: pd.DataFrame,
    subscription_df: Optional[pd.DataFrame],
    overrides: Optional[Dict[str, float]],
) -> pd.DataFrame:
    """月次KPI履歴を作成する。"""

    if merged_df.empty:
        return pd.DataFrame()

    months = (
        merged_df["order_month"].dropna().sort_values().unique()
        if "order_month" in merged_df.columns
        else []
    )
    history: List[Dict[str, Any]] = []
    for month in months:
        kpi_row = calculate_kpis(merged_df, subscription_df, month=month, overrides=overrides)
        if kpi_row:
            history.append(kpi_row)

    if not history:
        return pd.DataFrame()

    history_df = pd.DataFrame(history)
    if "month" in history_df.columns:
        history_df["month"] = pd.PeriodIndex(history_df["month"], freq="M")
    return history_df


def aggregate_kpi_history(history_df: pd.DataFrame, freq: str) -> pd.DataFrame:
    """KPI履歴を指定した粒度で集計する。"""

    columns = [
        "period",
        "period_start",
        "period_end",
        "period_label",
        "sales",
        "gross_profit",
        "marketing_cost",
        "active_customers_avg",
        "new_customers",
        "repeat_customers",
        "cancelled_subscriptions",
        "previous_active_customers",
        "ltv",
        "arpu",
        "churn_rate",
        "repeat_rate",
        "gross_margin_rate",
        "inventory_turnover_days",
        "stockout_rate",
        "training_sessions",
        "new_product_count",
        "ltv_prev",
        "ltv_delta",
        "arpu_prev",
        "arpu_delta",
        "churn_prev",
        "churn_delta",
        "gross_margin_prev",
        "gross_margin_delta",
        "repeat_prev",
        "repeat_delta",
        "inventory_turnover_prev",
        "inventory_turnover_delta",
        "stockout_prev",
        "stockout_delta",
        "training_prev",
        "training_delta",
        "new_product_prev",
        "new_product_delta",
    ]
    if history_df.empty:
        return pd.DataFrame(columns=columns)

    working = history_df.dropna(subset=["month"]).copy()
    if working.empty:
        return pd.DataFrame(columns=columns)

    working["timestamp"] = working["month"].dt.to_timestamp()
    working["period"] = working["timestamp"].dt.to_period(freq)
    aggregated = (
        working.groupby("period").agg(
            sales=("sales", "sum"),
            gross_profit=("gross_profit", "sum"),
            marketing_cost=("marketing_cost", "sum"),
            active_customers=("active_customers", _nanmean),
            new_customers=("new_customers", "sum"),
            repeat_customers=("repeat_customers", "sum"),
            cancelled_subscriptions=("cancelled_subscriptions", "sum"),
            previous_active_customers=("previous_active_customers", "sum"),
            ltv=("ltv", _nanmean),
            inventory_turnover_days=("inventory_turnover_days", _nanmean),
            stockout_rate=("stockout_rate", _nanmean),
            training_sessions=("training_sessions", "sum"),
            new_product_count=("new_product_count", "sum"),
        )
    ).reset_index()

    if aggregated.empty:
        return pd.DataFrame(columns=columns)

    aggregated.rename(columns={"active_customers": "active_customers_avg"}, inplace=True)
    aggregated["period_start"] = aggregated["period"].dt.to_timestamp()
    aggregated["period_end"] = aggregated["period"].dt.to_timestamp(how="end")
    aggregated["period_label"] = aggregated["period"].apply(lambda p: format_period_label(p, freq))

    aggregated["arpu"] = aggregated.apply(
        lambda row: row["sales"] / row["active_customers_avg"]
        if row["active_customers_avg"]
        else np.nan,
        axis=1,
    )
    aggregated["churn_rate"] = aggregated.apply(
        lambda row: row["cancelled_subscriptions"] / row["previous_active_customers"]
        if row["previous_active_customers"]
        else np.nan,
        axis=1,
    )
    aggregated["repeat_rate"] = aggregated.apply(
        lambda row: row["repeat_customers"] / row["active_customers_avg"]
        if row["active_customers_avg"]
        else np.nan,
        axis=1,
    )
    aggregated["gross_margin_rate"] = aggregated.apply(
        lambda row: row["gross_profit"] / row["sales"] if row["sales"] else np.nan,
        axis=1,
    )

    aggregated.sort_values("period", inplace=True)
    aggregated["ltv_prev"] = aggregated["ltv"].shift(1)
    aggregated["ltv_delta"] = aggregated["ltv"] - aggregated["ltv_prev"]
    aggregated["arpu_prev"] = aggregated["arpu"].shift(1)
    aggregated["arpu_delta"] = aggregated["arpu"] - aggregated["arpu_prev"]
    aggregated["churn_prev"] = aggregated["churn_rate"].shift(1)
    aggregated["churn_delta"] = aggregated["churn_rate"] - aggregated["churn_prev"]
    aggregated["gross_margin_prev"] = aggregated["gross_margin_rate"].shift(1)
    aggregated["gross_margin_delta"] = aggregated["gross_margin_rate"] - aggregated["gross_margin_prev"]
    aggregated["repeat_prev"] = aggregated["repeat_rate"].shift(1)
    aggregated["repeat_delta"] = aggregated["repeat_rate"] - aggregated["repeat_prev"]
    aggregated["inventory_turnover_prev"] = aggregated["inventory_turnover_days"].shift(1)
    aggregated["inventory_turnover_delta"] = (
        aggregated["inventory_turnover_days"] - aggregated["inventory_turnover_prev"]
    )
    aggregated["stockout_prev"] = aggregated["stockout_rate"].shift(1)
    aggregated["stockout_delta"] = aggregated["stockout_rate"] - aggregated["stockout_prev"]
    aggregated["training_prev"] = aggregated["training_sessions"].shift(1)
    aggregated["training_delta"] = aggregated["training_sessions"] - aggregated["training_prev"]
    aggregated["new_product_prev"] = aggregated["new_product_count"].shift(1)
    aggregated["new_product_delta"] = (
        aggregated["new_product_count"] - aggregated["new_product_prev"]
    )

    return aggregated[columns]


def format_currency(value: Optional[float]) -> str:
    """通貨表記で値を整形する。"""

    if value is None or pd.isna(value):
        return "-"
    return f"{value:,.0f} 円"


def format_percent(value: Optional[float], digits: int = 1) -> str:
    """割合値を%表示に変換する。"""

    if value is None or pd.isna(value):
        return "-"
    return f"{value * 100:.{digits}f}%"


def format_number(value: Optional[float], *, digits: int = 1, unit: str = "") -> str:
    """一般的な数値を文字列化する。"""

    if value is None or pd.isna(value):
        return "-"
    formatted = f"{value:,.{digits}f}" if digits > 0 else f"{value:,.0f}"
    return f"{formatted}{unit}"


def format_delta(
    value: Optional[float], *, digits: int = 1, unit: str = "", percentage: bool = False
) -> Optional[str]:
    """指標変化量の表示を整える。"""

    if value is None or pd.isna(value):
        return None
    if abs(float(value)) < 1e-9:
        return None
    if percentage:
        return f"{value * 100:+.{digits}f} pt"
    formatted = f"{value:+.{digits}f}"
    if unit:
        formatted = f"{formatted}{unit}"
    return formatted


def render_bsc_card(
    *, title: str, icon: str, subtitle: Optional[str], metrics: List[Dict[str, Optional[str]]]
) -> None:
    """バランスト・スコアカードのカードUIを描画する。"""

    st.markdown("<div class='bsc-card'>", unsafe_allow_html=True)
    st.markdown(
        f"<div class='bsc-card__title'>{icon} {html.escape(title)}</div>", unsafe_allow_html=True
    )
    if subtitle:
        st.markdown(
            f"<div class='bsc-card__subtitle'>{html.escape(subtitle)}</div>",
            unsafe_allow_html=True,
        )
    for metric in metrics:
        st.metric(metric["label"], metric["value"], delta=metric.get("delta"))
    st.markdown("</div>", unsafe_allow_html=True)


def _nav_sections_lookup() -> Dict[str, List[str]]:
    """メインメニューごとのセクション一覧を返す。"""

    return {label: sections for label, sections in MAIN_NAV_STRUCTURE}


def render_navigation() -> Tuple[str, str]:
    """トップレベルのナビゲーションを描画し、選択されたセクションを返す。"""

    nav_lookup = _nav_sections_lookup()
    main_labels = list(nav_lookup.keys())

    selected_main = st.radio(
        "主要メニュー",
        options=main_labels,
        horizontal=True,
        key="main_nav",
        label_visibility="collapsed",
    )

    sections = nav_lookup[selected_main]
    sub_key = f"sub_nav_{selected_main}"
    if len(sections) == 1:
        st.session_state[sub_key] = sections[0]
        return selected_main, sections[0]

    if sub_key not in st.session_state or st.session_state[sub_key] not in sections:
        st.session_state[sub_key] = sections[0]

    selected_section = st.radio(
        "セクション選択",
        options=sections,
        horizontal=True,
        key=sub_key,
        label_visibility="collapsed",
    )
    return selected_main, selected_section


def render_breadcrumb(main_label: str, section_label: Optional[str]) -> None:
    """現在地がわかるパンくずリストを表示する。"""

    parts = [main_label]
    if section_label and section_label != main_label:
        parts.append(section_label)
    breadcrumb = " / ".join(parts)
    st.markdown(
        f"<div class='breadcrumb-trail'>{html.escape(breadcrumb)}</div>",
        unsafe_allow_html=True,
    )


def render_hero_section(
    latest_label: str, period_label: str, record_count: int, alert_count: int
) -> None:
    """ヒーローエリアをマッキンゼー風に表示する。"""

    if alert_count > 0:
        status_text = f"⚠️ 要確認: {alert_count}件"
        status_class = "hero-badge hero-badge--alert"
    else:
        status_text = "✅ 主要指標は安定しています"
        status_class = "hero-badge hero-badge--accent"

    st.markdown(
        """
        <div class="hero-panel">
            <div class="hero-title">くらしいきいき社 計数管理ダッシュボード</div>
            <p class="hero-subtitle">高粗利商材のパフォーマンスを即座に把握し、迅速な意思決定を支援します。</p>
            <div class="hero-meta">
                <span class="hero-badge">最新データ: {latest}</span>
                <span class="hero-badge">表示期間: {period}</span>
                <span class="hero-badge">対象レコード: {records}</span>
                <span class="{status_class}">{status}</span>
            </div>
            <div class="hero-persona">
                <span class="hero-chip">👤 社長: 売上・粗利を5秒確認</span>
                <span class="hero-chip">🏪 店長: リピーターと在庫</span>
                <span class="hero-chip">📊 経理: 資金繰りと育成</span>
            </div>
        </div>
        """.format(
            latest=html.escape(latest_label or "-"),
            period=html.escape(period_label or "-"),
            records=f"{record_count:,} 件",
            status_class=status_class,
            status=html.escape(status_text),
        ),
        unsafe_allow_html=True,
    )


def render_status_banner(alerts: Optional[List[str]]) -> None:
    """アラート状況をアクセントカラーで表示する。"""

    if alerts:
        items = "".join(f"<li>{html.escape(msg)}</li>" for msg in alerts)
        st.markdown(
            f"""
            <div class="alert-banner alert-banner--warning">
                <div class="alert-banner__title">⚠️ 警告が検知されました</div>
                <ul>{items}</ul>
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            """
            <div class="alert-banner alert-banner--ok">
                <div class="alert-banner__title">✅ 主要指標は設定した閾値内に収まっています。</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def render_search_bar() -> str:
    """ヒーロー直下のクイック検索をカードスタイルで表示する。"""

    with st.container():
        st.markdown(
            "<div class='surface-card search-card'>", unsafe_allow_html=True
        )
        st.markdown(
            "<div class='search-title'>クイック検索</div>",
            unsafe_allow_html=True,
        )
        query = st.text_input(
            "クイック検索",
            placeholder="商品名、チャネル、チュートリアルを検索",
            key="global_search",
            label_visibility="collapsed",
        )
        st.markdown("</div>", unsafe_allow_html=True)
    return query


def render_global_search_results(query: str, merged_df: pd.DataFrame) -> None:
    """検索クエリに一致するデータやチュートリアルをまとめて表示する。"""

    query = (query or "").strip()
    if not query:
        return

    query_lower = query.lower()
    with st.container():
        st.markdown("<div class='surface-card search-results-card'>", unsafe_allow_html=True)
        st.markdown("### クイック検索結果")

        if merged_df is not None and not merged_df.empty:
            searchable = merged_df.copy()
            for column in ["product_name", "channel", "category"]:
                if column in searchable.columns:
                    searchable[column] = searchable[column].astype(str)
            fallback = pd.Series([False] * len(searchable), index=searchable.index)
            product_series = (
                searchable["product_name"].str.contains(query, case=False, na=False)
                if "product_name" in searchable.columns
                else fallback
            )
            channel_series = (
                searchable["channel"].str.contains(query, case=False, na=False)
                if "channel" in searchable.columns
                else fallback
            )
            category_series = (
                searchable["category"].str.contains(query, case=False, na=False)
                if "category" in searchable.columns
                else fallback
            )
            mask = product_series | channel_series | category_series
            matched_sales = searchable[mask].copy()
            if not matched_sales.empty and "order_date" in matched_sales.columns:
                matched_sales.sort_values("order_date", ascending=False, inplace=True)
            if not matched_sales.empty:
                display_cols = []
                if "order_date" in matched_sales.columns:
                    matched_sales["order_date"] = pd.to_datetime(matched_sales["order_date"])
                    matched_sales["order_date_str"] = matched_sales["order_date"].dt.strftime("%Y-%m-%d")
                    display_cols.append("order_date_str")
                if "channel" in matched_sales.columns:
                    display_cols.append("channel")
                if "product_name" in matched_sales.columns:
                    display_cols.append("product_name")
                if "sales_amount" in matched_sales.columns:
                    display_cols.append("sales_amount")
                summary_table = matched_sales.head(10)[display_cols].rename(
                    columns={
                        "order_date_str": "受注日",
                        "channel": "チャネル",
                        "product_name": "商品名",
                        "sales_amount": "売上高",
                    }
                )
                if "売上高" in summary_table.columns:
                    summary_table["売上高"] = summary_table["売上高"].map(lambda v: f"{v:,.0f}")
                st.dataframe(summary_table, hide_index=True, use_container_width=True)
            else:
                st.caption("売上データに一致する項目は見つかりませんでした。")
        else:
            st.caption("売上データが読み込まれていないため検索できません。")

        matches = [
            tutorial
            for tutorial in TUTORIAL_INDEX
            if query_lower in tutorial["title"].lower()
            or any(query_lower in keyword.lower() for keyword in tutorial.get("keywords", []))
        ]
        if matches:
            st.markdown("**関連チュートリアル**")
            for tutorial in matches:
                st.markdown(f"- [{tutorial['title']}]({tutorial['path']})")
        st.markdown("</div>", unsafe_allow_html=True)


def _format_currency_compact(value: Optional[float]) -> str:
    """通貨をスペースなしの円表示に整形する。"""

    if value is None or pd.isna(value):
        return "-"
    return f"{float(value):,.0f}円"


def format_percentage_delta(value: Optional[float], *, digits: int = 1) -> Optional[str]:
    """百分率の変化量を%表記で返す。"""

    if value is None or pd.isna(value):
        return None
    return f"{float(value) * 100:+.{digits}f}%"


def format_target_gap(
    value: Optional[float],
    target: Optional[float],
    *,
    percentage: bool = False,
    digits: int = 1,
) -> Tuple[str, Optional[float]]:
    """値と目標値の差分をテキストと数値で返す。"""

    if value is None or pd.isna(value) or target is None or pd.isna(target):
        return "-", None
    gap = float(value) - float(target)
    if percentage:
        text = f"{gap * 100:+.{digits}f} pt"
    else:
        text = f"{gap:+,.0f} 円"
    return text, gap


def delta_class_from_value(value: Optional[float]) -> str:
    """KGIカード用のデルタクラスを決定する。"""

    if value is None or pd.isna(value):
        return ""
    numeric = float(value)
    if numeric > 0:
        return "kgi-card__delta--up"
    if numeric < 0:
        return "kgi-card__delta--down"
    return ""


def kpi_delta_class(value: Optional[float]) -> str:
    """KPIストリップ用のデルタクラスを返す。"""

    if value is None or pd.isna(value):
        return ""
    return "kpi-strip__delta--up" if float(value) >= 0 else "kpi-strip__delta--down"


def build_delta_label(prefix: str, formatted: Optional[str], raw_value: Optional[float]) -> str:
    """矢印付きのデルタ表示を生成する。"""

    if not formatted:
        return f"{prefix} -"
    arrow = "―"
    if raw_value is not None and not pd.isna(raw_value):
        numeric = float(raw_value)
        if numeric > 0:
            arrow = "▲"
        elif numeric < 0:
            arrow = "▼"
    return f"{prefix} {arrow} {formatted}"


def render_kgi_cards(
    selected_kpi_row: pd.Series,
    period_row: Optional[pd.DataFrame],
    cash_forecast: pd.DataFrame,
    starting_cash: float,
) -> None:
    """KGI3指標のカードを描画する。"""

    if selected_kpi_row is None or selected_kpi_row.empty:
        return

    sales_value = selected_kpi_row.get("sales")
    sales_delta_val: Optional[float] = None
    if period_row is not None and not period_row.empty:
        raw = period_row["sales_mom"].iloc[0]
        if pd.notna(raw):
            sales_delta_val = float(raw)
    sales_delta_text = format_percentage_delta(sales_delta_val)
    sales_gap_text, sales_gap_val = format_target_gap(sales_value, KGI_TARGETS.get("sales"))

    gross_margin_rate = selected_kpi_row.get("gross_margin_rate")
    gross_delta_val = selected_kpi_row.get("gross_margin_delta")
    if pd.isna(gross_delta_val):
        gross_delta_val = None
    gross_delta_text = format_percentage_delta(gross_delta_val)
    gross_gap_text, gross_gap_val = format_target_gap(
        gross_margin_rate,
        KGI_TARGETS.get("gross_margin_rate"),
        percentage=True,
    )

    cash_balance = starting_cash
    cash_delta_val: Optional[float] = None
    if cash_forecast is not None and not cash_forecast.empty:
        first_row = cash_forecast.iloc[0]
        cash_balance = float(first_row.get("cash_balance", starting_cash))
        net_cf_val = first_row.get("net_cf")
        if net_cf_val is not None and not pd.isna(net_cf_val):
            cash_delta_val = float(net_cf_val)
    cash_delta_text = (
        f"{cash_delta_val:+,.0f} 円" if cash_delta_val is not None else None
    )
    cash_gap_text, cash_gap_val = format_target_gap(
        cash_balance,
        KGI_TARGETS.get("cash_balance"),
        digits=0,
    )

    cards_data = [
        {
            "title": "月次売上高",
            "value": _format_currency_compact(sales_value),
            "delta_label": build_delta_label("前期比", sales_delta_text, sales_delta_val),
            "delta_class": delta_class_from_value(sales_delta_val),
            "target_text": sales_gap_text,
            "target_class": "kgi-card__target--behind"
            if sales_gap_val is not None and sales_gap_val < 0
            else "",
        },
        {
            "title": "粗利率",
            "value": format_percent(gross_margin_rate),
            "delta_label": build_delta_label("前期比", gross_delta_text, gross_delta_val),
            "delta_class": delta_class_from_value(gross_delta_val),
            "target_text": gross_gap_text,
            "target_class": "kgi-card__target--behind"
            if gross_gap_val is not None and gross_gap_val < 0
            else "",
        },
        {
            "title": "資金残高",
            "value": _format_currency_compact(cash_balance),
            "delta_label": build_delta_label("前期比", cash_delta_text, cash_delta_val),
            "delta_class": delta_class_from_value(cash_delta_val),
            "target_text": cash_gap_text,
            "target_class": "kgi-card__target--behind"
            if cash_gap_val is not None and cash_gap_val < 0
            else "",
        },
    ]

    cards_html = []
    for card in cards_data:
        cards_html.append(
            """
            <div class="kgi-card">
                <div class="kgi-card__title">{title}</div>
                <div class="kgi-card__value">{value}</div>
                <div class="kgi-card__delta {delta_class}">{delta_label}</div>
                <div class="kgi-card__target {target_class}">目標差 {target_text}</div>
            </div>
            """.format(
                title=html.escape(card["title"]),
                value=html.escape(card["value"] if card["value"] else "-"),
                delta_class=card["delta_class"],
                delta_label=html.escape(card["delta_label"]),
                target_class=card["target_class"],
                target_text=html.escape(card["target_text"]),
            )
        )

    st.markdown(
        "<div class='kgi-grid'>{}</div>".format("".join(cards_html)),
        unsafe_allow_html=True,
    )


def render_dashboard_meta(
    latest_label: str, period_label: str, record_count: int, alert_count: int
) -> None:
    """データのメタ情報をチップ状に表示する。"""

    chips = [
        ("📅 最新データ", latest_label or "-"),
        ("🗓 表示期間", period_label or "-"),
        ("💾 対象レコード", f"{record_count:,} 件"),
    ]
    if alert_count:
        chips.append(("⚠️ アラート", f"{alert_count} 件"))

    chips_html = "".join(
        "<span class='dashboard-meta__chip'>{label}: {value}</span>".format(
            label=html.escape(label), value=html.escape(value)
        )
        for label, value in chips
    )
    st.markdown(f"<div class='dashboard-meta'>{chips_html}</div>", unsafe_allow_html=True)


def render_first_level_kpi_strip(
    kpi_period_summary: pd.DataFrame, selected_kpi_row: pd.Series
) -> None:
    """第1階層KPIを4枚のカードで表示する。"""

    if selected_kpi_row is None or selected_kpi_row.empty:
        return

    prev_row: Optional[pd.Series] = None
    if (
        kpi_period_summary is not None
        and not kpi_period_summary.empty
        and "period" in kpi_period_summary.columns
    ):
        current_period = selected_kpi_row.get("period")
        if current_period is not None:
            candidates = kpi_period_summary[kpi_period_summary["period"] < current_period]
            if not candidates.empty:
                prev_row = candidates.iloc[-1]

    active_value = selected_kpi_row.get("active_customers_avg")
    prev_active = prev_row.get("active_customers_avg") if prev_row is not None else np.nan
    active_delta: Optional[float] = None
    if pd.notna(active_value) and pd.notna(prev_active):
        active_delta = float(active_value) - float(prev_active)

    ltv_value = selected_kpi_row.get("ltv")
    ltv_delta = selected_kpi_row.get("ltv_delta")
    if pd.isna(ltv_delta):
        ltv_delta = None

    arpu_value = selected_kpi_row.get("arpu")
    arpu_delta = selected_kpi_row.get("arpu_delta")
    if pd.isna(arpu_delta):
        arpu_delta = None

    churn_value = selected_kpi_row.get("churn_rate")
    churn_delta = selected_kpi_row.get("churn_delta")
    if pd.isna(churn_delta):
        churn_delta = None

    metrics = [
        {
            "label": "月次顧客数",
            "value": format_number(active_value, digits=0, unit=" 人"),
            "delta_value": active_delta,
            "delta_text": format_delta(active_delta, digits=0, unit=" 人")
            if active_delta is not None
            else None,
        },
        {
            "label": "LTV",
            "value": _format_currency_compact(ltv_value),
            "delta_value": ltv_delta,
            "delta_text": format_delta(ltv_delta, digits=0, unit=" 円")
            if ltv_delta is not None
            else None,
        },
        {
            "label": "ARPU",
            "value": _format_currency_compact(arpu_value),
            "delta_value": arpu_delta,
            "delta_text": format_delta(arpu_delta, digits=0, unit=" 円")
            if arpu_delta is not None
            else None,
        },
        {
            "label": "解約率",
            "value": format_percent(churn_value),
            "delta_value": churn_delta,
            "delta_text": format_delta(churn_delta, percentage=True)
            if churn_delta is not None
            else None,
        },
    ]

    cards_html = []
    for metric in metrics:
        delta_label = build_delta_label("前月比", metric["delta_text"], metric["delta_value"])
        cards_html.append(
            """
            <div class="kpi-strip__card">
                <div class="kpi-strip__label">{label}</div>
                <div class="kpi-strip__value">{value}</div>
                <div class="kpi-strip__delta {delta_class}">{delta}</div>
            </div>
            """.format(
                label=html.escape(metric["label"]),
                value=html.escape(metric["value"] if metric["value"] else "-"),
                delta_class=kpi_delta_class(metric["delta_value"]),
                delta=html.escape(delta_label),
            )
        )

    st.markdown(
        "<div class='kpi-strip'>{}</div>".format("".join(cards_html)),
        unsafe_allow_html=True,
    )


def render_sales_tab(
    merged_df: pd.DataFrame,
    period_summary: pd.DataFrame,
    channel_share_df: pd.DataFrame,
    category_share_df: pd.DataFrame,
    selected_granularity_label: str,
) -> None:
    """売上タブの可視化と明細を描画する。"""

    if period_summary is not None and not period_summary.empty:
        st.markdown("<div class='chart-section'>", unsafe_allow_html=True)
        st.markdown(
            "<div class='chart-section__header'><div class='chart-section__title'>売上推移</div></div>",
            unsafe_allow_html=True,
        )
        latest_periods = period_summary.tail(12).copy()
        latest_periods["period_start"] = pd.to_datetime(latest_periods["period_start"])
        sales_chart_source = latest_periods.rename(
            columns={
                "period_start": "期間開始",
                "period_label": "期間",
                "sales_amount": "現状売上",
                "prev_year_sales": "前年同期間売上",
            }
        )
        value_columns = [
            col for col in ["現状売上", "前年同期間売上"] if col in sales_chart_source.columns
        ]
        if value_columns:
            melted = (
                sales_chart_source.melt(
                    id_vars=["期間開始", "期間"],
                    value_vars=value_columns,
                    var_name="指標",
                    value_name="金額",
                )
                .dropna(subset=["金額"])
                .sort_values("期間開始")
            )
            color_domain: List[str] = []
            color_range: List[str] = []
            for column in value_columns:
                color_domain.append(column)
                if column == "現状売上":
                    color_range.append(SALES_SERIES_COLOR)
                elif column == "前年同期間売上":
                    color_range.append(YOY_SERIES_COLOR)
                else:
                    color_range.append(SALES_SERIES_COLOR)

            sales_line = alt.Chart(melted).mark_line(
                point=alt.OverlayMarkDef(size=70, filled=True)
            ).encode(
                x=alt.X(
                    "期間開始:T",
                    title=f"{selected_granularity_label}開始日",
                    axis=alt.Axis(format="%Y-%m", labelOverlap=True),
                ),
                y=alt.Y(
                    "金額:Q",
                    title="売上高 (円)",
                    axis=alt.Axis(format=",.0f"),
                ),
                color=alt.Color(
                    "指標:N",
                    scale=alt.Scale(domain=color_domain, range=color_range),
                    legend=alt.Legend(title="系列"),
                ),
                tooltip=[
                    alt.Tooltip("期間:T", title="期間"),
                    alt.Tooltip("指標:N", title="系列"),
                    alt.Tooltip("金額:Q", title="金額", format=",.0f"),
                ],
            )

            chart_layers: List[alt.Chart] = [sales_line]
            sales_target = KGI_TARGETS.get("sales")
            if sales_target is not None and not pd.isna(sales_target):
                target_df = pd.DataFrame({"基準": ["売上目標"], "金額": [float(sales_target)]})
                target_rule = alt.Chart(target_df).mark_rule(strokeDash=[6, 4]).encode(
                    y="金額:Q",
                    color=alt.Color(
                        "基準:N",
                        scale=alt.Scale(domain=["売上目標"], range=[BASELINE_SERIES_COLOR]),
                        legend=alt.Legend(title="基準"),
                    ),
                    tooltip=[alt.Tooltip("金額:Q", title="売上目標", format=",.0f")],
                )
                chart_layers.append(target_rule)

            sales_chart = alt.layer(*chart_layers).resolve_scale(color="independent").properties(
                height=320,
            )
            sales_chart = apply_altair_theme(sales_chart)
            st.altair_chart(sales_chart, use_container_width=True)
        else:
            st.caption("売上推移を表示するための指標が不足しています。")

        latest_row = latest_periods.iloc[-1]
        peak_idx = latest_periods["sales_amount"].idxmax()
        peak_row = latest_periods.loc[peak_idx]
        latest_sales = float(latest_row.get("sales_amount", 0.0))
        yoy_value = latest_row.get("sales_yoy")
        yoy_text = f"{float(yoy_value) * 100:+.1f}%" if pd.notna(yoy_value) else "前年比データなし"
        sales_target = KGI_TARGETS.get("sales")
        target_gap_text, _ = format_target_gap(latest_sales, sales_target)
        summary_parts = [
            f"売上は{latest_row['period_label']}に{latest_sales:,.0f}円で、前年同期間比 {yoy_text}。",
            f"ピークは{peak_row['period_label']}の{float(peak_row['sales_amount']):,.0f}円です。",
        ]
        if target_gap_text != "-":
            summary_parts.append(f"目標値との差は{target_gap_text}です。")
        st.caption(" ".join(summary_parts))
        st.markdown("</div>", unsafe_allow_html=True)
    else:
        st.info("売上推移を表示するデータが不足しています。")

    if (channel_share_df is not None and not channel_share_df.empty) or (
        category_share_df is not None and not category_share_df.empty
    ):
        st.markdown("<div class='chart-section'>", unsafe_allow_html=True)
        st.markdown(
            "<div class='chart-section__header'><div class='chart-section__title'>チャネル・カテゴリ内訳</div></div>",
            unsafe_allow_html=True,
        )
        chart_cols = st.columns(2)
        if channel_share_df is not None and not channel_share_df.empty:
            channel_rank = channel_share_df.sort_values("sales_amount", ascending=False).copy()
            channel_rank["構成比"] = channel_rank["sales_amount"] / channel_rank["sales_amount"].sum()
            channel_rank.rename(
                columns={"channel": "チャネル", "sales_amount": "売上高"}, inplace=True
            )
            bar = alt.Chart(channel_rank.head(10)).mark_bar(
                cornerRadiusTopLeft=3,
                cornerRadiusTopRight=3,
            ).encode(
                y=alt.Y("チャネル:N", sort="-x", title=None),
                x=alt.X("売上高:Q", title="売上高 (円)", axis=alt.Axis(format=",.0f")),
                color=alt.value(SALES_SERIES_COLOR),
                tooltip=[
                    alt.Tooltip("チャネル:N", title="チャネル"),
                    alt.Tooltip("売上高:Q", title="売上高", format=",.0f"),
                    alt.Tooltip("構成比:Q", title="構成比", format=".1%"),
                ],
            )
            labels = alt.Chart(channel_rank.head(10)).mark_text(
                align="left",
                baseline="middle",
                dx=6,
                color="#0F1E2E",
                fontWeight="bold",
            ).encode(
                y=alt.Y("チャネル:N", sort="-x"),
                x=alt.X("売上高:Q"),
                text=alt.Text("構成比:Q", format=".1%"),
            )
            channel_chart = apply_altair_theme((bar + labels).properties(height=260))
            chart_cols[0].altair_chart(channel_chart, use_container_width=True)

            top_channel = channel_rank.iloc[0]
            if len(channel_rank) >= 5:
                fifth_channel = channel_rank.iloc[4]
                diff_value = float(top_channel["売上高"]) - float(fifth_channel["売上高"])
                chart_cols[0].caption(
                    f"売上上位チャネルは{top_channel['チャネル']}で構成比{top_channel['構成比']:.1%}。5位との差は{diff_value:,.0f}円です。"
                )
            else:
                chart_cols[0].caption(
                    f"売上上位チャネルは{top_channel['チャネル']}で構成比{top_channel['構成比']:.1%}です。"
                )
        else:
            chart_cols[0].info("チャネル別の集計データがありません。")

        if category_share_df is not None and not category_share_df.empty:
            category_rank = category_share_df.sort_values("sales_amount", ascending=False).copy()
            category_rank["構成比"] = (
                category_rank["sales_amount"] / category_rank["sales_amount"].sum()
            )
            category_rank.rename(
                columns={"category": "カテゴリ", "sales_amount": "売上高"}, inplace=True
            )
            bar = alt.Chart(category_rank.head(10)).mark_bar(
                cornerRadiusTopLeft=3,
                cornerRadiusTopRight=3,
                color=GROSS_SERIES_COLOR,
            ).encode(
                y=alt.Y("カテゴリ:N", sort="-x", title=None),
                x=alt.X("売上高:Q", title="売上高 (円)", axis=alt.Axis(format=",.0f")),
                tooltip=[
                    alt.Tooltip("カテゴリ:N", title="カテゴリ"),
                    alt.Tooltip("売上高:Q", title="売上高", format=",.0f"),
                    alt.Tooltip("構成比:Q", title="構成比", format=".1%"),
                ],
            )
            labels = alt.Chart(category_rank.head(10)).mark_text(
                align="left",
                baseline="middle",
                dx=6,
                color="#0F1E2E",
                fontWeight="bold",
            ).encode(
                y=alt.Y("カテゴリ:N", sort="-x"),
                x=alt.X("売上高:Q"),
                text=alt.Text("構成比:Q", format=".1%"),
            )
            category_chart = apply_altair_theme((bar + labels).properties(height=260))
            chart_cols[1].altair_chart(category_chart, use_container_width=True)

            top_category = category_rank.iloc[0]
            chart_cols[1].caption(
                f"売上トップカテゴリは{top_category['カテゴリ']}で、構成比は{top_category['構成比']:.1%}です。"
            )
        else:
            chart_cols[1].info("カテゴリ別の集計データがありません。")
        st.markdown("</div>", unsafe_allow_html=True)

    with st.expander("売上明細（商品別・上位50件）", expanded=False):
        if merged_df is None or merged_df.empty:
            st.info("売上データがありません。")
        else:
            detail_df = (
                merged_df.groupby(["product_code", "product_name", "category"])
                .agg(
                    売上高=("sales_amount", "sum"),
                    粗利=("net_gross_profit", "sum"),
                    販売数量=("quantity", "sum"),
                )
                .reset_index()
                .sort_values("売上高", ascending=False)
                .head(50)
            )
            if detail_df.empty:
                st.info("表示できる明細がありません。")
            else:
                detail_df["粗利率"] = np.where(
                    detail_df["売上高"] != 0,
                    detail_df["粗利"] / detail_df["売上高"],
                    np.nan,
                )
                display_df = detail_df.copy()
                display_df["販売数量"] = display_df["販売数量"].map(lambda v: f"{v:,.0f}")
                for column in ["売上高", "粗利"]:
                    display_df[column] = display_df[column].map(lambda v: f"{v:,.0f}")
                display_df["粗利率"] = display_df["粗利率"].map(
                    lambda v: f"{v * 100:.1f}%" if pd.notna(v) else "-"
                )
                st.dataframe(display_df, hide_index=True, use_container_width=True)
                toolbar = st.columns(2)
                with toolbar[0]:
                    download_button_from_df("CSV出力", detail_df, "sales_detail.csv")
                with toolbar[1]:
                    st.button("PDF出力 (準備中)", disabled=True)


def render_gross_tab(
    merged_df: pd.DataFrame,
    period_summary: pd.DataFrame,
    selected_granularity_label: str,
) -> None:
    """粗利タブのグラフと明細を描画する。"""

    if period_summary is not None and not period_summary.empty:
        st.markdown("<div class='chart-section'>", unsafe_allow_html=True)
        st.markdown(
            "<div class='chart-section__header'><div class='chart-section__title'>粗利と粗利率の推移</div></div>",
            unsafe_allow_html=True,
        )
        latest_periods = period_summary.tail(12).copy()
        latest_periods["period_start"] = pd.to_datetime(latest_periods["period_start"])
        latest_periods["gross_margin_pct"] = latest_periods["gross_margin_rate"] * 100

        gross_bar = alt.Chart(latest_periods).mark_bar(color=GROSS_SERIES_COLOR).encode(
            x=alt.X(
                "period_start:T",
                title=f"{selected_granularity_label}開始日",
                axis=alt.Axis(format="%Y-%m", labelOverlap=True),
            ),
            y=alt.Y(
                "net_gross_profit:Q",
                title="粗利 (円)",
                axis=alt.Axis(format=",.0f"),
            ),
            tooltip=[
                alt.Tooltip("period_label:N", title="期間"),
                alt.Tooltip("net_gross_profit:Q", title="粗利", format=",.0f"),
            ],
        )

        gross_line = alt.Chart(latest_periods).mark_line(
            color=YOY_SERIES_COLOR, point=alt.OverlayMarkDef(size=60, filled=True)
        ).encode(
            x=alt.X("period_start:T"),
            y=alt.Y(
                "gross_margin_pct:Q",
                title="粗利率 (%)",
                axis=alt.Axis(format=".1f", orient="right"),
            ),
            tooltip=[
                alt.Tooltip("period_label:N", title="期間"),
                alt.Tooltip("gross_margin_pct:Q", title="粗利率", format=".1f"),
            ],
        )

        gross_layers: List[alt.Chart] = [gross_bar, gross_line]
        gross_target = KGI_TARGETS.get("gross_margin_rate")
        if gross_target is not None and not pd.isna(gross_target):
            gross_target_df = pd.DataFrame(
                {"基準": ["粗利率目標"], "粗利率": [float(gross_target) * 100]}
            )
            gross_target_rule = alt.Chart(gross_target_df).mark_rule(strokeDash=[6, 4]).encode(
                y=alt.Y(
                    "粗利率:Q",
                    title="粗利率 (%)",
                ),
                color=alt.Color(
                    "基準:N",
                    scale=alt.Scale(domain=["粗利率目標"], range=[BASELINE_SERIES_COLOR]),
                    legend=alt.Legend(title="基準"),
                ),
                tooltip=[alt.Tooltip("粗利率:Q", title="粗利率目標", format=".1f")],
            )
            gross_layers.append(gross_target_rule)

        gross_chart = (
            alt.layer(*gross_layers)
            .resolve_scale(y="independent", color="independent")
            .properties(height=320)
        )
        st.altair_chart(apply_altair_theme(gross_chart), use_container_width=True)

        latest_row = latest_periods.iloc[-1]
        latest_gross = float(latest_row.get("net_gross_profit", 0.0))
        gross_yoy = latest_row.get("gross_yoy")
        gross_margin = latest_row.get("gross_margin_rate")
        gross_margin_text = format_percent(gross_margin)
        gross_yoy_text = (
            f"{float(gross_yoy) * 100:+.1f}%" if pd.notna(gross_yoy) else "前年比データなし"
        )
        peak_idx = latest_periods["net_gross_profit"].idxmax()
        peak_row = latest_periods.loc[peak_idx]
        gross_target_gap_text, _ = format_target_gap(
            gross_margin, KGI_TARGETS.get("gross_margin_rate"), percentage=True
        )
        summary_parts = [
            f"最新の粗利は{latest_row['period_label']}で{latest_gross:,.0f}円、粗利率は{gross_margin_text}です。",
            f"前年同期間比は{gross_yoy_text}、粗利のピークは{peak_row['period_label']}の{float(peak_row['net_gross_profit']):,.0f}円です。",
        ]
        if gross_target_gap_text != "-":
            summary_parts.append(f"粗利率目標との差は{gross_target_gap_text}です。")
        st.caption(" ".join(summary_parts))
        st.markdown("</div>", unsafe_allow_html=True)
    else:
        st.info("粗利推移を表示するデータが不足しています。")

    if merged_df is not None and not merged_df.empty:
        st.markdown("<div class='chart-section'>", unsafe_allow_html=True)
        st.markdown(
            "<div class='chart-section__header'><div class='chart-section__title'>粗利構成</div></div>",
            unsafe_allow_html=True,
        )
        chart_cols = st.columns(2)
        category_gross = (
            merged_df.groupby("category")["net_gross_profit"].sum().reset_index().sort_values("net_gross_profit", ascending=False).head(10)
        )
        if not category_gross.empty:
            category_gross.rename(
                columns={"category": "カテゴリ", "net_gross_profit": "粗利"}, inplace=True
            )
            total = category_gross["粗利"].sum()
            if total:
                category_gross["構成比"] = category_gross["粗利"] / total
            else:
                category_gross["構成比"] = 0
            bar = alt.Chart(category_gross).mark_bar(
                cornerRadiusTopLeft=3,
                cornerRadiusTopRight=3,
                color=GROSS_SERIES_COLOR,
            ).encode(
                y=alt.Y("カテゴリ:N", sort="-x", title=None),
                x=alt.X("粗利:Q", title="粗利 (円)", axis=alt.Axis(format=",.0f")),
                tooltip=[
                    alt.Tooltip("カテゴリ:N", title="カテゴリ"),
                    alt.Tooltip("粗利:Q", title="粗利", format=",.0f"),
                    alt.Tooltip("構成比:Q", title="構成比", format=".1%"),
                ],
            )
            labels = alt.Chart(category_gross).mark_text(
                align="left",
                baseline="middle",
                dx=6,
                color="#0F1E2E",
                fontWeight="bold",
            ).encode(
                y=alt.Y("カテゴリ:N", sort="-x"),
                x=alt.X("粗利:Q"),
                text=alt.Text("構成比:Q", format=".1%"),
            )
            chart_cols[0].altair_chart(
                apply_altair_theme((bar + labels).properties(height=260)),
                use_container_width=True,
            )
            top_category = category_gross.iloc[0]
            chart_cols[0].caption(
                f"粗利が最も高いカテゴリは{top_category['カテゴリ']}で、構成比は{top_category['構成比']:.1%}です。"
            )
        else:
            chart_cols[0].info("カテゴリ別の粗利データがありません。")

        product_gross = (
            merged_df.groupby("product_name")["net_gross_profit"].sum().reset_index().sort_values("net_gross_profit", ascending=False).head(10)
        )
        if not product_gross.empty:
            product_gross.rename(
                columns={"product_name": "商品", "net_gross_profit": "粗利"}, inplace=True
            )
            total = product_gross["粗利"].sum()
            if total:
                product_gross["構成比"] = product_gross["粗利"] / total
            else:
                product_gross["構成比"] = 0
            bar = alt.Chart(product_gross).mark_bar(
                cornerRadiusTopLeft=3,
                cornerRadiusTopRight=3,
                color=GROSS_SERIES_COLOR,
            ).encode(
                y=alt.Y("商品:N", sort="-x", title=None),
                x=alt.X("粗利:Q", title="粗利 (円)", axis=alt.Axis(format=",.0f")),
                tooltip=[
                    alt.Tooltip("商品:N", title="商品"),
                    alt.Tooltip("粗利:Q", title="粗利", format=",.0f"),
                    alt.Tooltip("構成比:Q", title="構成比", format=".1%"),
                ],
            )
            labels = alt.Chart(product_gross).mark_text(
                align="left",
                baseline="middle",
                dx=6,
                color="#0F1E2E",
                fontWeight="bold",
            ).encode(
                y=alt.Y("商品:N", sort="-x"),
                x=alt.X("粗利:Q"),
                text=alt.Text("構成比:Q", format=".1%"),
            )
            chart_cols[1].altair_chart(
                apply_altair_theme((bar + labels).properties(height=260)),
                use_container_width=True,
            )
            top_product = product_gross.iloc[0]
            chart_cols[1].caption(
                f"粗利トップ商品は{top_product['商品']}で、構成比は{top_product['構成比']:.1%}です。"
            )
        else:
            chart_cols[1].info("商品別の粗利データがありません。")
        st.markdown("</div>", unsafe_allow_html=True)

    with st.expander("原価率・粗利テーブル", expanded=False):
        if merged_df is None or merged_df.empty:
            st.info("データがありません。")
        else:
            detail_df = (
                merged_df.groupby(["product_code", "product_name", "category"])
                .agg(
                    売上高=("sales_amount", "sum"),
                    粗利=("net_gross_profit", "sum"),
                    推定原価=("estimated_cost", "sum"),
                    原価率=("cost_rate", "mean"),
                )
                .reset_index()
            )
            if detail_df.empty:
                st.info("表示できる明細がありません。")
            else:
                detail_df["粗利率"] = np.where(
                    detail_df["売上高"] != 0,
                    detail_df["粗利"] / detail_df["売上高"],
                    np.nan,
                )
                detail_df.sort_values("粗利", ascending=False, inplace=True)
                display_df = detail_df.copy()
                for column in ["売上高", "粗利", "推定原価"]:
                    display_df[column] = display_df[column].map(lambda v: f"{v:,.0f}")
                display_df["原価率"] = display_df["原価率"].map(
                    lambda v: f"{v * 100:.1f}%" if pd.notna(v) else "-"
                )
                display_df["粗利率"] = display_df["粗利率"].map(
                    lambda v: f"{v * 100:.1f}%" if pd.notna(v) else "-"
                )
                st.dataframe(display_df.head(50), hide_index=True, use_container_width=True)
                toolbar = st.columns(2)
                with toolbar[0]:
                    download_button_from_df("CSV出力", detail_df, "gross_profit_detail.csv")
                with toolbar[1]:
                    st.button("PDF出力 (準備中)", disabled=True)


def render_inventory_tab(
    merged_df: pd.DataFrame,
    kpi_period_summary: pd.DataFrame,
    selected_kpi_row: pd.Series,
) -> None:
    """在庫タブの主要指標と推計表を表示する。"""

    if kpi_period_summary is not None and not kpi_period_summary.empty:
        st.markdown("<div class='chart-section'>", unsafe_allow_html=True)
        st.markdown(
            "<div class='chart-section__header'><div class='chart-section__title'>在庫KPIの推移</div></div>",
            unsafe_allow_html=True,
        )
        history = kpi_period_summary.tail(12).copy()
        history["period_start"] = pd.to_datetime(history["period_start"])
        chart_cols = st.columns(2)
        turnover_line = alt.Chart(history).mark_line(
            color=INVENTORY_SERIES_COLOR, point=alt.OverlayMarkDef(size=60, filled=True)
        ).encode(
            x=alt.X("period_start:T", title="期間開始", axis=alt.Axis(format="%Y-%m", labelOverlap=True)),
            y=alt.Y("inventory_turnover_days:Q", title="在庫回転日数", axis=alt.Axis(format=",.0f")),
            tooltip=[
                alt.Tooltip("period_label:N", title="期間"),
                alt.Tooltip("inventory_turnover_days:Q", title="在庫回転日数", format=",.1f"),
            ],
        )
        chart_cols[0].altair_chart(
            apply_altair_theme(turnover_line.properties(height=260)), use_container_width=True
        )

        stockout_chart = alt.Chart(history).mark_line(
            color=YOY_SERIES_COLOR, point=alt.OverlayMarkDef(size=60, filled=True)
        ).encode(
            x=alt.X("period_start:T", title="期間開始", axis=alt.Axis(format="%Y-%m", labelOverlap=True)),
            y=alt.Y(
                "stockout_rate:Q",
                title="欠品率",
                axis=alt.Axis(format=".1%"),
            ),
            tooltip=[
                alt.Tooltip("period_label:N", title="期間"),
                alt.Tooltip("stockout_rate:Q", title="欠品率", format=".1%"),
            ],
        )
        chart_cols[1].altair_chart(
            apply_altair_theme(stockout_chart.properties(height=260)),
            use_container_width=True,
        )
        latest_inventory_row = history.iloc[-1]
        turnover_value = latest_inventory_row.get("inventory_turnover_days")
        stockout_value = latest_inventory_row.get("stockout_rate")
        chart_cols[0].caption(
            f"最新の在庫回転日数は{turnover_value:,.1f}日で、直近最大値は{history['inventory_turnover_days'].max():,.1f}日です。"
            if pd.notna(turnover_value)
            else "在庫回転日数の最新値が取得できません。"
        )
        chart_cols[1].caption(
            f"最新の欠品率は{stockout_value:.1%}で、最小値は{history['stockout_rate'].min():.1%}です。"
            if pd.notna(stockout_value)
            else "欠品率の最新値が取得できません。"
        )
        st.markdown("</div>", unsafe_allow_html=True)
    else:
        st.info("在庫関連KPIの履歴がありません。")

    if merged_df is not None and not merged_df.empty:
        st.markdown("<div class='chart-section'>", unsafe_allow_html=True)
        st.markdown(
            "<div class='chart-section__header'><div class='chart-section__title'>在庫構成の推計</div></div>",
            unsafe_allow_html=True,
        )
        chart_cols = st.columns(2)
        category_qty = (
            merged_df.groupby("category")["quantity"].sum().reset_index().sort_values("quantity", ascending=False).head(10)
        )
        if not category_qty.empty:
            category_qty.rename(columns={"quantity": "販売数量"}, inplace=True)
            total_qty = category_qty["販売数量"].sum()
            if total_qty:
                category_qty["構成比"] = category_qty["販売数量"] / total_qty
            else:
                category_qty["構成比"] = 0
            bar = alt.Chart(category_qty).mark_bar(
                cornerRadiusTopLeft=3,
                cornerRadiusTopRight=3,
                color=INVENTORY_SERIES_COLOR,
            ).encode(
                y=alt.Y("category:N", sort="-x", title="カテゴリ"),
                x=alt.X("販売数量:Q", title="販売数量", axis=alt.Axis(format=",.0f")),
                tooltip=[
                    alt.Tooltip("category:N", title="カテゴリ"),
                    alt.Tooltip("販売数量:Q", title="販売数量", format=",.0f"),
                    alt.Tooltip("構成比:Q", title="構成比", format=".1%"),
                ],
            )
            labels = alt.Chart(category_qty).mark_text(
                align="left",
                baseline="middle",
                dx=6,
                color="#0F1E2E",
                fontWeight="bold",
            ).encode(
                y=alt.Y("category:N", sort="-x"),
                x=alt.X("販売数量:Q"),
                text=alt.Text("構成比:Q", format=".1%"),
            )
            chart_cols[0].altair_chart(
                apply_altair_theme((bar + labels).properties(height=260)),
                use_container_width=True,
            )
            top_category = category_qty.iloc[0]
            chart_cols[0].caption(
                f"在庫数量が最も多いカテゴリは{top_category['category']}で、構成比は{top_category['構成比']:.1%}です。"
            )
        else:
            chart_cols[0].info("カテゴリ別の販売数量が算出できませんでした。")

        product_qty = (
            merged_df.groupby("product_name")["quantity"].sum().reset_index().sort_values("quantity", ascending=False).head(10)
        )
        if not product_qty.empty:
            product_qty.rename(columns={"quantity": "販売数量"}, inplace=True)
            total_qty = product_qty["販売数量"].sum()
            if total_qty:
                product_qty["構成比"] = product_qty["販売数量"] / total_qty
            else:
                product_qty["構成比"] = 0
            bar = alt.Chart(product_qty).mark_bar(
                cornerRadiusTopLeft=3,
                cornerRadiusTopRight=3,
                color=INVENTORY_SERIES_COLOR,
            ).encode(
                y=alt.Y("product_name:N", sort="-x", title="商品"),
                x=alt.X("販売数量:Q", title="販売数量", axis=alt.Axis(format=",.0f")),
                tooltip=[
                    alt.Tooltip("product_name:N", title="商品"),
                    alt.Tooltip("販売数量:Q", title="販売数量", format=",.0f"),
                    alt.Tooltip("構成比:Q", title="構成比", format=".1%"),
                ],
            )
            labels = alt.Chart(product_qty).mark_text(
                align="left",
                baseline="middle",
                dx=6,
                color="#0F1E2E",
                fontWeight="bold",
            ).encode(
                y=alt.Y("product_name:N", sort="-x"),
                x=alt.X("販売数量:Q"),
                text=alt.Text("構成比:Q", format=".1%"),
            )
            chart_cols[1].altair_chart(
                apply_altair_theme((bar + labels).properties(height=260)),
                use_container_width=True,
            )
            top_product = product_qty.iloc[0]
            chart_cols[1].caption(
                f"在庫数量が最も多い商品は{top_product['product_name']}で、構成比は{top_product['構成比']:.1%}です。"
            )
        else:
            chart_cols[1].info("商品別の販売数量が算出できませんでした。")
        st.markdown("</div>", unsafe_allow_html=True)

    with st.expander("在庫推計テーブル", expanded=False):
        if merged_df is None or merged_df.empty:
            st.info("データがありません。")
        else:
            detail_df = (
                merged_df.groupby(["product_code", "product_name", "category"])
                .agg(
                    販売数量=("quantity", "sum"),
                    売上高=("sales_amount", "sum"),
                    推定原価=("estimated_cost", "sum"),
                )
                .reset_index()
            )
            if detail_df.empty:
                st.info("表示できる明細がありません。")
            else:
                turnover_days = selected_kpi_row.get("inventory_turnover_days")
                if turnover_days is not None and not pd.isna(turnover_days) and turnover_days > 0:
                    detail_df["推定在庫金額"] = detail_df["推定原価"] / 30.0 * float(turnover_days)
                else:
                    detail_df["推定在庫金額"] = np.nan
                detail_df.sort_values("推定在庫金額", ascending=False, inplace=True)
                display_df = detail_df.copy()
                display_df["販売数量"] = display_df["販売数量"].map(lambda v: f"{v:,.0f}")
                for column in ["売上高", "推定原価", "推定在庫金額"]:
                    display_df[column] = display_df[column].map(lambda v: f"{v:,.0f}" if pd.notna(v) else "-")
                st.dataframe(display_df.head(50), hide_index=True, use_container_width=True)
                toolbar = st.columns(2)
                with toolbar[0]:
                    download_button_from_df("CSV出力", detail_df, "inventory_overview.csv")
                with toolbar[1]:
                    st.button("PDF出力 (準備中)", disabled=True)


def render_cash_tab(
    cash_plan: pd.DataFrame,
    cash_forecast: pd.DataFrame,
    starting_cash: float,
) -> None:
    """資金タブのグラフと明細を描画する。"""

    if cash_forecast is not None and not cash_forecast.empty:
        st.markdown("<div class='chart-section'>", unsafe_allow_html=True)
        st.markdown(
            "<div class='chart-section__header'><div class='chart-section__title'>キャッシュ残高推移</div></div>",
            unsafe_allow_html=True,
        )
        forecast_df = cash_forecast.copy()
        forecast_df["period_start"] = forecast_df["month"].dt.to_timestamp()
        forecast_df["period_label"] = forecast_df["month"].astype(str)
        cash_line = alt.Chart(forecast_df).mark_line(
            color=CASH_SERIES_COLOR, point=alt.OverlayMarkDef(size=60, filled=True)
        ).encode(
            x=alt.X("period_start:T", title="期間開始", axis=alt.Axis(format="%Y-%m", labelOverlap=True)),
            y=alt.Y("cash_balance:Q", title="期末現金残高 (円)", axis=alt.Axis(format=",.0f")),
            tooltip=[
                alt.Tooltip("period_label:N", title="期間"),
                alt.Tooltip("cash_balance:Q", title="期末現金残高", format=",.0f"),
                alt.Tooltip("net_cf:Q", title="純キャッシュフロー", format=",.0f"),
            ],
        )

        cash_layers: List[alt.Chart] = [cash_line]
        cash_target = KGI_TARGETS.get("cash_balance")
        if cash_target is not None and not pd.isna(cash_target):
            cash_target_df = pd.DataFrame({"基準": ["目標残高"], "金額": [float(cash_target)]})
            target_rule = alt.Chart(cash_target_df).mark_rule(strokeDash=[6, 4]).encode(
                y="金額:Q",
                color=alt.Color(
                    "基準:N",
                    scale=alt.Scale(domain=["目標残高"], range=[BASELINE_SERIES_COLOR]),
                    legend=alt.Legend(title="基準"),
                ),
                tooltip=[alt.Tooltip("金額:Q", title="目標残高", format=",.0f")],
            )
            cash_layers.append(target_rule)

        cash_chart = alt.layer(*cash_layers).resolve_scale(color="independent").properties(
            height=320,
        )
        st.altair_chart(apply_altair_theme(cash_chart), use_container_width=True)

        latest_row = forecast_df.iloc[-1]
        latest_cash = float(latest_row.get("cash_balance", starting_cash))
        net_cf = latest_row.get("net_cf")
        net_cf_text = f"{float(net_cf):,.0f}円" if pd.notna(net_cf) else "-"
        target_gap_text, _ = format_target_gap(latest_cash, cash_target)
        summary_parts = [
            f"最新の期末現金残高は{latest_cash:,.0f}円、純キャッシュフローは{net_cf_text}です。",
        ]
        if target_gap_text != "-":
            summary_parts.append(f"目標残高との差は{target_gap_text}です。")
        st.caption(" ".join(summary_parts))
        st.markdown("</div>", unsafe_allow_html=True)
    else:
        st.info("資金繰り予測を表示するデータが不足しています。")

    if cash_plan is not None and not cash_plan.empty:
        st.markdown("<div class='chart-section'>", unsafe_allow_html=True)
        st.markdown(
            "<div class='chart-section__header'><div class='chart-section__title'>キャッシュフロー内訳</div></div>",
            unsafe_allow_html=True,
        )
        plan_df = cash_plan.copy()
        plan_df["period_start"] = plan_df["month"].dt.to_timestamp()
        melted = plan_df.melt(
            id_vars=["period_start"],
            value_vars=["operating_cf", "investment_cf", "financing_cf", "loan_repayment"],
            var_name="type",
            value_name="金額",
        )
        label_map = {
            "operating_cf": "営業CF",
            "investment_cf": "投資CF",
            "financing_cf": "財務CF",
            "loan_repayment": "返済",
        }
        melted["区分"] = melted["type"].map(label_map)
        melted = melted.dropna(subset=["区分"])
        domain = [label_map[key] for key in label_map]
        range_colors = [CF_COLOR_MAPPING[label] for label in domain]
        cf_chart = alt.Chart(melted).mark_bar().encode(
            x=alt.X("period_start:T", title="期間開始", axis=alt.Axis(format="%Y-%m", labelOverlap=True)),
            y=alt.Y("金額:Q", title="キャッシュフロー (円)", axis=alt.Axis(format=",.0f")),
            color=alt.Color("区分:N", scale=alt.Scale(domain=domain, range=range_colors), legend=alt.Legend(title="区分")),
            tooltip=[
                alt.Tooltip("period_start:T", title="期間"),
                alt.Tooltip("区分:N", title="区分"),
                alt.Tooltip("金額:Q", title="金額", format=",.0f"),
            ],
        )
        st.altair_chart(apply_altair_theme(cf_chart.properties(height=320)), use_container_width=True)

        latest_plan = plan_df.iloc[-1]
        dominant_key = max(label_map, key=lambda key: abs(float(latest_plan.get(key, 0.0))))
        dominant_label = label_map[dominant_key]
        dominant_value = float(latest_plan.get(dominant_key, 0.0))
        st.caption(
            f"直近の主要キャッシュフローは{dominant_label}で{dominant_value:,.0f}円です。"
        )
        st.markdown("</div>", unsafe_allow_html=True)

    with st.expander("キャッシュフロー明細", expanded=False):
        if cash_plan is None or cash_plan.empty:
            st.info("キャッシュフロー計画データがありません。")
        else:
            table_df = cash_plan.copy()
            table_df["month_label"] = table_df["month"].astype(str)
            export_df = table_df[[
                "month_label",
                "operating_cf",
                "investment_cf",
                "financing_cf",
                "loan_repayment",
            ]].copy()
            if cash_forecast is not None and not cash_forecast.empty:
                forecast_export = cash_forecast.copy()
                forecast_export["month_label"] = forecast_export["month"].astype(str)
                export_df = export_df.merge(
                    forecast_export[["month_label", "net_cf", "cash_balance"]],
                    on="month_label",
                    how="left",
                )
            else:
                export_df["net_cf"] = (
                    export_df["operating_cf"]
                    + export_df["financing_cf"]
                    - export_df["investment_cf"]
                    - export_df["loan_repayment"]
                )
                export_df["cash_balance"] = (
                    export_df["net_cf"].cumsum() + float(starting_cash)
                )

            display_df = export_df.rename(
                columns={
                    "month_label": "月",
                    "operating_cf": "営業CF",
                    "investment_cf": "投資CF",
                    "financing_cf": "財務CF",
                    "loan_repayment": "返済",
                    "net_cf": "純キャッシュフロー",
                    "cash_balance": "期末現金残高",
                }
            )
            format_columns = ["営業CF", "投資CF", "財務CF", "返済", "純キャッシュフロー", "期末現金残高"]
            formatted_df = display_df.copy()
            for column in format_columns:
                formatted_df[column] = formatted_df[column].map(lambda v: f"{v:,.0f}" if pd.notna(v) else "-")
            st.dataframe(formatted_df, hide_index=True, use_container_width=True)
            toolbar = st.columns(2)
            with toolbar[0]:
                download_button_from_df("CSV出力", display_df, "cash_flow_plan.csv")
            with toolbar[1]:
                st.button("PDF出力 (準備中)", disabled=True)


def render_data_status_section(
    merged_df: pd.DataFrame,
    cost_df: pd.DataFrame,
    subscription_df: pd.DataFrame,
    *,
    use_sample_data: bool,
    automated_sales_data: Dict[str, Any],
) -> None:
    """データアップロード状況をカード形式で表示する。"""

    st.markdown("### データアップロード状況")
    st.caption("チャネルや補助データの最新状態を確認できます。")

    cards: List[str] = []

    if merged_df is not None and not merged_df.empty:
        channel_summary = (
            merged_df.groupby("channel")
            .agg(
                records=("sales_amount", "size"),
                amount=("sales_amount", "sum"),
                latest=("order_date", "max"),
                earliest=("order_date", "min"),
            )
            .reset_index()
            .sort_values("records", ascending=False)
        )
        for _, row in channel_summary.iterrows():
            latest = pd.to_datetime(row["latest"]).strftime("%Y-%m-%d") if pd.notna(row["latest"]) else "-"
            earliest = pd.to_datetime(row["earliest"]).strftime("%Y-%m-%d") if pd.notna(row["earliest"]) else "-"
            meta = f"{earliest} 〜 {latest}"
            body = f"件数: {int(row['records']):,} / 売上高: {row['amount']:,.0f}円"
            cards.append(
                """
                <div class="data-status-card">
                    <div class="data-status-card__title">{title}</div>
                    <div class="data-status-card__meta">{meta}</div>
                    <div class="data-status-card__body">{body}</div>
                    <div class="data-status-card__status data-status-card__status--ok">✅ 正常</div>
                </div>
                """.format(
                    title=html.escape(str(row["channel"])),
                    meta=html.escape(meta),
                    body=html.escape(body),
                )
            )
    else:
        cards.append(
            """
            <div class="data-status-card">
                <div class="data-status-card__title">売上データ</div>
                <div class="data-status-card__meta">-</div>
                <div class="data-status-card__body">売上ファイルが未読み込みです。</div>
                <div class="data-status-card__status data-status-card__status--warning">⚠️ 未取込</div>
            </div>
            """
        )

    cost_loaded = cost_df is not None and not cost_df.empty
    cost_status_class = (
        "data-status-card__status data-status-card__status--ok"
        if cost_loaded
        else "data-status-card__status data-status-card__status--warning"
    )
    cost_status_label = "✅ 正常" if cost_loaded else "⚠️ 未登録"
    cost_body = (
        f"登録済みアイテム: {len(cost_df):,}件" if cost_loaded else "原価率データが未設定です。"
    )
    cards.append(
        """
        <div class="data-status-card">
            <div class="data-status-card__title">原価率マスタ</div>
            <div class="data-status-card__meta">-</div>
            <div class="data-status-card__body">{body}</div>
            <div class="{status_class}">{status}</div>
        </div>
        """.format(
            body=html.escape(cost_body),
            status_class=cost_status_class,
            status=html.escape(cost_status_label),
        )
    )

    sub_loaded = subscription_df is not None and not subscription_df.empty
    sub_status_class = (
        "data-status-card__status data-status-card__status--ok"
        if sub_loaded
        else "data-status-card__status data-status-card__status--warning"
    )
    sub_status_label = "✅ 正常" if sub_loaded else "⚠️ 未登録"
    sub_body = (
        f"月次レコード: {len(subscription_df):,}件" if sub_loaded else "サブスクKPIが未入力です。"
    )
    cards.append(
        """
        <div class="data-status-card">
            <div class="data-status-card__title">定期購買 / KPIデータ</div>
            <div class="data-status-card__meta">-</div>
            <div class="data-status-card__body">{body}</div>
            <div class="{status_class}">{status}</div>
        </div>
        """.format(
            body=html.escape(sub_body),
            status_class=sub_status_class,
            status=html.escape(sub_status_label),
        )
    )

    if automated_sales_data:
        api_last_fetched = st.session_state.get("api_last_fetched", {})
        api_reports = st.session_state.get("api_sales_validation", {})
        api_lines: List[str] = []
        error_count = 0
        warning_count = 0
        ok_count = 0
        for channel, df in automated_sales_data.items():
            last_fetch = api_last_fetched.get(channel)
            report = api_reports.get(channel)
            status_label = "正常"
            status_icon = "✅"
            if report and getattr(report, "has_errors", lambda: False)():
                status_label = "エラー"
                status_icon = "⛔"
                error_count += 1
            elif report and getattr(report, "has_warnings", lambda: False)():
                status_label = "警告あり"
                status_icon = "⚠️"
                warning_count += 1
            else:
                ok_count += 1
            timestamp = last_fetch.strftime("%Y-%m-%d %H:%M") if last_fetch else "-"
            api_lines.append(f"{channel}: {status_label} / 取得 {timestamp}")
        if error_count:
            api_status_class = "data-status-card__status data-status-card__status--error"
            api_status_label = f"⛔ エラー {error_count}件"
        elif warning_count:
            api_status_class = "data-status-card__status data-status-card__status--warning"
            api_status_label = f"⚠️ 警告 {warning_count}件"
        else:
            api_status_class = "data-status-card__status data-status-card__status--ok"
            api_status_label = f"✅ 正常 {ok_count}件"

        footnote_html = ""
        if api_lines:
            footnote_html = "<div class='data-status-card__footnote'>{}</div>".format(
                "<br />".join(html.escape(line) for line in api_lines)
            )

        cards.append(
            """
            <div class="data-status-card">
                <div class="data-status-card__title">API連携</div>
                <div class="data-status-card__meta">接続チャネル: {count}件</div>
                <div class="data-status-card__body">自動取得の最終実行状況を表示します。</div>
                <div class="{status_class}">{status}</div>
                {footnote}
            </div>
            """.format(
                count=len(automated_sales_data),
                status_class=api_status_class,
                status=html.escape(api_status_label),
                footnote=footnote_html,
            )
        )

    st.markdown(
        "<div class='data-status-grid'>{}</div>".format("".join(cards)),
        unsafe_allow_html=True,
    )

    if use_sample_data:
        st.caption("※ 現在はサンプルデータを表示しています。実データをアップロードすると自動的に置き換わります。")

def render_sidebar_upload_expander(
    label: str,
    *,
    uploader_key: str,
    description: str,
    multiple: bool,
    meta_text: str,
    help_text: str,
    file_types: Optional[List[str]] = None,
) -> Any:
    """サイドバーにアイコン付きのアップロード用アコーディオンを描画する。"""

    file_types = file_types or ["xlsx", "xls", "csv"]
    with st.sidebar.expander(label, expanded=False):
        st.markdown(
            f"""
            <div class="sidebar-upload-card">
                <div class="sidebar-upload-card__icons">
                    <span class="sidebar-upload-card__icon sidebar-upload-card__icon--csv">CSV</span>
                    <span class="sidebar-upload-card__icon sidebar-upload-card__icon--excel">XLSX</span>
                </div>
                <div class="sidebar-upload-card__body">
                    <div class="sidebar-upload-card__title">CSV / Excelファイルに対応</div>
                    <div class="sidebar-upload-card__meta">{meta_text}</div>
                    <p class="sidebar-upload-card__desc">{description}</p>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        uploaded = st.file_uploader(
            "ファイルを選択",
            type=file_types,
            accept_multiple_files=multiple,
            key=f"{uploader_key}_uploader",
            label_visibility="collapsed",
            help=help_text,
        )
    return uploaded


def main() -> None:
    inject_mckinsey_style()

    st.sidebar.header("データ設定")
    st.sidebar.markdown(
        """
        <div class="sidebar-section sidebar-section--emphasis">
            <div class="sidebar-section__eyebrow">データ準備</div>
            <div class="sidebar-section__title">サンプルデータの利用</div>
            <div class="sidebar-section__body">
                実データがそろっていない場合でも、サンプルデータでダッシュボードの動作を確認できます。
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    use_sample_data = st.sidebar.checkbox(
        "サンプルデータを使用",
        value=True,
        help="チェックするとダッシュボードにサンプルデータが読み込まれます。外すとアップロードしたファイルのみで指標を計算します。",
    )
    sample_status = (
        "サンプルデータを読み込み中です。"
        if use_sample_data
        else "アップロードしたファイルのみを使用しています。"
    )
    st.sidebar.markdown(
        f"<div class='sidebar-section__status'>{sample_status}</div>",
        unsafe_allow_html=True,
    )

    st.sidebar.markdown(
        "<div class='sidebar-subheading'>売上データアップロード</div>",
        unsafe_allow_html=True,
    )
    channel_files: Dict[str, List] = {}
    for config in SALES_UPLOAD_CONFIGS:
        channel_files[config["channel"]] = render_sidebar_upload_expander(
            config["label"],
            uploader_key=f"sales_{config['channel']}",
            description=config["description"],
            multiple=True,
            meta_text=UPLOAD_META_MULTIPLE,
            help_text=UPLOAD_HELP_MULTIPLE,
        )

    st.sidebar.markdown(
        "<div class='sidebar-subheading'>補助データ</div>",
        unsafe_allow_html=True,
    )
    ancillary_results: Dict[str, Any] = {}
    for config in ANCILLARY_UPLOAD_CONFIGS:
        ancillary_results[config["key"]] = render_sidebar_upload_expander(
            config["label"],
            uploader_key=config["key"],
            description=config["description"],
            multiple=config.get("multiple", False),
            meta_text=config.get("meta_text", UPLOAD_META_SINGLE),
            help_text=config.get("help_text", UPLOAD_HELP_SINGLE),
        )

    cost_file = ancillary_results.get("cost")
    subscription_file = ancillary_results.get("subscription")

    if "api_sales_data" not in st.session_state:
        st.session_state["api_sales_data"] = {}
    if "api_sales_validation" not in st.session_state:
        st.session_state["api_sales_validation"] = {}
    if "api_last_fetched" not in st.session_state:
        st.session_state["api_last_fetched"] = {}

    st.sidebar.markdown("---")
    with st.sidebar.expander("API/RPA自動連携設定", expanded=False):
        st.caption("各モールのAPIやRPAが出力したURLを登録すると、手動アップロードなしで売上データを取得できます。")
        for channel in channel_files.keys():
            endpoint = st.text_input(f"{channel} APIエンドポイント", key=f"api_endpoint_{channel}")
            token = st.text_input(
                f"{channel} APIトークン/キー",
                key=f"api_token_{channel}",
                type="password",
                help="必要に応じてBasic認証やBearerトークンを設定してください。",
            )
            params_raw = st.text_input(
                f"{channel} クエリパラメータ (key=value&...)",
                key=f"api_params_{channel}",
                help="日付範囲などの条件が必要な場合に指定します。",
            )

            params_dict: Optional[Dict[str, str]] = None
            if params_raw:
                parsed_pairs = parse_qsl(params_raw, keep_blank_values=False)
                if parsed_pairs:
                    params_dict = {k: v for k, v in parsed_pairs}

            fetch_now = st.button(f"{channel}の最新データを取得", key=f"fetch_api_{channel}")
            if fetch_now:
                if not endpoint:
                    st.warning("エンドポイントURLを入力してください。")
                else:
                    with st.spinner(f"{channel}のデータを取得中..."):
                        fetched_df, fetch_report = fetch_sales_from_endpoint(
                            endpoint,
                            token=token or None,
                            params=params_dict,
                            channel_hint=channel,
                        )
                    st.session_state["api_sales_data"][channel] = fetched_df
                    st.session_state["api_sales_validation"][channel] = fetch_report
                    st.session_state["api_last_fetched"][channel] = datetime.now()
                    if fetch_report.has_errors():
                        st.error(f"{channel}のAPI連携でエラーが発生しました。詳細はデータ管理タブをご確認ください。")
                    elif fetch_report.has_warnings():
                        st.warning(f"{channel}のデータは取得しましたが警告があります。データ管理タブで確認してください。")
                    else:
                        st.success(f"{channel}のデータ取得が完了しました。")

            last_fetch = st.session_state["api_last_fetched"].get(channel)
            if last_fetch:
                status_report: Optional[ValidationReport] = st.session_state["api_sales_validation"].get(channel)
                latest_df = st.session_state["api_sales_data"].get(channel)
                record_count = len(latest_df) if isinstance(latest_df, pd.DataFrame) else 0
                if status_report and status_report.has_errors():
                    status_level = "error"
                elif status_report and status_report.has_warnings():
                    status_level = "warning"
                else:
                    status_level = "ok"
                icon, status_label = STATUS_PILL_DETAILS.get(status_level, ("ℹ️", "情報"))
                st.markdown(
                    f"<div class='status-pill status-pill--{status_level}'>{icon} 状態: {status_label}</div>",
                    unsafe_allow_html=True,
                )
                st.markdown(
                    f"<div class='sidebar-meta'>最終取得: {last_fetch.strftime('%Y-%m-%d %H:%M')} / {record_count:,} 件</div>",
                    unsafe_allow_html=True,
                )

        if st.button("自動取得データをクリア", key="clear_api_sales"):
            st.session_state["api_sales_data"].clear()
            st.session_state["api_sales_validation"].clear()
            st.session_state["api_last_fetched"].clear()
            st.success("保存されていたAPI取得データをクリアしました。")

    fixed_cost = st.sidebar.number_input(
        "月間固定費（販管費のうち人件費・地代等）",
        value=float(DEFAULT_FIXED_COST),
        step=50_000.0,
        format="%.0f",
    )
    starting_cash = st.sidebar.number_input(
        "現在の現金残高（円）",
        value=3_000_000.0,
        step=100_000.0,
        format="%.0f",
    )

    with st.sidebar.expander("KPIの手入力（任意）"):
        manual_active = st.number_input("当月アクティブ顧客数", min_value=0.0, value=0.0, step=50.0)
        manual_new = st.number_input("当月新規顧客数", min_value=0.0, value=0.0, step=10.0)
        manual_repeat = st.number_input("当月リピート顧客数", min_value=0.0, value=0.0, step=10.0)
        manual_cancel = st.number_input("当月解約件数", min_value=0.0, value=0.0, step=5.0)
        manual_prev_active = st.number_input("前月契約数", min_value=0.0, value=0.0, step=50.0)
        manual_marketing = st.number_input("当月広告費", min_value=0.0, value=0.0, step=50_000.0)
        manual_ltv = st.number_input("LTV試算値", min_value=0.0, value=0.0, step=1_000.0)

        st.markdown("#### バランスト・スコアカード指標")
        manual_inventory_days = st.number_input(
            "在庫回転日数（目標: 45日以下）",
            min_value=0.0,
            value=45.0,
            step=1.0,
            help="内部プロセス視点: 在庫を現金化するまでの日数を把握します。",
        )
        manual_stockout_pct = st.number_input(
            "欠品率（%）",
            min_value=0.0,
            max_value=100.0,
            value=4.0,
            step=0.5,
            help="内部プロセス視点: 欠品による販売機会損失を監視します。",
        )
        manual_training_sessions = st.number_input(
            "従業員研修実施数（月内）",
            min_value=0.0,
            value=2.0,
            step=1.0,
            format="%.0f",
            help="学習・成長視点: 店長や経理がスキルを磨いた回数です。",
        )
        manual_new_products = st.number_input(
            "新商品リリース数（月内）",
            min_value=0.0,
            value=1.0,
            step=1.0,
            format="%.0f",
            help="学習・成長視点: 新しい価値提案の数を追跡します。",
        )

    automated_sales_data = st.session_state.get("api_sales_data", {})
    automated_reports = list(st.session_state.get("api_sales_validation", {}).values())

    try:
        with st.spinner(STATE_MESSAGES["loading"]["text"]):
            data_dict = load_data(
                use_sample_data,
                channel_files,
                cost_file,
                subscription_file,
                automated_sales=automated_sales_data,
                automated_reports=automated_reports,
            )
    except Exception:
        display_state_message(
            "error",
            action=lambda: st.experimental_rerun(),
            action_key="reload_after_error",
        )
        return

    sales_df = data_dict["sales"].copy()
    cost_df = data_dict["cost"].copy()
    subscription_df = data_dict["subscription"].copy()
    sales_validation: ValidationReport = data_dict.get("sales_validation", ValidationReport())

    if sales_df.empty:
        display_state_message("empty_data")
        return

    merged_full = merge_sales_and_costs(sales_df, cost_df)
    sales_validation.extend(validate_channel_fees(merged_full))

    freq_lookup = {label: freq for label, freq in PERIOD_FREQ_OPTIONS}
    freq_labels = list(freq_lookup.keys())
    default_freq_label = next(
        (label for label, freq in PERIOD_FREQ_OPTIONS if freq == "M"),
        freq_labels[0],
    )

    unique_channels = sorted(sales_df["channel"].dropna().unique().tolist())
    unique_categories = sorted(sales_df["category"].dropna().unique().tolist())
    global_min_date = normalize_date_input(sales_df["order_date"].min()) or date.today()
    global_max_date = normalize_date_input(sales_df["order_date"].max()) or date.today()
    if global_min_date > global_max_date:
        global_min_date, global_max_date = global_max_date, global_min_date
    global_default_period = suggest_default_period(global_min_date, global_max_date)

    store_candidates = ["全社"]
    if "store" in sales_df.columns:
        candidate_values = [str(value) for value in sales_df["store"].dropna().unique()]
        store_candidates.extend(candidate_values)
    store_candidates.extend(option for option in DEFAULT_STORE_OPTIONS if option not in store_candidates)
    store_options = list(dict.fromkeys(store_candidates)) or ["全社"]
    default_store = store_options[0]

    default_filters = {
        FILTER_STATE_KEYS["store"]: default_store,
        FILTER_STATE_KEYS["channels"]: unique_channels,
        FILTER_STATE_KEYS["categories"]: unique_categories,
        FILTER_STATE_KEYS["period"]: global_default_period,
        FILTER_STATE_KEYS["freq"]: default_freq_label,
    }

    store_state_key = FILTER_STATE_KEYS["store"]
    if store_state_key not in st.session_state or st.session_state[store_state_key] not in store_options:
        st.session_state[store_state_key] = default_store
    store_index = (
        store_options.index(st.session_state[store_state_key])
        if st.session_state[store_state_key] in store_options
        else 0
    )
    selected_store = st.sidebar.selectbox(
        "店舗選択",
        options=store_options,
        index=store_index,
        key=store_state_key,
        help="最後に選択した店舗は次回アクセス時も自動で設定されます。",
    )

    if selected_store and selected_store != "全社" and "store" in sales_df.columns:
        store_sales_df = sales_df[sales_df["store"] == selected_store].copy()
    else:
        store_sales_df = sales_df.copy()

    store_min_candidate = normalize_date_input(store_sales_df["order_date"].min()) if not store_sales_df.empty else None
    store_max_candidate = normalize_date_input(store_sales_df["order_date"].max()) if not store_sales_df.empty else None
    min_date = store_min_candidate or global_min_date
    max_date = store_max_candidate or global_max_date
    if min_date > max_date:
        min_date, max_date = max_date, min_date

    period_state_key = FILTER_STATE_KEYS["period"]
    stored_period = st.session_state.get(period_state_key)
    default_period = suggest_default_period(min_date, max_date)
    if (
        period_state_key not in st.session_state
        or not isinstance(stored_period, (list, tuple))
        or len(stored_period) != 2
    ):
        st.session_state[period_state_key] = default_period
    else:
        start_candidate = normalize_date_input(stored_period[0]) or min_date
        end_candidate = normalize_date_input(stored_period[1]) or max_date
        if start_candidate < min_date or end_candidate > max_date:
            st.session_state[period_state_key] = default_period
        else:
            st.session_state[period_state_key] = (start_candidate, end_candidate)

    st.sidebar.date_input(
        "表示期間（開始日 / 終了日）",
        value=st.session_state[period_state_key],
        min_value=min_date,
        max_value=max_date,
        key=period_state_key,
    )
    raw_period = st.session_state[period_state_key]
    if isinstance(raw_period, (list, tuple)) and len(raw_period) == 2:
        date_range = (
            normalize_date_input(raw_period[0]) or min_date,
            normalize_date_input(raw_period[1]) or max_date,
        )
    else:
        normalized_single = normalize_date_input(raw_period) or min_date
        date_range = (normalized_single, normalized_single)
    st.session_state[period_state_key] = date_range

    available_channels = sorted(store_sales_df["channel"].dropna().unique().tolist())
    channel_state_key = FILTER_STATE_KEYS["channels"]
    if channel_state_key not in st.session_state:
        st.session_state[channel_state_key] = available_channels
    else:
        preserved_channels = [ch for ch in st.session_state[channel_state_key] if ch in available_channels]
        if available_channels and not preserved_channels:
            preserved_channels = available_channels
        st.session_state[channel_state_key] = preserved_channels
    selected_channels = st.sidebar.multiselect(
        "表示するチャネル",
        options=available_channels,
        default=st.session_state[channel_state_key] if available_channels else [],
        key=channel_state_key,
        help="チャネル選択は関連レポートでも共有されます。",
    )

    available_categories = sorted(store_sales_df["category"].dropna().unique().tolist())
    category_state_key = FILTER_STATE_KEYS["categories"]
    if category_state_key not in st.session_state:
        st.session_state[category_state_key] = available_categories
    else:
        preserved_categories = [cat for cat in st.session_state[category_state_key] if cat in available_categories]
        if available_categories and not preserved_categories:
            preserved_categories = available_categories
        st.session_state[category_state_key] = preserved_categories
    selected_categories = st.sidebar.multiselect(
        "表示するカテゴリ",
        options=available_categories,
        default=st.session_state[category_state_key] if available_categories else [],
        key=category_state_key,
    )

    freq_state_key = FILTER_STATE_KEYS["freq"]
    if freq_state_key not in st.session_state or st.session_state[freq_state_key] not in freq_lookup:
        st.session_state[freq_state_key] = default_freq_label
    selected_granularity_label = st.sidebar.selectbox(
        "ダッシュボード表示粒度",
        options=freq_labels,
        index=freq_labels.index(st.session_state[freq_state_key]),
        key=freq_state_key,
    )
    selected_freq = freq_lookup[selected_granularity_label]

    st.sidebar.markdown("---")
    if st.sidebar.button("設定をリセット", key="reset_filter_button"):
        reset_filters(default_filters)
    if st.sidebar.button("セッション状態を初期化", key="clear_session_button"):
        st.session_state.clear()
        st.experimental_rerun()

    selected_store = st.session_state[store_state_key]
    selected_channels = st.session_state[channel_state_key]
    selected_categories = st.session_state[category_state_key]
    date_range = st.session_state[period_state_key]

    filter_signature = build_filter_signature(
        selected_store,
        selected_channels,
        selected_categories,
        date_range,
        selected_granularity_label,
    )
    signature_key = FILTER_STATE_KEYS["signature"]
    if signature_key not in st.session_state:
        st.session_state[signature_key] = filter_signature
    elif st.session_state[signature_key] != filter_signature:
        display_state_message("success", action_key="filters_success_message")
        st.session_state[signature_key] = filter_signature

    store_filter: Optional[List[str]] = None
    if selected_store and selected_store not in ("全社", None):
        store_filter = [selected_store]
    if isinstance(date_range, (tuple, list)) and len(date_range) == 2:
        date_range_list = [date_range[0], date_range[1]]
    else:
        date_range_list = [date_range, date_range]

    filtered_sales = apply_filters(
        sales_df,
        selected_channels,
        date_range_list,
        selected_categories,
        stores=store_filter,
    )
    if filtered_sales.empty:
        display_state_message(
            "empty_data",
            action=lambda: reset_filters(default_filters),
            action_key="reset_after_empty",
        )
    merged_df = merge_sales_and_costs(filtered_sales, cost_df)
    segmented_sales_df = annotate_customer_segments(merged_df)
    monthly_summary = monthly_sales_summary(merged_df)
    period_summary = summarize_sales_by_period(merged_df, selected_freq)

    kpi_overrides = {}
    if manual_active > 0:
        kpi_overrides["active_customers"] = manual_active
    if manual_new > 0:
        kpi_overrides["new_customers"] = manual_new
    if manual_repeat > 0:
        kpi_overrides["repeat_customers"] = manual_repeat
    if manual_cancel > 0:
        kpi_overrides["cancelled_subscriptions"] = manual_cancel
    if manual_prev_active > 0:
        kpi_overrides["previous_active_customers"] = manual_prev_active
    if manual_marketing > 0:
        kpi_overrides["marketing_cost"] = manual_marketing
    if manual_ltv > 0:
        kpi_overrides["ltv"] = manual_ltv

    kpi_overrides["inventory_turnover_days"] = manual_inventory_days
    kpi_overrides["stockout_rate"] = manual_stockout_pct / 100 if manual_stockout_pct >= 0 else np.nan
    kpi_overrides["training_sessions"] = manual_training_sessions
    kpi_overrides["new_product_count"] = manual_new_products

    kpis = calculate_kpis(merged_df, subscription_df, overrides=kpi_overrides)
    kpi_history_df = build_kpi_history_df(merged_df, subscription_df, kpi_overrides)
    kpi_period_summary = aggregate_kpi_history(kpi_history_df, selected_freq)

    base_pl = create_current_pl(merged_df, subscription_df, fixed_cost=fixed_cost)
    default_cash_plan = create_default_cashflow_plan(merged_df)
    default_cash_forecast = forecast_cashflow(default_cash_plan, starting_cash)

    alerts = build_alerts(monthly_summary, kpis, default_cash_forecast)

    channel_share_df = compute_channel_share(merged_df)
    category_share_df = compute_category_share(merged_df)

    latest_timestamp = None
    if not merged_df.empty and "order_date" in merged_df.columns:
        latest_timestamp = merged_df["order_date"].max()
    if latest_timestamp is not None and pd.notna(latest_timestamp):
        if isinstance(latest_timestamp, pd.Timestamp):
            latest_label = latest_timestamp.strftime("%Y-%m-%d")
        else:
            latest_label = str(latest_timestamp)
    else:
        latest_label = "-"

    range_label = "-"
    if isinstance(date_range, (tuple, list)) and len(date_range) == 2:
        start_value, end_value = date_range
        start_label = start_value.strftime("%Y-%m-%d") if hasattr(start_value, "strftime") else str(start_value)
        end_label = end_value.strftime("%Y-%m-%d") if hasattr(end_value, "strftime") else str(end_value)
        range_label = f"{start_label} 〜 {end_label}"

    total_records = int(len(merged_df)) if not merged_df.empty else 0
    alert_count = len(alerts) if alerts else 0

    search_query = render_search_bar()

    with st.container():
        st.markdown("<div class='surface-card main-nav-block'>", unsafe_allow_html=True)
        selected_main, selected_section = render_navigation()
        st.markdown("</div>", unsafe_allow_html=True)

    render_breadcrumb(selected_main, selected_section)

    if search_query:
        render_global_search_results(search_query, merged_df)
        st.divider()

    if selected_section == "ダッシュボード":
        st.subheader("経営ダッシュボード")
        if kpi_period_summary.empty:
            st.info(
                "KPI情報が不足しています。KPIデータをアップロードするか、サイドバーで数値を入力してください。"
            )
        else:
            period_options = kpi_period_summary["period_label"].tolist()
            default_period_idx = len(period_options) - 1 if period_options else 0
            selected_dashboard_period = st.selectbox(
                f"{selected_granularity_label}の表示期間",
                options=period_options,
                index=default_period_idx,
                key="dashboard_period_select",
            )
            selected_kpi_row = kpi_period_summary[
                kpi_period_summary["period_label"] == selected_dashboard_period
            ].iloc[0]
            selected_period = selected_kpi_row["period"]
            period_row = period_summary[period_summary["period"] == selected_period]
            period_start = pd.to_datetime(selected_kpi_row["period_start"]).date()
            period_end = pd.to_datetime(selected_kpi_row["period_end"]).date()

            gross_rate_value = selected_kpi_row.get("gross_margin_rate")
            gross_target = KGI_TARGETS.get("gross_margin_rate")
            if (
                gross_rate_value is not None
                and not pd.isna(gross_rate_value)
                and gross_target is not None
                and gross_rate_value < gross_target
            ):
                display_state_message(
                    "warning_gross_margin",
                    action=lambda: jump_to_section("分析", "利益分析"),
                    action_label="粗利タブを開く",
                    action_key="warning_gross_margin_button",
                )

            render_kgi_cards(selected_kpi_row, period_row, default_cash_forecast, starting_cash)
            render_dashboard_meta(latest_label, range_label, total_records, alert_count)
            render_status_banner(alerts)
            st.caption(f"対象期間: {period_start} 〜 {period_end}")

            render_first_level_kpi_strip(kpi_period_summary, selected_kpi_row)

            tab_labels = ["売上", "粗利", "在庫", "資金"]
            sales_tab, gross_tab, inventory_tab, cash_tab = st.tabs(
                [f"📈 {label}" for label in tab_labels]
            )
            with sales_tab:
                render_sales_tab(
                    merged_df,
                    period_summary,
                    channel_share_df,
                    category_share_df,
                    selected_granularity_label,
                )
            with gross_tab:
                render_gross_tab(merged_df, period_summary, selected_granularity_label)
            with inventory_tab:
                render_inventory_tab(merged_df, kpi_period_summary, selected_kpi_row)
            with cash_tab:
                render_cash_tab(default_cash_plan, default_cash_forecast, starting_cash)

            render_data_status_section(
                merged_df,
                cost_df,
                subscription_df,
                use_sample_data=use_sample_data,
                automated_sales_data=automated_sales_data,
            )
            st.divider()

    elif selected_section == "売上分析":
        st.subheader("売上分析")
        if merged_df.empty:
            st.info("売上データがありません。")
        else:
            st.caption("グラフをクリックすると他の可視化も同じ条件で絞り込まれます。")
            sales_cross_filters = st.session_state.setdefault(
                "sales_cross_filters", {"channel": None, "category": None}
            )

            available_analysis_channels = sorted(merged_df["channel"].unique())
            available_analysis_categories = sorted(merged_df["category"].unique())
            if (
                sales_cross_filters.get("channel")
                and sales_cross_filters["channel"] not in available_analysis_channels
            ):
                sales_cross_filters["channel"] = None
            if (
                sales_cross_filters.get("category")
                and sales_cross_filters["category"] not in available_analysis_categories
            ):
                sales_cross_filters["category"] = None

            analysis_df = merged_df.copy()
            active_highlights: List[str] = []
            if sales_cross_filters.get("channel"):
                analysis_df = analysis_df[analysis_df["channel"] == sales_cross_filters["channel"]]
                active_highlights.append(f"チャネル: {sales_cross_filters['channel']}")
            if sales_cross_filters.get("category"):
                analysis_df = analysis_df[analysis_df["category"] == sales_cross_filters["category"]]
                active_highlights.append(f"カテゴリ: {sales_cross_filters['category']}")

            if active_highlights:
                info_col, clear_col = st.columns([5, 1])
                info_col.info("ハイライト適用中: " + " / ".join(active_highlights))
                if clear_col.button("ハイライトをクリア", key="clear_sales_highlight"):
                    st.session_state["sales_cross_filters"] = {"channel": None, "category": None}
                    analysis_df = merged_df.copy()
                    active_highlights = []

            channel_trend_full = merged_df.copy()
            channel_trend_full["period"] = channel_trend_full["order_date"].dt.to_period(selected_freq)
            channel_trend_full = (
                channel_trend_full.groupby(["period", "channel"])["sales_amount"].sum().reset_index()
            )
            channel_trend_full["period_start"] = channel_trend_full["period"].dt.to_timestamp()
            channel_trend_full["period_label"] = channel_trend_full["period"].apply(
                lambda p: format_period_label(p, selected_freq)
            )
            channel_trend_full.sort_values(["channel", "period_start"], inplace=True)

            channel_chart = px.line(
                channel_trend_full,
                x="period_start",
                y="sales_amount",
                color="channel",
                markers=True,
                labels={
                    "sales_amount": "売上高",
                    "period_start": f"{selected_granularity_label}開始日",
                },
                custom_data=["channel", "period_label"],
                color_discrete_sequence=PLOTLY_COLORWAY,
            )
            channel_chart = apply_chart_theme(channel_chart)
            channel_chart.update_layout(
                clickmode="event+select",
                legend=dict(title="", itemclick="toggleothers", itemdoubleclick="toggle"),
            )
            for trace in channel_chart.data:
                trace.update(
                    hovertemplate="期間=%{customdata[1]}<br>チャネル=%{customdata[0]}<br>売上高=%{y:,.0f}円<extra></extra>"
                )
                if sales_cross_filters.get("channel") and trace.name != sales_cross_filters["channel"]:
                    trace.update(opacity=0.25, line={"width": 1})
                else:
                    trace.update(line={"width": 3})
            channel_events = plotly_events(
                channel_chart,
                click_event=True,
                override_width="100%",
                override_height=420,
                key="channel_trend_events",
            )
            if channel_events:
                clicked_channel = channel_events[0]["customdata"][0]
                current = st.session_state["sales_cross_filters"].get("channel")
                if current == clicked_channel:
                    st.session_state["sales_cross_filters"]["channel"] = None
                else:
                    st.session_state["sales_cross_filters"]["channel"] = clicked_channel

            category_sales_full = merged_df.copy()
            category_sales_full["period"] = category_sales_full["order_date"].dt.to_period(selected_freq)
            category_sales_full = (
                category_sales_full.groupby(["period", "category"])["sales_amount"].sum().reset_index()
            )
            category_sales_full["period_start"] = category_sales_full["period"].dt.to_timestamp()
            category_sales_full["period_label"] = category_sales_full["period"].apply(
                lambda p: format_period_label(p, selected_freq)
            )
            category_sales_full.sort_values(["category", "period_start"], inplace=True)

            category_bar = px.bar(
                category_sales_full,
                x="period_start",
                y="sales_amount",
                color="category",
                labels={
                    "sales_amount": "売上高",
                    "period_start": f"{selected_granularity_label}開始日",
                },
                custom_data=["category", "period_label"],
                color_discrete_sequence=PLOTLY_COLORWAY,
            )
            category_bar = apply_chart_theme(category_bar)
            category_bar.update_layout(
                barmode="stack",
                clickmode="event+select",
                legend=dict(title="", itemclick="toggleothers", itemdoubleclick="toggle"),
            )
            for trace in category_bar.data:
                trace.update(
                    hovertemplate="期間=%{customdata[1]}<br>カテゴリ=%{customdata[0]}<br>売上高=%{y:,.0f}円<extra></extra>"
                )
                if sales_cross_filters.get("category") and trace.name != sales_cross_filters["category"]:
                    trace.update(opacity=0.35)
                else:
                    trace.update(opacity=0.9)
            category_events = plotly_events(
                category_bar,
                click_event=True,
                override_width="100%",
                override_height=420,
                key="category_sales_events",
            )
            if category_events:
                clicked_category = category_events[0]["customdata"][0]
                current_category = st.session_state["sales_cross_filters"].get("category")
                if current_category == clicked_category:
                    st.session_state["sales_cross_filters"]["category"] = None
                else:
                    st.session_state["sales_cross_filters"]["category"] = clicked_category

            analysis_summary = summarize_sales_by_period(analysis_df, selected_freq)
            if analysis_df.empty:
                st.warning("選択された条件に該当するデータがありません。")
            elif analysis_summary.empty:
                st.info("指定した粒度で集計できる期間データがありません。")
            else:
                yoy_table = analysis_summary.tail(12)[
                    ["period_label", "sales_amount", "sales_yoy", "sales_mom"]
                ]
                yoy_table = yoy_table.rename(
                    columns={
                        "period_label": "期間",
                        "sales_amount": "売上高",
                        "sales_yoy": "前年同期比",
                        "sales_mom": "前期比",
                    }
                )
                st.dataframe(yoy_table)

    elif selected_section == "利益分析":
        st.subheader("利益分析")
        if merged_df.empty:
            st.info("データがありません。")
        else:
            product_profit = (
                merged_df.groupby(["product_code", "product_name", "category"], as_index=False)[
                    [
                        "sales_amount",
                        "estimated_cost",
                        "net_gross_profit",
                        "quantity",
                        "channel_fee_amount",
                    ]
                ]
                .sum()
            )
            product_profit["gross_margin_rate"] = product_profit["net_gross_profit"] / product_profit["sales_amount"]
            product_profit["average_unit_price"] = np.where(
                product_profit["quantity"] > 0,
                product_profit["sales_amount"] / product_profit["quantity"],
                np.nan,
            )
            product_profit["ad_ratio"] = np.where(
                product_profit["sales_amount"] != 0,
                product_profit["channel_fee_amount"] / product_profit["sales_amount"],
                np.nan,
            )
            product_profit.sort_values("net_gross_profit", ascending=False, inplace=True)
            display_columns = {
                "product_code": "商品コード",
                "product_name": "商品名",
                "category": "カテゴリ",
                "sales_amount": "売上高",
                "net_gross_profit": "粗利",
                "gross_margin_rate": "粗利率",
                "average_unit_price": "平均単価",
                "quantity": "販売個数",
                "ad_ratio": "広告費比率",
            }
            st.dataframe(
                product_profit[list(display_columns.keys())]
                .rename(columns=display_columns)
                .style.format({
                    "売上高": "{:,.0f}",
                    "粗利": "{:,.0f}",
                    "粗利率": "{:.2%}",
                    "平均単価": "{:,.0f}",
                    "販売個数": "{:,.0f}",
                    "広告費比率": "{:.2%}",
                }),
                use_container_width=True,
            )

            channel_profit = (
                merged_df.groupby("channel")["net_gross_profit"].sum().reset_index()
            )
            channel_profit_chart = px.bar(
                channel_profit,
                x="channel",
                y="net_gross_profit",
                labels={"channel": "チャネル", "net_gross_profit": "粗利"},
                title="チャネル別粗利比較",
                color_discrete_sequence=[ACCENT_BLUE],
            )
            channel_profit_chart = apply_chart_theme(channel_profit_chart)
            channel_profit_chart.update_layout(
                legend=dict(title=""),
                xaxis_title="チャネル",
                yaxis_title="粗利",
            )
            st.plotly_chart(channel_profit_chart, use_container_width=True)

            top_products = product_profit.head(10).copy()
            st.subheader("高利益商材トップ10")
            selected_product_code = st.session_state.setdefault("profit_focus_product", None)
            if selected_product_code and selected_product_code not in top_products["product_code"].values:
                st.session_state["profit_focus_product"] = None
                selected_product_code = None

            top_products_sorted = top_products.sort_values("net_gross_profit")
            top_products_chart = px.bar(
                top_products_sorted,
                x="net_gross_profit",
                y="product_name",
                orientation="h",
                labels={"net_gross_profit": "粗利", "product_name": "商品名"},
                custom_data=["product_code", "product_name"],
                color_discrete_sequence=[ACCENT_BLUE],
            )
            top_products_chart = apply_chart_theme(top_products_chart)
            highlight_code = st.session_state.get("profit_focus_product")
            bar_colors = [
                ACCENT_ORANGE if code == highlight_code else ACCENT_BLUE
                for code in top_products_sorted["product_code"]
            ]
            top_products_chart.update_traces(
                marker_color=bar_colors,
                hovertemplate="%{customdata[1]}<br>粗利=%{x:,.0f}円<extra></extra>",
            )
            top_products_chart.update_layout(
                height=420,
                xaxis_title="粗利",
                yaxis_title="商品名",
                clickmode="event+select",
            )
            events_top_products = plotly_events(
                top_products_chart,
                click_event=True,
                override_width="100%",
                override_height=420,
                key="top_products_events",
            )
            if events_top_products:
                clicked_code = events_top_products[0]["customdata"][0]
                current_code = st.session_state.get("profit_focus_product")
                if current_code == clicked_code:
                    st.session_state["profit_focus_product"] = None
                else:
                    st.session_state["profit_focus_product"] = clicked_code

            focus_code = st.session_state.get("profit_focus_product")
            if focus_code is None and not product_profit.empty:
                focus_code = product_profit.iloc[0]["product_code"]
                st.session_state["profit_focus_product"] = focus_code

            if focus_code and focus_code in product_profit["product_code"].values:
                focus_row = product_profit[product_profit["product_code"] == focus_code].iloc[0]
                st.markdown(
                    f"### 選択した商品の詳細: {focus_row['product_name']} ({focus_code})"
                )
                detail_cols = st.columns(5)
                detail_cols[0].metric("売上高", f"{focus_row['sales_amount']:,.0f} 円")
                detail_cols[1].metric("粗利", f"{focus_row['net_gross_profit']:,.0f} 円")
                detail_cols[2].metric(
                    "平均単価",
                    f"{focus_row['average_unit_price']:,.0f} 円"
                    if pd.notna(focus_row["average_unit_price"])
                    else "-",
                )
                detail_cols[3].metric(
                    "販売個数",
                    f"{focus_row['quantity']:,.0f} 個"
                    if pd.notna(focus_row["quantity"])
                    else "-",
                )
                detail_cols[4].metric(
                    "広告費比率",
                    f"{focus_row['ad_ratio'] * 100:.2f}%"
                    if pd.notna(focus_row["ad_ratio"])
                    else "-",
                )

                product_detail = merged_df[merged_df["product_code"] == focus_code].copy()
                channel_breakdown = (
                    product_detail.groupby("channel")[
                        ["sales_amount", "net_gross_profit", "quantity", "channel_fee_amount"]
                    ]
                    .sum()
                    .reset_index()
                )
                channel_breakdown["広告費比率"] = np.where(
                    channel_breakdown["sales_amount"] != 0,
                    channel_breakdown["channel_fee_amount"] / channel_breakdown["sales_amount"],
                    np.nan,
                )
                if not channel_breakdown.empty:
                    breakdown_chart = px.bar(
                        channel_breakdown,
                        x="channel",
                        y="net_gross_profit",
                        labels={"channel": "チャネル", "net_gross_profit": "粗利"},
                        title="選択商品のチャネル別粗利",
                        color_discrete_sequence=[ACCENT_BLUE],
                    )
                    breakdown_chart = apply_chart_theme(breakdown_chart)
                    breakdown_chart.update_layout(
                        legend=dict(title=""),
                        xaxis_title="チャネル",
                        yaxis_title="粗利",
                    )
                    st.plotly_chart(breakdown_chart, use_container_width=True)
                    st.dataframe(
                        channel_breakdown.rename(
                            columns={
                                "channel": "チャネル",
                                "sales_amount": "売上高",
                                "net_gross_profit": "粗利",
                                "quantity": "販売個数",
                            }
                        ).style.format(
                            {
                                "売上高": "{:,.0f}",
                                "粗利": "{:,.0f}",
                                "販売個数": "{:,.0f}",
                                "広告費比率": "{:.2%}",
                            }
                        ),
                        use_container_width=True,
                    )

                product_trend = product_detail.copy()
                product_trend["period"] = product_trend["order_date"].dt.to_period(selected_freq)
                product_trend_summary = (
                    product_trend.groupby("period")[
                        ["sales_amount", "net_gross_profit", "quantity"]
                    ]
                    .sum()
                    .reset_index()
                )
                if not product_trend_summary.empty:
                    product_trend_summary["period_start"] = product_trend_summary["period"].dt.to_timestamp()
                    product_trend_summary["period_label"] = product_trend_summary["period"].apply(
                        lambda p: format_period_label(p, selected_freq)
                    )
                    profit_trend_chart = px.line(
                        product_trend_summary,
                        x="period_start",
                        y="net_gross_profit",
                        markers=True,
                        labels={
                            "period_start": f"{selected_granularity_label}開始日",
                            "net_gross_profit": "粗利",
                        },
                        hover_data={"period_label": True},
                        color_discrete_sequence=[ACCENT_BLUE],
                    )
                    profit_trend_chart = apply_chart_theme(profit_trend_chart)
                    profit_trend_chart.update_layout(title="選択商品の粗利推移")
                    st.plotly_chart(profit_trend_chart, use_container_width=True)
                    st.dataframe(
                        product_trend_summary.rename(
                            columns={
                                "period_label": "期間",
                                "sales_amount": "売上高",
                                "net_gross_profit": "粗利",
                                "quantity": "販売個数",
                            }
                        ).style.format(
                            {
                                "売上高": "{:,.0f}",
                                "粗利": "{:,.0f}",
                                "販売個数": "{:,.0f}",
                            }
                        ),
                        use_container_width=True,
                    )
            else:
                st.info("表示する高利益商材がありません。")

    elif selected_section == "財務モニタリング":
        st.subheader("財務モニタリング")
        st.markdown("売上計画や広告費を調整してPL・キャッシュフローをシミュレートします。")

        col1, col2, col3, col4 = st.columns(4)
        sales_growth = col1.slider("売上成長率", min_value=-0.5, max_value=0.5, value=0.05, step=0.01)
        cost_adj = col2.slider("原価率変動", min_value=-0.1, max_value=0.1, value=0.0, step=0.01)
        sga_change = col3.slider("販管費変動率", min_value=-0.3, max_value=0.3, value=0.0, step=0.01)
        extra_ad = col4.number_input("追加広告費", min_value=0.0, value=0.0, step=50_000.0, format="%.0f")

        pl_result = simulate_pl(
            base_pl,
            sales_growth_rate=sales_growth,
            cost_rate_adjustment=cost_adj,
            sga_change_rate=sga_change,
            additional_ad_cost=extra_ad,
        )
        st.dataframe(pl_result.style.format({"現状": "{:,.0f}", "シナリオ": "{:,.0f}", "増減": "{:,.0f}"}))

        st.metric(
            "シナリオ営業利益",
            f"{pl_result.loc[pl_result['項目'] == '営業利益', 'シナリオ'].iloc[0]:,.0f} 円",
            delta=f"{pl_result.loc[pl_result['項目'] == '営業利益', '増減'].iloc[0]:,.0f} 円",
        )

        plan_edit = create_default_cashflow_plan(merged_df).copy()
        plan_edit["month"] = plan_edit["month"].astype(str)
        with st.expander("キャッシュフロープランを編集"):
            edited_plan = st.data_editor(
                plan_edit,
                num_rows="dynamic",
                use_container_width=True,
            )
        if isinstance(edited_plan, pd.DataFrame):
            plan_to_use = edited_plan.copy()
        else:
            plan_to_use = pd.DataFrame(edited_plan)
        if not plan_to_use.empty:
            plan_to_use["month"] = plan_to_use["month"].apply(lambda x: pd.Period(x, freq="M"))
        cash_forecast = forecast_cashflow(plan_to_use, starting_cash)
        if not cash_forecast.empty:
            cash_chart = px.line(
                cash_forecast.assign(month=cash_forecast["month"].astype(str)),
                x="month",
                y="cash_balance",
                markers=True,
                title="資金残高予測",
                color_discrete_sequence=[ACCENT_BLUE],
            )
            cash_chart = apply_chart_theme(cash_chart)
            cash_chart.update_layout(yaxis_title="円", xaxis_title="月")
            st.plotly_chart(cash_chart, use_container_width=True)
            st.dataframe(cash_forecast)
        else:
            st.info("キャッシュフロープランが未設定です。")

    elif selected_section == "KPIモニタリング":
        st.subheader("KPIモニタリング")
        if kpi_history_df.empty:
            st.info("KPI履歴がありません。")
        else:
            kpi_history_display = kpi_history_df.sort_values("month").copy()
            kpi_history_display["month_str"] = kpi_history_display["month"].astype(str)
            kpi_charts = st.tabs(["LTV", "CAC", "リピート率", "チャーン率", "ROAS"])

            with kpi_charts[0]:
                fig = px.line(
                    kpi_history_display,
                    x="month_str",
                    y="ltv",
                    markers=True,
                    title="LTV推移",
                    color_discrete_sequence=[ACCENT_BLUE],
                )
                fig = apply_chart_theme(fig)
                st.plotly_chart(fig, use_container_width=True)
            with kpi_charts[1]:
                fig = px.line(
                    kpi_history_display,
                    x="month_str",
                    y="cac",
                    markers=True,
                    title="CAC推移",
                    color_discrete_sequence=[ACCENT_BLUE],
                )
                fig = apply_chart_theme(fig)
                st.plotly_chart(fig, use_container_width=True)
            with kpi_charts[2]:
                fig = px.bar(
                    kpi_history_display,
                    x="month_str",
                    y="repeat_rate",
                    title="リピート率推移",
                    color_discrete_sequence=[ACCENT_BLUE],
                )
                fig = apply_chart_theme(fig)
                st.plotly_chart(fig, use_container_width=True)
            with kpi_charts[3]:
                fig = px.bar(
                    kpi_history_display,
                    x="month_str",
                    y="churn_rate",
                    title="チャーン率推移",
                    color_discrete_sequence=[ACCENT_ORANGE],
                )
                fig = apply_chart_theme(fig)
                st.plotly_chart(fig, use_container_width=True)
            with kpi_charts[4]:
                fig = px.line(
                    kpi_history_display,
                    x="month_str",
                    y="roas",
                    markers=True,
                    title="ROAS推移",
                    color_discrete_sequence=[ACCENT_BLUE],
                )
                fig = apply_chart_theme(fig)
                st.plotly_chart(fig, use_container_width=True)

            st.dataframe(
                kpi_history_display[
                    [
                        "month_str",
                        "sales",
                        "gross_profit",
                        "ltv",
                        "arpu",
                        "repeat_rate",
                        "churn_rate",
                        "roas",
                        "cac",
                    ]
                ].rename(columns={"month_str": "month"})
            )

            st.markdown("### KPIセグメント分析")
            segment_months = (
                segmented_sales_df["order_month"].dropna().sort_values().unique()
                if not segmented_sales_df.empty and "order_month" in segmented_sales_df.columns
                else []
            )
            period_options = ["全期間"]
            period_map: Dict[str, Optional[pd.Period]] = {"全期間": None}
            for period_value in segment_months:
                label = str(period_value)
                period_options.append(label)
                period_map[label] = period_value
            default_period_index = len(period_options) - 1 if len(period_options) > 1 else 0
            selected_period_label = st.selectbox(
                "分析対象期間",
                options=period_options,
                index=default_period_index,
                help="チャネル別・カテゴリ別のKPI集計に用いる期間を選択します。",
            )
            selected_period_value = period_map.get(selected_period_label)
            if selected_period_value is None:
                segmented_target_df = segmented_sales_df.copy()
            else:
                segmented_target_df = segmented_sales_df[
                    segmented_sales_df["order_month"] == selected_period_value
                ]

            if segmented_target_df.empty:
                st.info("選択された期間に該当するデータがありません。")
            else:
                breakdown_configs = [
                    ("チャネル別", "channel", "チャネル"),
                    ("カテゴリ別", "category", "商品カテゴリ"),
                    ("顧客区分別", "customer_segment", "顧客区分"),
                ]
                breakdown_tables: List[Tuple[str, str, str, pd.DataFrame]] = []
                for title, column, label in breakdown_configs:
                    df_breakdown = compute_kpi_breakdown(
                        segmented_target_df, column, kpi_totals=kpis
                    )
                    breakdown_tables.append((title, column, label, df_breakdown))

                if "campaign" in segmented_target_df.columns:
                    campaign_breakdown = compute_kpi_breakdown(
                        segmented_target_df, "campaign", kpi_totals=kpis
                    )
                    breakdown_tables.append(
                        ("キャンペーン別", "campaign", "キャンペーン", campaign_breakdown)
                    )

                st.caption("広告費や解約率は最新KPI値をシェアに応じて按分した推計値です。")
                breakdown_tabs = st.tabs([title for title, *_ in breakdown_tables])
                for tab_obj, (title, column, label, df_breakdown) in zip(
                    breakdown_tabs, breakdown_tables
                ):
                    with tab_obj:
                        if df_breakdown is None or df_breakdown.empty:
                            st.info(f"{label}別のKPIを算出するためのデータが不足しています。")
                            continue

                        chart_data = df_breakdown.nlargest(10, "sales_amount")
                        bar_chart = px.bar(
                            chart_data,
                            x=column,
                            y="sales_amount",
                            labels={column: label, "sales_amount": "売上高"},
                            title=f"{label}別売上高 (上位{min(len(chart_data), 10)}件)",
                            color_discrete_sequence=PLOTLY_COLORWAY,
                        )
                        bar_chart = apply_chart_theme(bar_chart)
                        bar_chart.update_layout(yaxis_title="円", xaxis_title=label)
                        st.plotly_chart(bar_chart, use_container_width=True)

                        display_df = df_breakdown.rename(
                            columns={
                                column: label,
                                "sales_amount": "売上高",
                                "gross_profit": "粗利",
                                "gross_margin_rate": "粗利率",
                                "sales_share": "売上構成比",
                                "active_customers": "顧客数",
                                "new_customers": "新規顧客数",
                                "repeat_customers": "リピート顧客数",
                                "reactivated_customers": "休眠復活顧客数",
                                "repeat_rate": "リピート率",
                                "churn_rate": "推定解約率",
                                "arpu": "ARPU",
                                "ltv": "推定LTV",
                                "cac": "CAC",
                                "roas": "ROAS",
                                "marketing_cost": "広告費配分",
                                "profit_contribution": "粗利貢献額",
                                "profit_per_customer": "顧客あたり利益",
                                "avg_order_value": "平均受注単価",
                                "orders": "注文件数",
                            }
                        )
                        ordered_columns = [
                            label,
                            "売上高",
                            "粗利",
                            "粗利率",
                            "売上構成比",
                            "顧客数",
                            "新規顧客数",
                            "リピート顧客数",
                            "休眠復活顧客数",
                            "リピート率",
                            "推定解約率",
                            "ARPU",
                            "推定LTV",
                            "CAC",
                            "ROAS",
                            "広告費配分",
                            "粗利貢献額",
                            "顧客あたり利益",
                            "平均受注単価",
                            "注文件数",
                        ]
                        existing_columns = [col for col in ordered_columns if col in display_df.columns]
                        formatters = {
                            "売上高": "{:,.0f}",
                            "粗利": "{:,.0f}",
                            "粗利率": "{:.1%}",
                            "売上構成比": "{:.1%}",
                            "顧客数": "{:,.0f}",
                            "新規顧客数": "{:,.0f}",
                            "リピート顧客数": "{:,.0f}",
                            "休眠復活顧客数": "{:,.0f}",
                            "リピート率": "{:.1%}",
                            "推定解約率": "{:.1%}",
                            "ARPU": "{:,.0f}",
                            "推定LTV": "{:,.0f}",
                            "CAC": "{:,.0f}",
                            "ROAS": "{:,.2f}倍",
                            "広告費配分": "{:,.0f}",
                            "粗利貢献額": "{:,.0f}",
                            "顧客あたり利益": "{:,.0f}",
                            "平均受注単価": "{:,.0f}",
                            "注文件数": "{:,.0f}",
                        }
                        st.dataframe(
                            display_df[existing_columns].style.format({k: v for k, v in formatters.items() if k in existing_columns}),
                            use_container_width=True,
                        )

            profit_column = (
                "net_gross_profit"
                if "net_gross_profit" in segmented_target_df.columns
                else "gross_profit"
                if "gross_profit" in segmented_target_df.columns
                else None
            )
            repeat_scope_df = (
                segmented_target_df[
                    segmented_target_df.get("customer_segment", "既存").ne("新規")
                ]
                if not segmented_target_df.empty
                else pd.DataFrame()
            )
            repeat_customer_count = (
                repeat_scope_df["customer_id"].nunique()
                if not repeat_scope_df.empty and "customer_id" in repeat_scope_df.columns
                else 0
            )
            avg_repeat_sales = (
                repeat_scope_df["sales_amount"].sum() / repeat_customer_count
                if repeat_customer_count
                else float("nan")
            )
            avg_repeat_profit = (
                repeat_scope_df[profit_column].sum() / repeat_customer_count
                if profit_column and repeat_customer_count
                else float("nan")
            )

            st.subheader("施策効果の簡易比較")
            with st.form("ab_test"):
                before_rate = st.number_input("施策前リピート率(%)", min_value=0.0, max_value=100.0, value=60.0, step=1.0)
                after_rate = st.number_input("施策後リピート率(%)", min_value=0.0, max_value=100.0, value=68.0, step=1.0)
                before_count = st.number_input("施策前顧客数", min_value=1, value=100)
                after_count = st.number_input("施策後顧客数", min_value=1, value=100)
                submitted = st.form_submit_button("改善効果を計算")
                if submitted:
                    improvement = after_rate - before_rate
                    st.write(f"リピート率改善幅: {improvement:.1f}ポイント")
                    lift = (after_rate / before_rate - 1) if before_rate else np.nan
                    st.write(f"相対改善率: {lift:.2%}" if before_rate else "施策前のリピート率が0のため計算できません。")

                    before_repeat_customers = before_count * (before_rate / 100.0)
                    after_repeat_customers = after_count * (after_rate / 100.0)
                    customer_delta = after_repeat_customers - before_repeat_customers

                    revenue_uplift = (
                        customer_delta * avg_repeat_sales
                        if np.isfinite(avg_repeat_sales)
                        else float("nan")
                    )
                    profit_uplift = (
                        customer_delta * avg_repeat_profit
                        if np.isfinite(avg_repeat_profit)
                        else float("nan")
                    )
                    uplift_cols = st.columns(2)
                    uplift_cols[0].metric(
                        "想定売上増加額",
                        f"{revenue_uplift:,.0f} 円" if np.isfinite(revenue_uplift) else "算出不可",
                    )
                    uplift_cols[1].metric(
                        "想定粗利増加額",
                        f"{profit_uplift:,.0f} 円" if np.isfinite(profit_uplift) else "算出不可",
                    )

            if np.isfinite(avg_repeat_sales):
                profit_note = (
                    f"、平均リピート粗利 {avg_repeat_profit:,.0f} 円"
                    if np.isfinite(avg_repeat_profit)
                    else ""
                )
                st.caption(
                    f"リピート顧客1人あたりの平均売上 {avg_repeat_sales:,.0f} 円{profit_note} を基準に試算しています。"
                )
            else:
                st.caption("リピート顧客の平均売上を算出できなかったため、金額の試算は参考値です。")

    elif selected_section == "データアップロード/管理":
        st.subheader("データアップロード/管理")
        st.markdown(
            """
            - サイドバーから各チャネルのExcel/CSVファイルをアップロードしてください。
            - データはローカルセッション内でのみ保持され、アプリ終了時に消去されます。
            - 列名が異なる場合でも代表的な項目は自動マッピングされます。
            """
        )

        render_business_plan_wizard(merged_full)
        st.markdown("---")

        if sales_validation:
            st.markdown("### 読み込みバリデーション結果")
            for idx, message in enumerate(sales_validation.messages):
                display_text = message.message
                if message.count is not None:
                    display_text += f" (対象: {message.count:,}件)"
                if message.level == "error":
                    st.error(display_text)
                else:
                    st.warning(display_text)
                if message.sample is not None and not message.sample.empty:
                    with st.expander(f"該当レコードの例 ({idx + 1})"):
                        st.dataframe(message.sample)
            if not sales_validation.duplicate_rows.empty:
                st.warning("重複している可能性があるレコード一覧 (先頭200件)")
                st.dataframe(sales_validation.duplicate_rows.head(200))
        else:
            st.success("データ読み込み時に重大な問題は検出されませんでした。")

        if automated_sales_data:
            status_rows = []
            for channel, df in automated_sales_data.items():
                last_fetch = st.session_state["api_last_fetched"].get(channel)
                report: Optional[ValidationReport] = st.session_state["api_sales_validation"].get(channel)
                if last_fetch:
                    status = "エラー" if report and report.has_errors() else "警告あり" if report and report.has_warnings() else "正常"
                    status_rows.append(
                        {
                            "チャネル": channel,
                            "最終取得": last_fetch.strftime("%Y-%m-%d %H:%M"),
                            "取得件数": len(df) if isinstance(df, pd.DataFrame) else 0,
                            "ステータス": status,
                        }
                    )
            if status_rows:
                st.markdown("### API連携ステータス")
                st.dataframe(pd.DataFrame(status_rows))

        st.write("現在のデータ件数")
        summary_cols = st.columns(3)
        summary_cols[0].metric("売上明細件数", len(merged_full))
        summary_cols[1].metric("取り扱い商品数", merged_full["product_code"].nunique())
        summary_cols[2].metric("期間", f"{min_date} 〜 {max_date}")

        with st.expander("原価率データのプレビュー"):
            if cost_df.empty:
                st.info("原価率データが未設定です。")
            else:
                st.dataframe(cost_df)

        with st.expander("売上データのプレビュー"):
            st.dataframe(merged_full.head(100))

        st.markdown("テンプレート/サンプルデータのダウンロード")
        download_button_from_df("サンプル売上データ", generate_sample_sales_data().head(200), "sample_sales.csv")
        download_button_from_df("サンプル原価率データ", generate_sample_cost_data(), "sample_cost.csv")
        download_button_from_df("サンプルKPIデータ", generate_sample_subscription_data(), "sample_kpi.csv")

        st.markdown("---")
        st.markdown("アプリの使い方や改善要望があれば開発チームまでご連絡ください。")


if __name__ == "__main__":
    main()
