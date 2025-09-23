"""Streamlit dashboard for くらしいきいき社の計数管理アプリ."""
from __future__ import annotations

# TODO: Streamlit UIコンポーネントを使ってダッシュボードを構築
import io
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl

import numpy as np
import pandas as pd
import plotly.express as px
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
    page_icon="📊",
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
) -> pd.DataFrame:
    """サイドバーで選択した条件をもとに売上データを抽出する。"""
    if sales_df.empty:
        return sales_df

    filtered = sales_df.copy()
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
    st.download_button(label, buffer.getvalue(), file_name=filename, mime="text/csv")


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
        "ltv_prev",
        "ltv_delta",
        "arpu_prev",
        "arpu_delta",
        "churn_prev",
        "churn_delta",
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

    return aggregated[columns]


def main() -> None:
    st.title("📊 くらしいきいき社 計数管理ダッシュボード")
    st.caption("高粗利商材のパフォーマンスを即座に把握し、迅速な意思決定を支援します。")

    st.sidebar.header("データ設定")
    use_sample_data = st.sidebar.checkbox("サンプルデータを使用", value=True)

    channel_files: Dict[str, List] = {}
    for channel in ["自社サイト", "楽天市場", "Amazon", "Yahoo!ショッピング"]:
        channel_files[channel] = st.sidebar.file_uploader(
            f"{channel} 売上データ", type=["xlsx", "xls", "csv"], accept_multiple_files=True
        )

    cost_file = st.sidebar.file_uploader("商品原価率一覧", type=["xlsx", "xls", "csv"], accept_multiple_files=False)
    subscription_file = st.sidebar.file_uploader(
        "定期購買/KPIデータ", type=["xlsx", "xls", "csv"], accept_multiple_files=False
    )

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
                    status_icon = "❌"
                elif status_report and status_report.has_warnings():
                    status_icon = "⚠️"
                else:
                    status_icon = "✅"
                st.caption(
                    f"{status_icon} 最終取得: {last_fetch.strftime('%Y-%m-%d %H:%M')} / {record_count:,} 件"
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

    automated_sales_data = st.session_state.get("api_sales_data", {})
    automated_reports = list(st.session_state.get("api_sales_validation", {}).values())

    data_dict = load_data(
        use_sample_data,
        channel_files,
        cost_file,
        subscription_file,
        automated_sales=automated_sales_data,
        automated_reports=automated_reports,
    )
    sales_df = data_dict["sales"].copy()
    cost_df = data_dict["cost"].copy()
    subscription_df = data_dict["subscription"].copy()
    sales_validation: ValidationReport = data_dict.get("sales_validation", ValidationReport())

    if sales_df.empty:
        st.warning("売上データが読み込めませんでした。サンプルデータを利用するか、ファイルをアップロードしてください。")
        return

    merged_full = merge_sales_and_costs(sales_df, cost_df)
    sales_validation.extend(validate_channel_fees(merged_full))

    available_channels = sorted(sales_df["channel"].unique())
    available_categories = sorted(sales_df["category"].dropna().unique())
    min_date = sales_df["order_date"].min().date()
    max_date = sales_df["order_date"].max().date()

    selected_channels = st.sidebar.multiselect(
        "表示するチャネル", options=available_channels, default=available_channels
    )
    selected_categories = st.sidebar.multiselect(
        "表示するカテゴリ",
        options=available_categories,
        default=available_categories if available_categories else None,
    )
    freq_lookup = {label: freq for label, freq in PERIOD_FREQ_OPTIONS}
    default_freq_index = next(
        (idx for idx, (_, freq) in enumerate(PERIOD_FREQ_OPTIONS) if freq == "M"),
        0,
    )
    selected_granularity_label = st.sidebar.selectbox(
        "ダッシュボード表示粒度",
        options=[label for label, _ in PERIOD_FREQ_OPTIONS],
        index=default_freq_index,
    )
    selected_freq = freq_lookup[selected_granularity_label]

    date_range = st.sidebar.date_input(
        "表示期間（開始日 / 終了日）",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

    filtered_sales = apply_filters(sales_df, selected_channels, date_range, selected_categories)
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

    kpis = calculate_kpis(merged_df, subscription_df, overrides=kpi_overrides)
    kpi_history_df = build_kpi_history_df(merged_df, subscription_df, kpi_overrides)
    kpi_period_summary = aggregate_kpi_history(kpi_history_df, selected_freq)

    base_pl = create_current_pl(merged_df, subscription_df, fixed_cost=fixed_cost)
    default_cash_plan = create_default_cashflow_plan(merged_df)
    default_cash_forecast = forecast_cashflow(default_cash_plan, starting_cash)

    alerts = build_alerts(monthly_summary, kpis, default_cash_forecast)
    if alerts:
        for msg in alerts:
            st.error(f"⚠️ {msg}")
    else:
        st.success("主要指標は設定した閾値内に収まっています。")

    channel_share_df = compute_channel_share(merged_df)
    category_share_df = compute_category_share(merged_df)

    tabs = st.tabs([
        "ダッシュボード",
        "売上分析",
        "利益分析",
        "財務モニタリング",
        "KPIモニタリング",
        "データアップロード/管理",
    ])

    with tabs[0]:
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

            sales_delta_text = None
            gross_delta_text = None
            if not period_row.empty:
                sales_mom_val = period_row["sales_mom"].iloc[0]
                gross_mom_val = period_row["gross_mom"].iloc[0]
                if pd.notna(sales_mom_val):
                    sales_delta_text = f"{sales_mom_val * 100:.2f}%"
                if pd.notna(gross_mom_val):
                    gross_delta_text = f"{gross_mom_val * 100:.2f}%"

            ltv_delta_val = selected_kpi_row.get("ltv_delta")
            ltv_delta_text = (
                f"{ltv_delta_val:,.0f} 円" if pd.notna(ltv_delta_val) and ltv_delta_val != 0 else None
            )
            arpu_delta_val = selected_kpi_row.get("arpu_delta")
            arpu_delta_text = (
                f"{arpu_delta_val:,.0f} 円" if pd.notna(arpu_delta_val) and arpu_delta_val != 0 else None
            )
            churn_delta_val = selected_kpi_row.get("churn_delta")
            churn_delta_text = (
                f"{churn_delta_val * 100:.2f} pt"
                if pd.notna(churn_delta_val) and churn_delta_val != 0
                else None
            )

            metric_cols = st.columns(5)
            metric_cols[0].metric(
                f"{selected_granularity_label}売上高",
                f"{selected_kpi_row['sales']:,.0f} 円" if pd.notna(selected_kpi_row["sales"]) else "-",
                delta=sales_delta_text,
            )
            metric_cols[1].metric(
                f"{selected_granularity_label}粗利",
                f"{selected_kpi_row['gross_profit']:,.0f} 円"
                if pd.notna(selected_kpi_row["gross_profit"])
                else "-",
                delta=gross_delta_text,
            )
            metric_cols[2].metric(
                "LTV",
                f"{selected_kpi_row['ltv']:,.0f} 円" if pd.notna(selected_kpi_row["ltv"]) else "-",
                delta=ltv_delta_text,
            )
            metric_cols[3].metric(
                "ARPU",
                f"{selected_kpi_row['arpu']:,.0f} 円" if pd.notna(selected_kpi_row["arpu"]) else "-",
                delta=arpu_delta_text,
            )
            churn_value = selected_kpi_row.get("churn_rate")
            metric_cols[4].metric(
                "解約率",
                f"{churn_value * 100:.2f}%" if pd.notna(churn_value) else "-",
                delta=churn_delta_text,
            )

            st.caption(f"対象期間: {period_start} 〜 {period_end}")

        if not period_summary.empty:
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
            sales_value_columns = [
                col for col in ["現状売上", "前年同期間売上"] if col in sales_chart_source.columns
            ]
            if sales_value_columns:
                sales_chart = px.line(
                    sales_chart_source.melt(
                        id_vars=["期間開始", "期間"],
                        value_vars=sales_value_columns,
                        var_name="指標",
                        value_name="金額",
                    ),
                    x="期間開始",
                    y="金額",
                    color="指標",
                    markers=True,
                    hover_data={"期間": True},
                )
                sales_chart.update_layout(
                    yaxis_title="円",
                    xaxis_title=f"{selected_granularity_label}開始日",
                    legend=dict(title="", itemclick="toggleothers", itemdoubleclick="toggle"),
                )
                st.plotly_chart(sales_chart, use_container_width=True)

            gross_chart_source = latest_periods.rename(
                columns={
                    "period_start": "期間開始",
                    "period_label": "期間",
                    "net_gross_profit": "粗利",
                }
            )
            gross_chart = px.line(
                gross_chart_source,
                x="期間開始",
                y="粗利",
                markers=True,
                hover_data={"期間": True},
            )
            gross_chart.update_layout(
                yaxis_title="円",
                xaxis_title=f"{selected_granularity_label}開始日",
                legend=dict(title=""),
            )
            st.plotly_chart(gross_chart, use_container_width=True)

        chart_cols = st.columns(2)
        if not channel_share_df.empty:
            channel_chart = px.pie(
                channel_share_df,
                names="channel",
                values="sales_amount",
                title="チャネル別売上構成比",
            )
            chart_cols[0].plotly_chart(channel_chart, use_container_width=True)
        if not category_share_df.empty:
            category_chart = px.pie(
                category_share_df,
                names="category",
                values="sales_amount",
                title="カテゴリ別売上構成比",
            )
            chart_cols[1].plotly_chart(category_chart, use_container_width=True)

        if not period_summary.empty:
            yoy_cols = st.columns(2)
            latest_period_row = period_summary.iloc[-1]
            yoy_cols[0].metric(
                "前年同期比",
                f"{latest_period_row['sales_yoy'] * 100:.2f}%"
                if pd.notna(latest_period_row["sales_yoy"])
                else "-",
            )
            yoy_cols[1].metric(
                "前期比",
                f"{latest_period_row['sales_mom'] * 100:.2f}%"
                if pd.notna(latest_period_row["sales_mom"])
                else "-",
            )

    with tabs[1]:
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
            )
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
            )
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

    with tabs[2]:
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
            )
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
            )
            highlight_code = st.session_state.get("profit_focus_product")
            bar_colors = [
                "#d62728" if code == highlight_code else "#1f77b4"
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
                    )
                    breakdown_chart.update_layout(legend=dict(title=""))
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
                    )
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

    with tabs[3]:
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
            )
            cash_chart.update_layout(yaxis_title="円", xaxis_title="月")
            st.plotly_chart(cash_chart, use_container_width=True)
            st.dataframe(cash_forecast)
        else:
            st.info("キャッシュフロープランが未設定です。")

    with tabs[4]:
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
                )
                st.plotly_chart(fig, use_container_width=True)
            with kpi_charts[1]:
                fig = px.line(
                    kpi_history_display,
                    x="month_str",
                    y="cac",
                    markers=True,
                    title="CAC推移",
                )
                st.plotly_chart(fig, use_container_width=True)
            with kpi_charts[2]:
                fig = px.bar(
                    kpi_history_display,
                    x="month_str",
                    y="repeat_rate",
                    title="リピート率推移",
                )
                st.plotly_chart(fig, use_container_width=True)
            with kpi_charts[3]:
                fig = px.bar(
                    kpi_history_display,
                    x="month_str",
                    y="churn_rate",
                    title="チャーン率推移",
                )
                st.plotly_chart(fig, use_container_width=True)
            with kpi_charts[4]:
                fig = px.line(
                    kpi_history_display,
                    x="month_str",
                    y="roas",
                    markers=True,
                    title="ROAS推移",
                )
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
                        )
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

    with tabs[5]:
        st.subheader("データアップロード/管理")
        st.markdown(
            """
            - サイドバーから各チャネルのExcel/CSVファイルをアップロードしてください。
            - データはローカルセッション内でのみ保持され、アプリ終了時に消去されます。
            - 列名が異なる場合でも代表的な項目は自動マッピングされます。
            """
        )

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
