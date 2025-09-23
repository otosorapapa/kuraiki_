"""Streamlit dashboard for くらしいきいき社の計数管理アプリ."""
from __future__ import annotations

# TODO: Streamlit UIコンポーネントを使ってダッシュボードを構築
import io
from datetime import date, datetime
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qsl

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

from data_processing import (
    DEFAULT_FIXED_COST,
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
)

st.set_page_config(
    page_title="くらしいきいき社 計数管理ダッシュボード",
    layout="wide",
    page_icon="📊",
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


def apply_filters(sales_df: pd.DataFrame, channels: List[str], date_range: List[date]) -> pd.DataFrame:
    """サイドバーで選択した条件をもとに売上データを抽出する。"""
    if sales_df.empty:
        return sales_df

    filtered = sales_df.copy()
    if channels:
        filtered = filtered[filtered["channel"].isin(channels)]
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
    min_date = sales_df["order_date"].min().date()
    max_date = sales_df["order_date"].max().date()

    selected_channels = st.sidebar.multiselect("表示するチャネル", options=available_channels, default=available_channels)
    date_range = st.sidebar.date_input(
        "表示期間（開始日 / 終了日）",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

    filtered_sales = apply_filters(sales_df, selected_channels, date_range)
    merged_df = merge_sales_and_costs(filtered_sales, cost_df)
    monthly_summary = monthly_sales_summary(merged_df)

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
        if not kpis:
            st.info("KPI情報が不足しています。KPIデータをアップロードするか、サイドバーで数値を入力してください。")
        else:
            metric_cols = st.columns(5)
            metric_cols[0].metric("月間売上高", f"{kpis['sales']:,.0f} 円")
            metric_cols[1].metric("月間粗利", f"{kpis['gross_profit']:,.0f} 円")
            metric_cols[2].metric("LTV", f"{kpis['ltv']:,.0f} 円" if kpis.get("ltv") else "-")
            metric_cols[3].metric("ARPU", f"{kpis['arpu']:,.0f} 円" if kpis.get("arpu") else "-")
            if kpis.get("churn_rate") is not None and not np.isnan(kpis["churn_rate"]):
                metric_cols[4].metric("解約率", f"{kpis['churn_rate']*100:.2f}%")
            else:
                metric_cols[4].metric("解約率", "-")

        if not monthly_summary.empty:
            dashboard_summary = monthly_summary.copy()
            dashboard_summary["month"] = dashboard_summary["order_month"].dt.to_timestamp()
            latest_12 = dashboard_summary.tail(12)

            sales_chart_df = latest_12[["month", "sales_amount", "prev_year_sales"]].rename(
                columns={"sales_amount": "現状売上", "prev_year_sales": "前年同月売上"}
            )
            sales_chart = px.line(
                sales_chart_df.melt(id_vars="month", var_name="指標", value_name="金額"),
                x="month",
                y="金額",
                color="指標",
                markers=True,
            )
            sales_chart.update_layout(yaxis_title="円", xaxis_title="月")
            st.plotly_chart(sales_chart, use_container_width=True)

            gross_chart_df = latest_12[["month", "net_gross_profit"]].rename(columns={"net_gross_profit": "粗利"})
            gross_chart = px.line(gross_chart_df, x="month", y="粗利", markers=True)
            gross_chart.update_layout(yaxis_title="円", xaxis_title="月")
            st.plotly_chart(gross_chart, use_container_width=True)

        chart_cols = st.columns(2)
        if not channel_share_df.empty:
            channel_chart = px.pie(channel_share_df, names="channel", values="sales_amount", title="チャネル別売上構成比")
            chart_cols[0].plotly_chart(channel_chart, use_container_width=True)
        if not category_share_df.empty:
            category_chart = px.pie(category_share_df, names="category", values="sales_amount", title="カテゴリ別売上構成比")
            chart_cols[1].plotly_chart(category_chart, use_container_width=True)

        if not monthly_summary.empty:
            yoy_cols = st.columns(2)
            latest = monthly_summary.iloc[-1]
            yoy_cols[0].metric("前年同月比", f"{latest['sales_yoy']*100:.2f}%" if not np.isnan(latest["sales_yoy"]) else "-")
            yoy_cols[1].metric("前月比", f"{latest['sales_mom']*100:.2f}%" if not np.isnan(latest["sales_mom"]) else "-")

    with tabs[1]:
        st.subheader("売上分析")
        if merged_df.empty:
            st.info("売上データがありません。")
        else:
            st.write("チャネル別売上推移")
            channel_trend = (
                merged_df.groupby(["order_month", "channel"])["sales_amount"].sum().reset_index()
            )
            channel_trend["month"] = channel_trend["order_month"].dt.to_timestamp()
            channel_chart = px.line(
                channel_trend,
                x="month",
                y="sales_amount",
                color="channel",
                markers=True,
                labels={"sales_amount": "売上高", "month": "月"},
            )
            st.plotly_chart(channel_chart, use_container_width=True)

            st.write("商品カテゴリ別売上構成と成長率")
            category_sales = (
                merged_df.groupby(["order_month", "category"])["sales_amount"].sum().reset_index()
            )
            category_sales["month"] = category_sales["order_month"].dt.to_timestamp()
            category_bar = px.bar(
                category_sales,
                x="month",
                y="sales_amount",
                color="category",
                title="カテゴリ別売上推移",
            )
            st.plotly_chart(category_bar, use_container_width=True)

            yoy_table = monthly_summary.tail(12)[["order_month", "sales_amount", "sales_yoy", "sales_mom"]]
            yoy_table = yoy_table.rename(
                columns={"order_month": "月", "sales_amount": "売上高", "sales_yoy": "前年同月比", "sales_mom": "前月比"}
            )
            st.dataframe(yoy_table)

    with tabs[2]:
        st.subheader("利益分析")
        if merged_df.empty:
            st.info("データがありません。")
        else:
            product_profit = (
                merged_df.groupby(["product_code", "product_name", "category"])[
                    ["sales_amount", "estimated_cost", "net_gross_profit"]
                ]
                .sum()
                .reset_index()
            )
            product_profit["粗利率"] = product_profit["net_gross_profit"] / product_profit["sales_amount"]
            product_profit = product_profit.sort_values("net_gross_profit", ascending=False)
            st.dataframe(
                product_profit.rename(
                    columns={
                        "sales_amount": "売上高",
                        "estimated_cost": "推計原価",
                        "net_gross_profit": "粗利",
                    }
                ),
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
            st.plotly_chart(channel_profit_chart, use_container_width=True)

            top_products = product_profit.head(10)
            st.subheader("高利益商材トップ10")
            st.table(
                top_products[["product_code", "product_name", "category", "sales_amount", "net_gross_profit", "粗利率"]]
            )

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
        if merged_df.empty:
            st.info("データがありません。")
        else:
            history = []
            for month in monthly_summary["order_month"]:
                kpi_month = calculate_kpis(merged_df, subscription_df, month=month, overrides=kpi_overrides)
                if kpi_month:
                    history.append(kpi_month)
            kpi_history = pd.DataFrame(history)

            if kpi_history.empty:
                st.info("KPI履歴がありません。")
            else:
                kpi_history["month"] = kpi_history["month"].astype(str)
                kpi_charts = st.tabs(["LTV", "CAC", "リピート率", "チャーン率", "ROAS"])

                with kpi_charts[0]:
                    fig = px.line(kpi_history, x="month", y="ltv", markers=True, title="LTV推移")
                    st.plotly_chart(fig, use_container_width=True)
                with kpi_charts[1]:
                    fig = px.line(kpi_history, x="month", y="cac", markers=True, title="CAC推移")
                    st.plotly_chart(fig, use_container_width=True)
                with kpi_charts[2]:
                    fig = px.bar(kpi_history, x="month", y="repeat_rate", title="リピート率推移")
                    st.plotly_chart(fig, use_container_width=True)
                with kpi_charts[3]:
                    fig = px.bar(kpi_history, x="month", y="churn_rate", title="チャーン率推移")
                    st.plotly_chart(fig, use_container_width=True)
                with kpi_charts[4]:
                    fig = px.line(kpi_history, x="month", y="roas", markers=True, title="ROAS推移")
                    st.plotly_chart(fig, use_container_width=True)

                st.dataframe(
                    kpi_history[
                        [
                            "month",
                            "sales",
                            "gross_profit",
                            "ltv",
                            "arpu",
                            "repeat_rate",
                            "churn_rate",
                            "roas",
                            "cac",
                        ]
                    ]
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
