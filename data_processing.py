"""Utility functions for the くらしいきいき社向け計数管理Webアプリ."""
from __future__ import annotations

# TODO: pandasとnumpyを使ってデータ集計を行う
import math
from typing import Dict, Iterable, List, Optional

import numpy as np
import pandas as pd

# 共通で利用する列名の定義
NORMALIZED_SALES_COLUMNS = [
    "order_date",
    "channel",
    "product_code",
    "product_name",
    "category",
    "quantity",
    "sales_amount",
    "customer_id",
]

SALES_COLUMN_ALIASES: Dict[str, List[str]] = {
    "order_date": ["注文日", "注文日時", "Date", "order_date", "注文年月日"],
    "channel": ["チャネル", "販売チャネル", "channel", "モール"],
    "product_code": ["商品コード", "SKU", "品番", "product_code", "商品番号"],
    "product_name": ["商品名", "品名", "product", "product_name"],
    "category": ["カテゴリ", "カテゴリー", "category", "商品カテゴリ"],
    "quantity": ["数量", "個数", "quantity", "qty"],
    "sales_amount": ["売上", "売上高", "金額", "sales", "sales_amount", "合計金額"],
    "customer_id": ["顧客ID", "customer_id", "会員ID", "購入者ID"],
}

COST_COLUMN_ALIASES: Dict[str, List[str]] = {
    "product_code": ["商品コード", "product_code", "SKU", "品番"],
    "product_name": ["商品名", "品名", "product_name", "品目名"],
    "category": ["カテゴリ", "カテゴリー", "category"],
    "price": ["売価", "単価", "price", "販売価格"],
    "cost": ["原価", "仕入原価", "cost"],
    "cost_rate": ["原価率", "cost_rate"],
}

SUBSCRIPTION_COLUMN_ALIASES: Dict[str, List[str]] = {
    "month": ["month", "年月", "月", "date", "対象月"],
    "active_customers": ["active_customers", "アクティブ顧客数", "継続会員数", "契約数", "有効会員数"],
    "new_customers": ["new_customers", "新規顧客数", "新規獲得数"],
    "repeat_customers": ["repeat_customers", "リピート顧客数", "継続購入者数"],
    "cancelled_subscriptions": ["cancelled_subscriptions", "解約件数", "解約数", "キャンセル数"],
    "previous_active_customers": ["previous_active_customers", "前月契約数", "前月アクティブ顧客"],
    "marketing_cost": ["marketing_cost", "広告費", "販促費", "marketing"],
    "ltv": ["ltv", "LTV", "顧客生涯価値"],
    "total_sales": ["total_sales", "売上高", "sales", "売上"],
}

DEFAULT_CHANNEL_FEE_RATES: Dict[str, float] = {
    "自社サイト": 0.03,
    "楽天市場": 0.12,
    "Amazon": 0.15,
    "Yahoo!ショッピング": 0.10,
}

DEFAULT_FIXED_COST = 2_500_000  # 人件費や管理費などの固定費（目安）
DEFAULT_LOAN_REPAYMENT = 600_000  # 月次の借入返済額の仮値


def detect_channel_from_filename(filename: Optional[str]) -> Optional[str]:
    """ファイル名からチャネル名を推定する。"""
    if not filename:
        return None
    name = filename.lower()
    if "rakuten" in name or "楽天" in name:
        return "楽天市場"
    if "amazon" in name:
        return "Amazon"
    if "yahoo" in name:
        return "Yahoo!ショッピング"
    if "shop" in name or "ec" in name or "自社" in name:
        return "自社サイト"
    return None


def _build_rename_map(columns: Iterable[str], alias_config: Dict[str, List[str]]) -> Dict[str, str]:
    """指定した列から正規化用のrename辞書を作成する。"""
    rename_map: Dict[str, str] = {}
    normalized_cols = [col.lower() for col in columns]
    for canonical, aliases in alias_config.items():
        for alias in aliases:
            if alias.lower() in normalized_cols:
                idx = normalized_cols.index(alias.lower())
                rename_map[list(columns)[idx]] = canonical
                break
    return rename_map


def normalize_sales_df(df: pd.DataFrame, channel: Optional[str] = None) -> pd.DataFrame:
    """売上データを統一フォーマットに整形する。"""
    if df is None or df.empty:
        return pd.DataFrame(columns=NORMALIZED_SALES_COLUMNS)

    rename_map = _build_rename_map(df.columns, SALES_COLUMN_ALIASES)
    normalized = df.rename(columns=rename_map).copy()

    for column in NORMALIZED_SALES_COLUMNS:
        if column not in normalized.columns:
            if column == "channel" and channel:
                normalized[column] = channel
            elif column == "quantity":
                normalized[column] = 1
            else:
                normalized[column] = np.nan

    normalized = normalized[NORMALIZED_SALES_COLUMNS]
    normalized["order_date"] = pd.to_datetime(normalized["order_date"], errors="coerce")
    normalized["channel"] = normalized["channel"].fillna(channel or "不明").astype(str)
    normalized["product_code"] = normalized["product_code"].fillna("NA").astype(str)
    normalized["product_name"] = normalized["product_name"].fillna("不明商品").astype(str)
    normalized["category"] = normalized["category"].fillna("未分類").astype(str)
    normalized["quantity"] = pd.to_numeric(normalized["quantity"], errors="coerce").fillna(1)
    normalized["sales_amount"] = pd.to_numeric(normalized["sales_amount"], errors="coerce").fillna(0.0)
    normalized["customer_id"] = normalized["customer_id"].fillna("anonymous").astype(str)
    normalized = normalized.dropna(subset=["order_date"])

    # 単価を推計しておく（quantityが0の場合は回避）
    normalized["unit_price"] = normalized.apply(
        lambda row: row["sales_amount"] / row["quantity"] if row["quantity"] else row["sales_amount"],
        axis=1,
    )
    normalized["order_month"] = normalized["order_date"].dt.to_period("M")
    return normalized


def load_sales_workbook(uploaded_file, channel_hint: Optional[str] = None) -> pd.DataFrame:
    """アップロードされたExcel/CSVを読み込み正規化する。"""
    if uploaded_file is None:
        return pd.DataFrame(columns=NORMALIZED_SALES_COLUMNS)

    # TODO: pandas.read_excelでファイル内容を読み込む
    try:
        df = pd.read_excel(uploaded_file)
    except ValueError:
        uploaded_file.seek(0)
        df = pd.read_csv(uploaded_file)

    detected = channel_hint or detect_channel_from_filename(getattr(uploaded_file, "name", None))
    return normalize_sales_df(df, channel=detected)


def load_sales_files(files_by_channel: Dict[str, List]) -> pd.DataFrame:
    """チャネルごとのファイル群を統合した売上データを作成する。"""
    frames: List[pd.DataFrame] = []
    for channel, files in files_by_channel.items():
        if not files:
            continue
        for uploaded in files:
            normalized = load_sales_workbook(uploaded, channel_hint=channel)
            if not normalized.empty:
                frames.append(normalized)
    if not frames:
        return pd.DataFrame(columns=NORMALIZED_SALES_COLUMNS)
    combined = pd.concat(frames, ignore_index=True)
    combined.sort_values("order_date", inplace=True)
    return combined


def load_cost_workbook(uploaded_file) -> pd.DataFrame:
    """原価率表を読み込む。"""
    if uploaded_file is None:
        return pd.DataFrame(columns=["product_code", "product_name", "category", "price", "cost", "cost_rate"])

    try:
        df = pd.read_excel(uploaded_file)
    except ValueError:
        uploaded_file.seek(0)
        df = pd.read_csv(uploaded_file)

    rename_map = _build_rename_map(df.columns, COST_COLUMN_ALIASES)
    normalized = df.rename(columns=rename_map)

    for column in ["product_code", "product_name", "category", "price", "cost", "cost_rate"]:
        if column not in normalized.columns:
            normalized[column] = np.nan

    normalized["product_code"] = normalized["product_code"].fillna("NA").astype(str)
    normalized["product_name"] = normalized["product_name"].fillna("不明商品").astype(str)
    normalized["category"] = normalized["category"].fillna("未分類").astype(str)
    normalized["price"] = pd.to_numeric(normalized["price"], errors="coerce")
    normalized["cost"] = pd.to_numeric(normalized["cost"], errors="coerce")
    normalized["cost_rate"] = pd.to_numeric(normalized["cost_rate"], errors="coerce")

    if normalized["cost_rate"].isna().any():
        normalized.loc[:, "cost_rate"] = normalized.apply(
            lambda row: row["cost"] / row["price"] if row["price"] else np.nan,
            axis=1,
        )
    normalized["gross_margin_rate"] = 1 - normalized["cost_rate"].fillna(0)
    return normalized


def load_subscription_workbook(uploaded_file) -> pd.DataFrame:
    """サブスク/KPIデータを読み込む。"""
    if uploaded_file is None:
        return pd.DataFrame(
            columns=[
                "month",
                "active_customers",
                "previous_active_customers",
                "new_customers",
                "repeat_customers",
                "cancelled_subscriptions",
                "marketing_cost",
                "ltv",
                "total_sales",
            ]
        )

    try:
        df = pd.read_excel(uploaded_file)
    except ValueError:
        uploaded_file.seek(0)
        df = pd.read_csv(uploaded_file)

    rename_map = _build_rename_map(df.columns, SUBSCRIPTION_COLUMN_ALIASES)
    normalized = df.rename(columns=rename_map).copy()

    for column in [
        "month",
        "active_customers",
        "previous_active_customers",
        "new_customers",
        "repeat_customers",
        "cancelled_subscriptions",
        "marketing_cost",
        "ltv",
        "total_sales",
    ]:
        if column not in normalized.columns:
            normalized[column] = np.nan

    normalized["month"] = pd.to_datetime(normalized["month"], errors="coerce").dt.to_period("M")
    numeric_cols = [
        "active_customers",
        "previous_active_customers",
        "new_customers",
        "repeat_customers",
        "cancelled_subscriptions",
        "marketing_cost",
        "ltv",
        "total_sales",
    ]
    for col in numeric_cols:
        normalized[col] = pd.to_numeric(normalized[col], errors="coerce")

    return normalized


def merge_sales_and_costs(sales_df: pd.DataFrame, cost_df: pd.DataFrame) -> pd.DataFrame:
    """売上データに原価情報を結合し、粗利を計算する。"""
    if sales_df.empty:
        return sales_df

    merged = sales_df.copy()
    if not cost_df.empty:
        join_keys = ["product_code"] if "product_code" in cost_df.columns else []
        if not join_keys or cost_df["product_code"].eq("NA").all():
            join_keys = ["product_name"]
        merged = merged.merge(cost_df, on=join_keys, how="left", suffixes=("", "_cost"))
    else:
        merged = merged.assign(price=np.nan, cost=np.nan, cost_rate=np.nan, gross_margin_rate=np.nan)

    merged["cost_rate"] = merged["cost_rate"].fillna(0.3)
    merged["cost_rate"] = merged["cost_rate"].clip(0, 0.95)
    if "gross_margin_rate" in merged.columns:
        merged["gross_margin_rate"] = merged["gross_margin_rate"].fillna(1 - merged["cost_rate"])
    else:
        merged["gross_margin_rate"] = 1 - merged["cost_rate"]
    merged["estimated_cost"] = merged["sales_amount"] * merged["cost_rate"]
    merged["gross_profit"] = merged["sales_amount"] - merged["estimated_cost"]
    merged["channel_fee"] = merged["channel"].map(DEFAULT_CHANNEL_FEE_RATES).fillna(0)
    merged["channel_fee_amount"] = merged["sales_amount"] * merged["channel_fee"]
    merged["net_gross_profit"] = merged["gross_profit"] - merged["channel_fee_amount"]
    return merged


def aggregate_sales(df: pd.DataFrame, group_fields: List[str]) -> pd.DataFrame:
    """汎用的な売上集計処理。"""
    if df.empty:
        return pd.DataFrame(columns=group_fields + ["sales_amount"])
    aggregated = df.groupby(group_fields)["sales_amount"].sum().reset_index()
    return aggregated


def monthly_sales_summary(df: pd.DataFrame) -> pd.DataFrame:
    """月次の売上と粗利サマリを返す。"""
    if df.empty:
        return pd.DataFrame(columns=["order_month", "sales_amount", "gross_profit", "net_gross_profit"])
    summary = (
        df.groupby("order_month")[
            ["sales_amount", "gross_profit", "net_gross_profit"]
        ]
        .sum()
        .reset_index()
        .sort_values("order_month")
    )
    summary["prev_year_sales"] = summary["sales_amount"].shift(12)
    summary["prev_month_sales"] = summary["sales_amount"].shift(1)
    summary["sales_yoy"] = np.where(
        (summary["prev_year_sales"].notna()) & (summary["prev_year_sales"] != 0),
        (summary["sales_amount"] - summary["prev_year_sales"]) / summary["prev_year_sales"],
        np.nan,
    )
    summary["sales_mom"] = np.where(
        (summary["prev_month_sales"].notna()) & (summary["prev_month_sales"] != 0),
        (summary["sales_amount"] - summary["prev_month_sales"]) / summary["prev_month_sales"],
        np.nan,
    )
    return summary


def compute_channel_share(df: pd.DataFrame) -> pd.DataFrame:
    """チャネル別売上構成比。"""
    if df.empty:
        return pd.DataFrame(columns=["channel", "sales_amount"])
    return (
        df.groupby("channel")["sales_amount"].sum().reset_index().sort_values("sales_amount", ascending=False)
    )


def compute_category_share(df: pd.DataFrame) -> pd.DataFrame:
    """カテゴリ別売上構成比。"""
    if df.empty:
        return pd.DataFrame(columns=["category", "sales_amount"])
    return (
        df.groupby("category")["sales_amount"].sum().reset_index().sort_values("sales_amount", ascending=False)
    )


def calculate_kpis(
    merged_df: pd.DataFrame,
    subscription_df: Optional[pd.DataFrame],
    month: Optional[pd.Period] = None,
    overrides: Optional[Dict[str, float]] = None,
) -> Dict[str, Optional[float]]:
    """主要KPIを計算する。"""
    overrides = overrides or {}
    if merged_df.empty:
        return {}

    if month is None:
        month = merged_df["order_month"].max()

    monthly_df = merged_df[merged_df["order_month"] == month]
    monthly_sales = float(monthly_df["sales_amount"].sum())
    monthly_gross_profit = float(monthly_df["net_gross_profit"].sum())

    sub_row = None
    if subscription_df is not None and not subscription_df.empty:
        subscription_df = subscription_df.copy()
        if "month" in subscription_df.columns:
            subscription_df["month"] = pd.PeriodIndex(subscription_df["month"], freq="M")
        sub_candidates = subscription_df[subscription_df["month"] == month]
        if not sub_candidates.empty:
            sub_row = sub_candidates.iloc[0]

    active_customers = overrides.get(
        "active_customers", float(sub_row["active_customers"]) if sub_row is not None else np.nan
    )
    new_customers = overrides.get(
        "new_customers", float(sub_row["new_customers"]) if sub_row is not None else np.nan
    )
    repeat_customers = overrides.get(
        "repeat_customers", float(sub_row["repeat_customers"]) if sub_row is not None else np.nan
    )
    cancelled = overrides.get(
        "cancelled_subscriptions", float(sub_row["cancelled_subscriptions"]) if sub_row is not None else np.nan
    )
    prev_active = overrides.get(
        "previous_active_customers",
        float(sub_row.get("previous_active_customers", np.nan)) if sub_row is not None else np.nan,
    )
    marketing_cost = overrides.get(
        "marketing_cost", float(sub_row["marketing_cost"]) if sub_row is not None else np.nan
    )
    ltv_value = overrides.get("ltv", float(sub_row["ltv"]) if sub_row is not None else np.nan)

    arpu = monthly_sales / active_customers if active_customers else np.nan
    repeat_rate = repeat_customers / active_customers if active_customers else np.nan
    churn_rate = cancelled / prev_active if prev_active else np.nan
    roas = monthly_sales / marketing_cost if marketing_cost else np.nan
    adv_ratio = marketing_cost / monthly_sales if monthly_sales else np.nan
    gross_margin_rate = monthly_gross_profit / monthly_sales if monthly_sales else np.nan
    cac = marketing_cost / new_customers if new_customers else np.nan

    return {
        "month": month,
        "sales": monthly_sales,
        "gross_profit": monthly_gross_profit,
        "active_customers": active_customers,
        "new_customers": new_customers,
        "repeat_customers": repeat_customers,
        "cancelled_subscriptions": cancelled,
        "marketing_cost": marketing_cost,
        "ltv": ltv_value,
        "arpu": arpu,
        "repeat_rate": repeat_rate,
        "churn_rate": churn_rate,
        "roas": roas,
        "adv_ratio": adv_ratio,
        "gross_margin_rate": gross_margin_rate,
        "cac": cac,
    }


def generate_sample_sales_data(seed: int = 42) -> pd.DataFrame:
    """分析用のサンプル売上データを生成する。"""
    rng = np.random.default_rng(seed)
    months = pd.period_range("2023-01", periods=24, freq="M")
    channels = ["自社サイト", "楽天市場", "Amazon", "Yahoo!ショッピング"]

    sample_products = [
        {"code": "FKD01", "name": "低分子フコイダンドリンク", "category": "フコイダン", "price": 11800, "cost_rate": 0.24},
        {"code": "FKD02", "name": "まいにちフコイダン粒", "category": "フコイダン", "price": 9800, "cost_rate": 0.26},
        {"code": "APO01", "name": "アポネクスト", "category": "サプリ", "price": 12800, "cost_rate": 0.28},
        {"code": "SMG01", "name": "玄米麹スムージー", "category": "スムージー", "price": 5400, "cost_rate": 0.45},
        {"code": "LAC01", "name": "まいにち乳酸菌", "category": "サプリ", "price": 4800, "cost_rate": 0.35},
        {"code": "BTA01", "name": "美容酢ドリンク", "category": "美容", "price": 4200, "cost_rate": 0.62},
    ]

    records: List[Dict[str, object]] = []
    for month in months:
        seasonality = 1.0 + 0.15 * math.sin((month.month - 1) / 12 * 2 * math.pi)
        for channel in channels:
            channel_bias = {
                "自社サイト": 1.0,
                "楽天市場": 0.6,
                "Amazon": 0.7,
                "Yahoo!ショッピング": 0.4,
            }[channel]
            for product in sample_products:
                demand = rng.normal(loc=120, scale=35)
                demand = max(demand, 20)
                demand *= seasonality * channel_bias
                demand *= 1 + rng.normal(0, 0.08)
                quantity = int(demand / 2)
                sales_amount = quantity * product["price"]
                customer_count = max(1, int(quantity * 0.6))
                for i in range(max(1, customer_count // 3)):
                    records.append(
                        {
                            "order_date": month.to_timestamp("M") - pd.Timedelta(days=rng.integers(0, 27)),
                            "channel": channel,
                            "product_code": product["code"],
                            "product_name": product["name"],
                            "category": product["category"],
                            "quantity": quantity / max(1, customer_count // 3),
                            "sales_amount": sales_amount / max(1, customer_count // 3),
                            "customer_id": f"{channel[:2]}-{month.strftime('%Y%m')}-{rng.integers(1000, 9999)}",
                        }
                    )

    sales_df = pd.DataFrame(records)
    sales_df["order_date"] = pd.to_datetime(sales_df["order_date"])
    sales_df = normalize_sales_df(sales_df)
    # サンプル用のカテゴリ情報が失われないよう補正
    category_map = {p["name"]: p["category"] for p in sample_products}
    mapped_categories = sales_df["product_name"].map(category_map)
    update_mask = sales_df["category"].eq("未分類") & mapped_categories.notna()
    sales_df.loc[update_mask, "category"] = mapped_categories[update_mask]
    return sales_df


def generate_sample_cost_data() -> pd.DataFrame:
    """サンプルの原価率データを作成する。"""
    data = [
        ("FKD01", "低分子フコイダンドリンク", "フコイダン", 11800, 11800 * 0.24, 0.24),
        ("FKD02", "まいにちフコイダン粒", "フコイダン", 9800, 9800 * 0.26, 0.26),
        ("APO01", "アポネクスト", "サプリ", 12800, 12800 * 0.28, 0.28),
        ("SMG01", "玄米麹スムージー", "スムージー", 5400, 5400 * 0.45, 0.45),
        ("LAC01", "まいにち乳酸菌", "サプリ", 4800, 4800 * 0.35, 0.35),
        ("BTA01", "美容酢ドリンク", "美容", 4200, 4200 * 0.62, 0.62),
    ]
    return pd.DataFrame(data, columns=["product_code", "product_name", "category", "price", "cost", "cost_rate"])


def generate_sample_subscription_data() -> pd.DataFrame:
    """サンプルのサブスク/KPIデータを生成する。"""
    months = pd.period_range("2023-01", periods=24, freq="M")
    rng = np.random.default_rng(7)
    rows: List[Dict[str, object]] = []
    active = 2400
    for month in months:
        new_customers = rng.integers(120, 260)
        cancelled = rng.integers(70, 120)
        repeat_customers = int(active * 0.68 + rng.normal(0, 40))
        marketing_cost = 1_500_000 + rng.normal(0, 120_000)
        ltv = 68_000 + rng.normal(0, 3_000)
        total_sales = (active + new_customers - cancelled) * 12_000
        rows.append(
            {
                "month": month,
                "active_customers": active,
                "new_customers": new_customers,
                "repeat_customers": repeat_customers,
                "cancelled_subscriptions": cancelled,
                "previous_active_customers": active,
                "marketing_cost": marketing_cost,
                "ltv": ltv,
                "total_sales": total_sales,
            }
        )
        active = active + new_customers - cancelled
    return pd.DataFrame(rows)


def create_current_pl(merged_df: pd.DataFrame, subscription_df: Optional[pd.DataFrame], fixed_cost: float) -> Dict[str, float]:
    """現在のPLモデルを作成する。"""
    if merged_df.empty:
        return {
            "sales": 0.0,
            "cogs": 0.0,
            "gross_profit": 0.0,
            "sga": fixed_cost,
            "operating_profit": -fixed_cost,
        }
    monthly_summary = monthly_sales_summary(merged_df)
    latest = monthly_summary.iloc[-1]
    marketing_cost = 0.0
    if subscription_df is not None and not subscription_df.empty:
        subscription_df = subscription_df.copy()
        subscription_df["month"] = pd.PeriodIndex(subscription_df["month"], freq="M")
        match = subscription_df[subscription_df["month"] == latest["order_month"]]
        if not match.empty:
            marketing_cost = float(match.iloc[0]["marketing_cost"])
    sga = fixed_cost + marketing_cost
    cogs = latest["sales_amount"] - latest["net_gross_profit"]
    base_pl = {
        "sales": float(latest["sales_amount"]),
        "cogs": float(cogs),
        "gross_profit": float(latest["net_gross_profit"]),
        "sga": float(sga),
    }
    base_pl["operating_profit"] = base_pl["gross_profit"] - base_pl["sga"]
    return base_pl


def simulate_pl(
    base_pl: Dict[str, float],
    sales_growth_rate: float,
    cost_rate_adjustment: float,
    sga_change_rate: float,
    additional_ad_cost: float,
) -> pd.DataFrame:
    """PLシミュレーションを行い結果を返す。"""
    current_sales = base_pl.get("sales", 0.0)
    current_cogs = base_pl.get("cogs", 0.0)
    current_sga = base_pl.get("sga", 0.0)
    current_gross = base_pl.get("gross_profit", current_sales - current_cogs)

    new_sales = current_sales * (1 + sales_growth_rate)
    base_cost_ratio = current_cogs / current_sales if current_sales else 0
    new_cost_ratio = max(0, base_cost_ratio + cost_rate_adjustment)
    new_cogs = new_sales * new_cost_ratio
    new_gross = new_sales - new_cogs
    new_sga = current_sga * (1 + sga_change_rate) + additional_ad_cost
    new_operating_profit = new_gross - new_sga

    result = pd.DataFrame(
        {
            "項目": ["売上高", "売上原価", "粗利", "販管費", "営業利益"],
            "現状": [current_sales, current_cogs, current_gross, current_sga, current_gross - current_sga],
            "シナリオ": [new_sales, new_cogs, new_gross, new_sga, new_operating_profit],
        }
    )
    result["増減"] = result["シナリオ"] - result["現状"]
    return result


def create_default_cashflow_plan(merged_df: pd.DataFrame, horizon_months: int = 6) -> pd.DataFrame:
    """簡易キャッシュフロー予測の初期値を生成する。"""
    if merged_df.empty:
        months = pd.period_range(pd.Timestamp.today(), periods=horizon_months, freq="M")
        return pd.DataFrame(
            {
                "month": months,
                "operating_cf": 0.0,
                "investment_cf": 0.0,
                "financing_cf": 0.0,
                "loan_repayment": DEFAULT_LOAN_REPAYMENT,
            }
        )

    summary = monthly_sales_summary(merged_df)
    recent_gross = summary.tail(6)["net_gross_profit"].mean()
    plan_months = pd.period_range(summary["order_month"].iloc[-1] + 1, periods=horizon_months, freq="M")
    operating_cf = recent_gross * 0.75

    plan_df = pd.DataFrame(
        {
            "month": plan_months,
            "operating_cf": operating_cf,
            "investment_cf": -250_000,
            "financing_cf": 0.0,
            "loan_repayment": DEFAULT_LOAN_REPAYMENT,
        }
    )
    return plan_df


def forecast_cashflow(plan_df: pd.DataFrame, starting_cash: float) -> pd.DataFrame:
    """キャッシュ残高推移を計算する。"""
    if plan_df.empty:
        return pd.DataFrame(columns=["month", "net_cf", "cash_balance"])
    cash = starting_cash
    records: List[Dict[str, object]] = []
    for _, row in plan_df.iterrows():
        operating_cf = float(row.get("operating_cf", 0.0))
        investment_cf = float(row.get("investment_cf", 0.0))
        financing_cf = float(row.get("financing_cf", 0.0))
        loan_repayment = float(row.get("loan_repayment", 0.0))
        net_cf = operating_cf + financing_cf - investment_cf - loan_repayment
        cash += net_cf
        records.append(
            {
                "month": row.get("month"),
                "net_cf": net_cf,
                "cash_balance": cash,
            }
        )
    forecast_df = pd.DataFrame(records)
    return forecast_df


def build_alerts(
    monthly_summary: pd.DataFrame,
    kpi_summary: Dict[str, Optional[float]],
    cashflow_forecast: pd.DataFrame,
    thresholds: Optional[Dict[str, float]] = None,
) -> List[str]:
    """アラート文言のリストを作成する。"""
    thresholds = thresholds or {
        "revenue_drop_pct": 0.3,
        "churn_rate": 0.05,
        "gross_margin_rate": 0.6,
        "cash_balance": 0,
    }
    alerts: List[str] = []

    if monthly_summary is not None and len(monthly_summary) >= 2:
        latest = monthly_summary.iloc[-1]
        prev = monthly_summary.iloc[-2]
        if prev["sales_amount"] and (latest["sales_amount"] < prev["sales_amount"] * (1 - thresholds["revenue_drop_pct"])):
            drop_pct = (latest["sales_amount"] - prev["sales_amount"]) / prev["sales_amount"]
            alerts.append(f"売上が前月比で{drop_pct:.1%}減少しています。原因分析を行ってください。")

    churn_rate = kpi_summary.get("churn_rate") if kpi_summary else None
    if churn_rate and churn_rate > thresholds["churn_rate"]:
        alerts.append(f"解約率が{churn_rate:.1%}と高水準です。定期顧客のフォローを見直してください。")

    gross_margin_rate = kpi_summary.get("gross_margin_rate") if kpi_summary else None
    if gross_margin_rate and gross_margin_rate < thresholds["gross_margin_rate"]:
        alerts.append(f"粗利率が{gross_margin_rate:.1%}と目標を下回っています。商品ミックスを確認しましょう。")

    if cashflow_forecast is not None and not cashflow_forecast.empty:
        min_balance = cashflow_forecast["cash_balance"].min()
        if min_balance < thresholds["cash_balance"]:
            alerts.append("将来の資金残高がマイナスに落ち込む見込みです。資金繰り対策を検討してください。")

    return alerts
