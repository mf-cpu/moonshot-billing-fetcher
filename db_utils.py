"""
数据库操作工具模块
包含 Supabase 相关的 upsert、查询、删除等函数
"""

import hashlib
from datetime import datetime, timedelta

try:
    from postgrest.exceptions import APIError
except ImportError:
    APIError = Exception


def stable_bigint(s: str) -> int:
    """生成稳定的 bigint ID（基于字符串哈希）"""
    h = hashlib.sha256(s.encode("utf-8")).hexdigest()
    return int(h[:15], 16)


def iter_days(start_day: str, end_day: str):
    """迭代日期范围（包含起止日期）"""
    start = datetime.fromisoformat(start_day).date()
    end = datetime.fromisoformat(end_day).date()
    current = start
    while current <= end:
        yield current.isoformat()
        current += timedelta(days=1)


def _normalize_amount(amount):
    """规范化金额：四舍五入到 6 位小数"""
    try:
        return round(float(amount or 0), 6)
    except (TypeError, ValueError):
        return 0.0


def _safe_float(val) -> float:
    """安全转换为 float"""
    if val is None:
        return 0.0
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


# ==================== Token 相关 ====================

def upsert_weekly_with_id(sb, vendor: str, week_start: str, week_end: str, token_total: int):
    row = {
        "id": stable_bigint(f"week:{vendor}:{week_start}:{week_end}"),
        "vendor_code": vendor,
        "week_start": week_start,
        "week_end": week_end,
        "token_total": token_total,
    }
    return (
        sb.schema("financial_hub_prod")
        .table("llm_token_weekly_usage")
        .upsert(row, on_conflict="vendor_code,week_start,week_end")
        .execute()
    )


def upsert_monthly_with_id(sb, vendor: str, month: str, token_total: int):
    row = {
        "id": stable_bigint(f"month:{vendor}:{month}"),
        "vendor_code": vendor,
        "month": month,
        "token_total": token_total,
    }
    return (
        sb.schema("financial_hub_prod")
        .table("llm_token_monthly_usage")
        .upsert(row, on_conflict="vendor_code,month")
        .execute()
    )


def delete_existing_daily(sb, day_str: str, vendor: str, project_id: str | None):
    query = (
        sb.schema("financial_hub_prod")
        .table("llm_token_daily_usage")
        .delete()
        .eq("day", day_str)
        .eq("vendor", vendor)
    )
    if project_id:
        query = query.eq("project_id", project_id)
    return query.execute()


# ==================== 账单相关 ====================

def delete_aliyun_bill_daily(sb, billing_date: str):
    return (
        sb.schema("financial_hub_prod")
        .table("aliyun_bill_daily")
        .delete()
        .eq("billing_date", billing_date)
        .execute()
    )


def upsert_bill_daily_summary(
    sb,
    vendor_code: str,
    billing_date: str,
    amount: float,
    gross_amount: float,
    currency: str,
    is_ai_cost: bool = False,
):
    row = {
        "vendor_code": vendor_code,
        "billing_date": billing_date,
        "is_ai_cost": is_ai_cost,
        "amount": float(amount or 0),
        "gross_amount": float(gross_amount or 0),
        "currency": currency or "USD",
    }
    return (
        sb.schema("financial_hub_prod")
        .table("bill_daily_summary")
        .upsert(row, on_conflict="vendor_code,billing_date,is_ai_cost")
        .execute()
    )


def upsert_bill_weekly_summary(
    sb,
    vendor_code: str,
    week_start: str,
    week_end: str,
    amount: float,
    gross_amount: float,
    currency: str,
    is_ai_cost: bool,
):
    row = {
        "vendor_code": vendor_code,
        "week_start": week_start,
        "week_end": week_end,
        "is_ai_cost": is_ai_cost,
        "amount": float(amount or 0),
        "gross_amount": float(gross_amount or 0),
        "currency": currency or "CNY",
    }
    return (
        sb.schema("financial_hub_prod")
        .table("bill_weekly_summary")
        .upsert(row, on_conflict="vendor_code,week_start,week_end,is_ai_cost")
        .execute()
    )


def upsert_bill_monthly_summary(
    sb,
    vendor_code: str,
    month: str,
    amount: float,
    gross_amount: float,
    currency: str,
    is_ai_cost: bool,
):
    row = {
        "vendor_code": vendor_code,
        "month": month,
        "is_ai_cost": is_ai_cost,
        "amount": float(amount or 0),
        "gross_amount": float(gross_amount or 0),
        "currency": currency or "CNY",
    }
    return (
        sb.schema("financial_hub_prod")
        .table("bill_monthly_summary")
        .upsert(row, on_conflict="vendor_code,month,is_ai_cost")
        .execute()
    )


def sum_bill_daily(sb, vendor_code: str, is_ai_cost: bool, start_date: str, end_date: str):
    resp = (
        sb.schema("financial_hub_prod")
        .table("bill_daily_summary")
        .select("amount,gross_amount,currency")
        .eq("vendor_code", vendor_code)
        .eq("is_ai_cost", is_ai_cost)
        .gte("billing_date", start_date)
        .lte("billing_date", end_date)
        .execute()
    )
    rows = resp.data or []
    if not rows:
        return None
    amount_total = float(sum(float(r.get("amount") or 0) for r in rows))
    gross_total = float(sum(float(r.get("gross_amount") or 0) for r in rows))
    currencies = {r.get("currency") for r in rows if r.get("currency")}
    if len(currencies) == 1:
        currency = next(iter(currencies))
    elif len(currencies) > 1:
        currency = "MIXED"
    else:
        currency = "CNY"
    return {
        "amount": _normalize_amount(amount_total),
        "gross": _normalize_amount(gross_total),
        "currency": currency,
    }


# ==================== 辅助函数 ====================

def _strip_pretax_gross(rows: list[dict]) -> list[dict]:
    cleaned = []
    for row in rows:
        item = dict(row)
        item.pop("pretax_gross_amount", None)
        cleaned.append(item)
    return cleaned


def _is_missing_gross_error(exc: Exception) -> bool:
    message = ""
    if isinstance(exc, APIError):
        try:
            message = (exc.args[0] or {}).get("message", "")
        except Exception:
            message = str(exc)
    else:
        message = str(exc)
    return "pretax_gross_amount" in message


AI_PRODUCT_SET = {
    "sfm",
}


def is_ai_product(product_code: str) -> bool:
    return (product_code or "").strip().lower() in AI_PRODUCT_SET


def _parse_items(items):
    items_list = []
    if isinstance(items, list):
        items_list = items
    elif hasattr(items, "to_map"):
        mapped = items.to_map()
        for key in ["Item", "item", "Items", "items"]:
            if key in mapped:
                value = mapped[key]
                if isinstance(value, list):
                    items_list = value
                elif isinstance(value, dict):
                    items_list = [value]
                break
    return items_list
