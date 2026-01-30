"""
AWS 账单 API 模块
"""

import os
from datetime import timedelta
from db_utils import _normalize_amount


def aws_ce_client():
    """创建 AWS Cost Explorer 客户端"""
    try:
        import boto3
        from botocore.config import Config
    except ImportError as exc:
        raise RuntimeError("missing aws sdk (boto3)") from exc

    access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    session_token = (os.environ.get("AWS_SESSION_TOKEN") or "").strip() or None
    region_name = (
        os.environ.get("AWS_REGION")
        or os.environ.get("AWS_DEFAULT_REGION")
        or "cn-north-1"
    )
    return boto3.client(
        "ce",
        region_name=region_name,
        aws_access_key_id=access_key_id or None,
        aws_secret_access_key=secret_access_key or None,
        aws_session_token=session_token or None,
        config=Config(retries={"max_attempts": 5, "mode": "standard"}),
    )


def fetch_aws_bill_daily(client, billing_date, *, include_raw: bool = False):
    """
    获取 AWS 日账单
    返回 summary 字典
    """
    start = billing_date.strftime("%Y-%m-%d")
    end = (billing_date + timedelta(days=1)).strftime("%Y-%m-%d")
    resp = client.get_cost_and_usage(
        TimePeriod={"Start": start, "End": end},
        Granularity="DAILY",
        Metrics=["UnblendedCost", "NetUnblendedCost"],
        GroupBy=[{"Type": "DIMENSION", "Key": "RECORD_TYPE"}],
    )
    results = resp.get("ResultsByTime", [])
    if not results:
        summary = {
            "amount": 0.0,
            "gross": 0.0,
            "currency": "USD",
            "record_type_totals": {},
            "usage_amount": 0.0,
            "credit_amount": 0.0,
        }
        if include_raw:
            summary["raw_response"] = resp
        return summary

    first = results[0]
    total = first.get("Total") or {}
    gross = total.get("UnblendedCost", {})
    net = total.get("NetUnblendedCost", {})
    gross_amount = float(gross.get("Amount") or 0)
    net_amount = float(net.get("Amount") or 0)
    record_type_totals = {}
    record_type_net_totals = {}
    currency = gross.get("Unit") or net.get("Unit") or ""
    raw_net_sum = 0.0

    for group in first.get("Groups", []) or []:
        keys = group.get("Keys") or []
        record_type = keys[0] if keys else "UNKNOWN"
        metrics = group.get("Metrics", {})
        unblended = metrics.get("UnblendedCost", {}) or {}
        net_unblended = metrics.get("NetUnblendedCost", {}) or {}
        unblended_amount = float(unblended.get("Amount") or 0)
        net_amount_value = float(net_unblended.get("Amount") or 0)
        record_type_totals[record_type] = _normalize_amount(unblended_amount)
        record_type_net_totals[record_type] = _normalize_amount(net_amount_value)
        raw_net_sum += net_amount_value
        if not currency:
            currency = unblended.get("Unit") or net_unblended.get("Unit") or ""

    if not currency:
        currency = "USD"

    if record_type_totals:
        excluded_types = {"Credit", "Discount", "Refund"}
        gross_from_record_types = sum(
            value
            for key, value in record_type_totals.items()
            if key not in excluded_types and value > 0
        )
        if gross_from_record_types > 0:
            gross_amount = float(gross_from_record_types)

    if not gross_amount and record_type_totals:
        gross_amount = float(record_type_totals.get("Usage") or 0)
    if not net_amount and record_type_net_totals:
        net_amount = float(raw_net_sum)

    usage_amount = record_type_totals.get("Usage", 0.0)
    credit_amount = record_type_totals.get("Credit", 0.0)
    if usage_amount > 0 and credit_amount < 0:
        net_amount = usage_amount
        gross_amount = usage_amount - credit_amount

    summary = {
        "amount": _normalize_amount(net_amount),
        "gross": _normalize_amount(gross_amount),
        "currency": currency,
        "raw_amount": net_amount,
        "raw_gross": gross_amount,
        "record_type_totals": record_type_totals,
        "record_type_net_totals": record_type_net_totals,
        "usage_amount": usage_amount,
        "credit_amount": credit_amount,
    }
    if include_raw:
        summary["raw_response"] = resp
    return summary
