import os
import json
from decimal import Decimal
from datetime import date
import argparse

import boto3
from botocore.config import Config
from supabase import create_client


DEFAULT_METRIC = "UnblendedCost"
NET_METRIC = "NetUnblendedCost"


def month_range(month_str: str | None = None) -> tuple[str, str]:
    if month_str:
        year, month = (int(part) for part in month_str.split("-", 1))
        start = date(year, month, 1)
    else:
        today = date.today()
        start = date(today.year, today.month, 1)
    next_month = (start.replace(day=28) + date.resolution * 4).replace(day=1)
    return start.isoformat(), next_month.isoformat()


def ce_client():
    region = (
        os.environ.get("AWS_COST_EXPLORER_REGION")
        or os.environ.get("AWS_REGION")
        or os.environ.get("AWS_DEFAULT_REGION")
        or "us-east-1"
    )
    return boto3.client(
        "ce",
        region_name=region,
        config=Config(retries={"max_attempts": 5, "mode": "standard"}),
    )


def _merge_results(pages: list[dict]) -> dict:
    merged = {
        "ResultsByTime": [],
        "GroupDefinitions": [],
        "DimensionValueAttributes": [],
    }
    for page in pages:
        merged["ResultsByTime"].extend(page.get("ResultsByTime", []))
        if page.get("GroupDefinitions"):
            merged["GroupDefinitions"] = page["GroupDefinitions"]
        if page.get("DimensionValueAttributes"):
            merged["DimensionValueAttributes"] = page["DimensionValueAttributes"]
    return merged


def get_cost_and_usage_all_pages(client, payload: dict) -> dict:
    pages = []
    token = None
    while True:
        body = dict(payload)
        if token:
            body["NextPageToken"] = token
        resp = client.get_cost_and_usage(**body)
        pages.append(resp)
        token = resp.get("NextPageToken")
        if not token:
            break
    return _merge_results(pages)


def _amount(metrics: dict, metric: str) -> Decimal:
    value = metrics.get(metric, {}).get("Amount", "0")
    try:
        return Decimal(value)
    except Exception:
        return Decimal("0")


def _unit(metrics: dict, metric: str) -> str:
    return metrics.get(metric, {}).get("Unit", "USD") or "USD"


def fetch_monthly_total(
    start: str,
    end: str,
    metric: str = DEFAULT_METRIC,
    cost_filter: dict | None = None,
):
    client = ce_client()
    payload = {
        "TimePeriod": {"Start": start, "End": end},
        "Granularity": "MONTHLY",
        "Metrics": [metric],
    }
    if cost_filter:
        payload["Filter"] = cost_filter
    data = get_cost_and_usage_all_pages(client, payload)
    if not data.get("ResultsByTime"):
        return Decimal("0"), "USD"
    total = data["ResultsByTime"][0].get("Total", {})
    return _amount(total, metric), _unit(total, metric)


def fetch_monthly_totals(
    start: str, end: str, metrics: list[str], cost_filter: dict | None = None
):
    client = ce_client()
    payload = {
        "TimePeriod": {"Start": start, "End": end},
        "Granularity": "MONTHLY",
        "Metrics": metrics,
    }
    if cost_filter:
        payload["Filter"] = cost_filter
    data = get_cost_and_usage_all_pages(client, payload)
    if not data.get("ResultsByTime"):
        return {metric: (Decimal("0"), "USD") for metric in metrics}
    total = data["ResultsByTime"][0].get("Total", {})
    return {metric: (_amount(total, metric), _unit(total, metric)) for metric in metrics}


def fetch_monthly_daily(start: str, end: str, metric: str = DEFAULT_METRIC, cost_filter: dict | None = None):
    client = ce_client()
    payload = {
        "TimePeriod": {"Start": start, "End": end},
        "Granularity": "DAILY",
        "Metrics": [metric],
    }
    if cost_filter:
        payload["Filter"] = cost_filter
    data = get_cost_and_usage_all_pages(client, payload)
    daily = []
    for row in data.get("ResultsByTime", []):
        day = row.get("TimePeriod", {}).get("Start")
        total = _amount(row.get("Total", {}), metric)
        daily.append({"day": day, "amount": total})
    return daily


def fetch_monthly_by_service(start: str, end: str, metric: str = DEFAULT_METRIC, cost_filter: dict | None = None):
    client = ce_client()
    payload = {
        "TimePeriod": {"Start": start, "End": end},
        "Granularity": "MONTHLY",
        "Metrics": [metric],
        "GroupBy": [{"Type": "DIMENSION", "Key": "SERVICE"}],
    }
    if cost_filter:
        payload["Filter"] = cost_filter
    data = get_cost_and_usage_all_pages(client, payload)
    if not data.get("ResultsByTime"):
        return []
    groups = data["ResultsByTime"][0].get("Groups", [])
    out = []
    for g in groups:
        service = (g.get("Keys") or [None])[0]
        amount = _amount(g.get("Metrics", {}), metric)
        out.append({"service": service, "amount": amount})
    return out


def supabase_client():
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_ROLE_KEY"])


def upsert_bill_monthly_summary(
    sb,
    vendor_code: str,
    month: str,
    amount: Decimal,
    gross_amount: Decimal,
    currency: str,
    is_ai_cost: bool = False,
):
    row = {
        "vendor_code": vendor_code,
        "month": month,
        "is_ai_cost": is_ai_cost,
        "amount": float(amount),
        "gross_amount": float(gross_amount),
        "currency": currency,
    }
    return (
        sb.schema("financial_hub_prod")
        .table("bill_monthly_summary")
        .upsert(row, on_conflict="vendor_code,month,is_ai_cost")
        .execute()
    )


def main():
    parser = argparse.ArgumentParser(description="AWS Cost Explorer monthly billing fetch")
    parser.add_argument("--month", help="YYYY-MM, default: current month")
    parser.add_argument("--metric", default=DEFAULT_METRIC)
    parser.add_argument("--by-service", action="store_true")
    parser.add_argument("--daily", action="store_true")
    parser.add_argument("--filter-json", help="Cost Explorer Filter JSON string")
    parser.add_argument("--write-summary", action="store_true", help="Upsert into bill_monthly_summary")
    args = parser.parse_args()

    start, end = month_range(args.month)
    cost_filter = json.loads(args.filter_json) if args.filter_json else None

    if args.by_service:
        data = fetch_monthly_by_service(start, end, metric=args.metric, cost_filter=cost_filter)
        print(json.dumps(data, ensure_ascii=False, default=str))
        return
    if args.daily:
        data = fetch_monthly_daily(start, end, metric=args.metric, cost_filter=cost_filter)
        print(json.dumps(data, ensure_ascii=False, default=str))
        return

    total, currency = fetch_monthly_total(
        start, end, metric=args.metric, cost_filter=cost_filter
    )
    print(str(total))
    if args.write_summary:
        sb = supabase_client()
        month_str = start[:7]
        totals = fetch_monthly_totals(
            start, end, metrics=[DEFAULT_METRIC, NET_METRIC], cost_filter=cost_filter
        )
        gross_amount, gross_currency = totals[DEFAULT_METRIC]
        net_amount, net_currency = totals[NET_METRIC]
        currency = net_currency or gross_currency or currency
        upsert_bill_monthly_summary(
            sb,
            "aws",
            month_str,
            net_amount,
            gross_amount,
            currency,
            is_ai_cost=False,
        )


if __name__ == "__main__":
    main()
