#!/usr/bin/env python3
"""
Authing 月固定费用入库脚本
每月固定 99 元，非 AI 成本
"""

import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()


def get_supabase_client():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise ValueError("missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")
    return create_client(url, key)


def upsert_bill_monthly_summary(sb, vendor_code, month, amount, gross_amount, currency, is_ai_cost):
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


def insert_authing_months(start_month: str, end_month: str, amount: float = 99.0):
    """
    插入 Authing 月度费用
    start_month: 开始月份 YYYY-MM
    end_month: 结束月份 YYYY-MM
    amount: 月固定费用，默认 99 元
    """
    sb = get_supabase_client()

    start = datetime.strptime(start_month, "%Y-%m")
    end = datetime.strptime(end_month, "%Y-%m")

    current = start
    count = 0
    while current <= end:
        month_str = current.strftime("%Y-%m")
        upsert_bill_monthly_summary(
            sb,
            vendor_code="authing",
            month=month_str,
            amount=amount,
            gross_amount=amount,
            currency="CNY",
            is_ai_cost=False,
        )
        print(f"[OK] authing month={month_str} amount={amount} CNY")
        current += relativedelta(months=1)
        count += 1

    print(f"\n共插入 {count} 条 Authing 月度记录")


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("用法: python authing_monthly.py <开始月份> <结束月份> [月费]")
        print("示例: python authing_monthly.py 2025-01 2026-01")
        print("示例: python authing_monthly.py 2025-01 2026-01 99")
        sys.exit(1)

    start = sys.argv[1]
    end = sys.argv[2]
    fee = float(sys.argv[3]) if len(sys.argv) > 3 else 99.0

    insert_authing_months(start, end, fee)
