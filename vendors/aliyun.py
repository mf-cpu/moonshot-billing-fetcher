"""
阿里云账单 API 模块
"""

import os
from db_utils import is_ai_product, _parse_items


def aliyun_bss_client():
    """创建阿里云 BSS 客户端"""
    try:
        from alibabacloud_bssopenapi20171214.client import Client as BssClient
        from alibabacloud_tea_openapi import models as open_api_models
        from alibabacloud_bssopenapi20171214 import models as bss_models
    except ImportError as exc:
        raise RuntimeError("missing aliyun bss sdk") from exc

    ak = os.getenv("ALIYUN_ACCESS_KEY_ID")
    sk = os.getenv("ALIYUN_ACCESS_KEY_SECRET")
    if not ak or not sk:
        raise RuntimeError("missing env: ALIYUN_ACCESS_KEY_ID/ALIYUN_ACCESS_KEY_SECRET")

    config = open_api_models.Config(
        access_key_id=ak,
        access_key_secret=sk,
        endpoint="business.aliyuncs.com",
    )
    return BssClient(config), bss_models


def fetch_aliyun_bill_rows(client, bss_models, billing_date):
    """
    获取阿里云账单明细
    返回 (rows, summary)
    """
    billing_cycle = billing_date.strftime("%Y-%m")
    request = bss_models.QueryAccountBillRequest(
        billing_cycle=billing_cycle,
        billing_date=billing_date.strftime("%Y-%m-%d"),
        granularity="DAILY",
        is_group_by_product=True,
        page_num=1,
        page_size=200,
    )
    response = client.query_account_bill(request)
    body = response.body
    data = body.data
    if not data:
        return [], {"amount": 0.0, "gross": 0.0, "currency": "CNY"}

    items_list = _parse_items(data.items)
    if not items_list:
        return [], {"amount": 0.0, "gross": 0.0, "currency": "CNY"}

    aggregated = {}
    billing_date_str = billing_date.strftime("%Y-%m-%d")
    for it in items_list:
        product_code = it.get("ProductCode")
        product_name = it.get("ProductName")
        currency = it.get("Currency")
        is_ai = is_ai_product(product_code)
        key = (
            billing_date_str,
            billing_cycle,
            product_code,
            product_name,
            currency,
            is_ai,
        )
        amount = float(it.get("PretaxAmount") or 0)
        gross = float(it.get("PretaxGrossAmount") or 0)
        if key not in aggregated:
            row = {
                "billing_date": billing_date_str,
                "billing_cycle": billing_cycle,
                "product_code": product_code,
                "product_name": product_name,
                "pretax_amount": amount,
                "pretax_gross_amount": gross,
                "currency": currency,
                "is_ai_cost": is_ai,
            }
            aggregated[key] = row
        else:
            aggregated[key]["pretax_amount"] += amount
            aggregated[key]["pretax_gross_amount"] += gross

    rows = list(aggregated.values())
    amount_total = float(sum(r.get("pretax_amount") or 0 for r in rows))
    gross_total = float(sum(r.get("pretax_gross_amount") or 0 for r in rows))
    currencies = {r.get("currency") for r in rows if r.get("currency")}
    if len(currencies) == 1:
        currency = next(iter(currencies))
    elif len(currencies) > 1:
        currency = "MIXED"
    else:
        currency = "CNY"
    return rows, {"amount": amount_total, "gross": gross_total, "currency": currency}
