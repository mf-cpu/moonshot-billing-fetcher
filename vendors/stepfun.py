"""
阶跃星辰 (StepFun) API 模块
"""

import json
import os

import requests


STEPFUN_API = os.getenv(
    "STEPFUN_USAGE_URL",
    "https://platform.stepfun.com/api/step.openapi.devcenter.Dashboard/DevQueryUsageHistory",
)


def _parse_cookie_header(cookie_header: str) -> dict:
    out = {}
    for part in cookie_header.split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        k, v = part.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def _parse_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _first_cost_value(record: dict, cost_keys: list[str]):
    for key in cost_keys:
        if key in record:
            val = _parse_float(record.get(key))
            if val is not None:
                return val
    return None


def _stepfun_cost_keys() -> list[str]:
    override = os.getenv("STEPFUN_COST_KEYS", "").strip()
    if override:
        return [k.strip() for k in override.split(",") if k.strip()]
    return [
        "cost",
        "costAmount",
        "cost_amount",
        "fee",
        "feeAmount",
        "billingAmount",
        "chargeAmount",
        "amountYuan",
        "amountCny",
        "amountRmb",
    ]


def fetch_stepfun_usage(
    cookie_header: str,
    from_ms: int,
    to_ms: int,
    *,
    page: int = 1,
    page_size: int = 200,
    quota_type: str = "1",
    merge_by_time: int = 1,
):
    """
    获取阶跃星辰用量数据
    返回 API 响应 JSON
    """
    headers = {
        "accept": "*/*",
        "accept-language": "zh-CN,zh;q=0.9",
        "content-type": "application/json",
        "oasis-appid": os.environ["STEPFUN_OASIS_APPID"],
        "oasis-platform": os.environ.get("STEPFUN_OASIS_PLATFORM", "web"),
        "oasis-webid": os.environ["STEPFUN_OASIS_WEBID"],
        "origin": "https://platform.stepfun.com",
        "referer": "https://platform.stepfun.com/account-overview",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0",
    }

    payload = {
        "fromTime": str(from_ms),
        "toTime": str(to_ms),
        "pageSize": int(page_size),
        "page": int(page),
        "quotaType": str(quota_type),
        "mergeByTime": int(merge_by_time),
    }

    cookies = _parse_cookie_header(cookie_header)
    r = requests.post(STEPFUN_API, headers=headers, data=json.dumps(payload), timeout=30, cookies=cookies)
    r.raise_for_status()
    return r.json()


def sum_stepfun_metrics(records: list[dict]) -> dict:
    """
    汇总阶跃星辰指标
    返回 {tokens, cost, currency}
    """
    total_tokens = 0
    total_cost = 0.0
    currency = os.getenv("STEPFUN_COST_CURRENCY", "CNY")
    cost_keys = _stepfun_cost_keys()

    for record in records:
        input_tokens = int(record.get("inputTokens") or 0)
        output_tokens = int(record.get("outputTokens") or 0)
        total_tokens += input_tokens + output_tokens

        cost_val = _first_cost_value(record, cost_keys)
        if cost_val is not None:
            total_cost += cost_val

    return {
        "tokens": total_tokens,
        "cost": round(total_cost, 6),
        "currency": currency,
    }
