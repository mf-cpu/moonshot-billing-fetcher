"""
火山引擎账单 API 模块
"""

import hashlib
import hmac
import json
import os
from datetime import datetime
from urllib.parse import parse_qsl, quote, urlparse, urlunparse

import requests

from db_utils import _normalize_amount, _safe_float


VOLCENGINE_API = os.getenv(
    "VOLCENGINE_BILLING_URL",
    "https://open.volcengineapi.com?Action=ListBillDetail&Version=2022-01-01",
)


def _volcengine_credentials():
    access_key = os.environ.get("VOLCENGINE_ACCESS_KEY")
    secret_key = os.environ.get("VOLCENGINE_SECRET_KEY")
    region = os.environ.get("VOLCENGINE_REGION", "cn-beijing")
    security_token = os.environ.get("VOLCENGINE_SECURITY_TOKEN") or None
    if not access_key or not secret_key:
        raise RuntimeError("missing env: VOLCENGINE_ACCESS_KEY/VOLCENGINE_SECRET_KEY")
    return access_key, secret_key, region, security_token


def _volcengine_headers():
    return {
        "Content-Type": "application/json; charset=utf-8",
        "User-Agent": "volcengine-python-sdk",
    }


def _hmac_sha256(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _volcengine_sign_headers(
    method: str,
    base_url: str,
    params: dict,
    body: str,
    access_key: str,
    secret_key: str,
    region: str,
    service: str = "billing",
    security_token: str | None = None,
):
    parsed = urlparse(base_url)
    host = parsed.netloc or "open.volcengineapi.com"
    base_path = parsed.path or "/"
    existing_params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query_params = {**existing_params, **params}
    canonical_query = "&".join(
        f"{quote(k, safe='-_.~')}={quote(str(query_params[k]), safe='-_.~')}"
        for k in sorted(query_params.keys())
    )

    now = datetime.utcnow()
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    datestamp = now.strftime("%Y%m%d")
    scope = f"{datestamp}/{region}/{service}/request"

    payload_hash = hashlib.sha256(body.encode("utf-8")).hexdigest()
    canonical_headers = (
        f"content-type:application/json; charset=utf-8\n"
        f"host:{host}\n"
        f"x-content-sha256:{payload_hash}\n"
        f"x-date:{amz_date}\n"
    )
    signed_headers = "content-type;host;x-content-sha256;x-date"
    canonical_request = (
        f"{method.upper()}\n"
        f"{base_path}\n"
        f"{canonical_query}\n"
        f"{canonical_headers}\n"
        f"{signed_headers}\n"
        f"{payload_hash}"
    )

    canonical_request_hash = hashlib.sha256(
        canonical_request.encode("utf-8")
    ).hexdigest()
    string_to_sign = f"HMAC-SHA256\n{amz_date}\n{scope}\n{canonical_request_hash}"

    k_date = _hmac_sha256(secret_key.encode("utf-8"), datestamp)
    k_region = _hmac_sha256(k_date, region)
    k_service = _hmac_sha256(k_region, service)
    k_signing = _hmac_sha256(k_service, "request")
    signature = hmac.new(
        k_signing, string_to_sign.encode("utf-8"), hashlib.sha256
    ).hexdigest()

    authorization = (
        f"HMAC-SHA256 Credential={access_key}/{scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )

    headers = _volcengine_headers()
    headers.update(
        {
            "Host": host,
            "X-Date": amz_date,
            "X-Content-Sha256": payload_hash,
            "Authorization": authorization,
        }
    )
    if security_token:
        headers["X-Security-Token"] = security_token

    signed_url = urlunparse(
        (parsed.scheme or "https", host, base_path, "", canonical_query, "")
    )
    return signed_url, headers


def fetch_volcengine_bill_daily(
    billing_date, *, limit: int = 100, ignore_zero: int = 0, verbose: bool = False
):
    """
    获取火山引擎日账单
    返回 (rows, summary)
    """
    access_key, secret_key, region, security_token = _volcengine_credentials()
    bill_period = billing_date.strftime("%Y-%m")
    expense_date = billing_date.strftime("%Y-%m-%d")
    offset = 0
    rows = []

    while True:
        payload = {
            "BillPeriod": bill_period,
            "ExpenseDate": expense_date,
            "GroupPeriod": 1,
            "GroupTerm": 0,
            "Limit": int(limit),
            "Offset": int(offset),
            "NeedRecordNum": 1,
            "IgnoreZero": int(ignore_zero),
        }
        signed_url, signed_headers = _volcengine_sign_headers(
            "POST",
            VOLCENGINE_API,
            {"Action": "ListBillDetail", "Version": "2022-01-01"},
            json.dumps(payload, ensure_ascii=False),
            access_key,
            secret_key,
            region,
            service="billing",
            security_token=security_token,
        )
        if verbose:
            print(f"[VOLCENGINE] request url: {signed_url}")
            print(f"[VOLCENGINE] request body: {json.dumps(payload, ensure_ascii=False)}")
        resp = requests.post(signed_url, headers=signed_headers, json=payload, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(f"volcengine http {resp.status_code}: {resp.text[:200]}")
        body = resp.json()
        err = (body.get("ResponseMetadata") or {}).get("Error")
        if err:
            raise RuntimeError(f"volcengine api error: {err}")
        if verbose and offset == 0:
            print(f"[VOLCENGINE] response: {json.dumps(body, ensure_ascii=False)}")
        result = body.get("Result") or {}
        batch = result.get("List") or []
        rows.extend(batch)
        total = result.get("Total")
        if not batch:
            break
        offset += len(batch)
        if total and isinstance(total, int) and total > 0 and offset >= total:
            break

    amount_total = 0.0
    gross_total = 0.0
    ai_amount_total = 0.0
    ai_gross_total = 0.0
    non_ai_amount_total = 0.0
    non_ai_gross_total = 0.0
    currencies = set()
    token_total = 0
    token_input = 0
    token_output = 0
    token_rows = []

    # AI 产品关键词（火山引擎）
    AI_KEYWORDS = ["豆包", "doubao", "llm", "大模型", "语言模型", "token", "智能"]

    def is_ai_item(item):
        """判断是否是 AI 产品"""
        # 检查单位是否包含 token
        unit = (item.get("Unit") or "").lower()
        if "token" in unit:
            return True
        # 检查产品名称/元素名称是否包含 AI 关键词
        element = (item.get("Element") or "").lower()
        product = (item.get("Product") or "").lower()
        expand = (item.get("ExpandField") or "").lower()
        instance = (item.get("InstanceName") or "").lower()
        for keyword in AI_KEYWORDS:
            kw = keyword.lower()
            if kw in element or kw in product or kw in expand or kw in instance:
                return True
        return False

    for item in rows:
        gross = _safe_float(item.get("OriginalBillAmount"))
        amount = _safe_float(item.get("PayableAmount"))
        if amount == 0.0:
            amount = _safe_float(item.get("PreferentialBillAmount"))
        if amount == 0.0:
            amount = _safe_float(item.get("PretaxAmount"))
        amount_total += amount
        gross_total += gross
        currency = item.get("Currency")
        if currency:
            currencies.add(currency)

        # 判断是否 AI 产品
        if is_ai_item(item):
            ai_amount_total += amount
            ai_gross_total += gross
        else:
            non_ai_amount_total += amount
            non_ai_gross_total += gross

        # 提取 Token 用量
        unit_raw = item.get("Unit") or ""
        unit = unit_raw.lower()
        if "token" in unit:
            count = _safe_float(item.get("Count") or 0)
            # 根据单位转换：千tokens 乘 1000，否则直接取值
            is_kilo = "千" in unit_raw or unit.startswith("k") or "ktoken" in unit
            if is_kilo:
                tokens = int(count * 1000)
            else:
                tokens = int(count)
            token_total += tokens
            element = item.get("Element") or ""
            if "输入" in element or "input" in element.lower():
                token_input += tokens
            elif "输出" in element or "output" in element.lower():
                token_output += tokens
            token_rows.append({
                "element": element,
                "model": item.get("ExpandField") or "",
                "tokens": tokens,
                "count": count,
                "unit": item.get("Unit"),
            })

    if len(currencies) == 1:
        currency = next(iter(currencies))
    elif len(currencies) > 1:
        currency = "MIXED"
    else:
        currency = "CNY"

    summary = {
        "amount": _normalize_amount(amount_total),
        "gross": _normalize_amount(gross_total),
        "ai_amount": _normalize_amount(ai_amount_total),
        "ai_gross": _normalize_amount(ai_gross_total),
        "non_ai_amount": _normalize_amount(non_ai_amount_total),
        "non_ai_gross": _normalize_amount(non_ai_gross_total),
        "currency": currency,
        "rows": len(rows),
        "total": total if isinstance(total, int) else None,
        "token_total": token_total,
        "token_input": token_input,
        "token_output": token_output,
        "token_rows": token_rows,
    }
    return rows, summary
