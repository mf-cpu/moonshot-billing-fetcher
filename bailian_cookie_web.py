import hashlib
import hmac
import html
import json
import os
import threading
import time
import webbrowser
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, parse_qsl, quote, urlencode, urlparse, urlunparse

import requests
try:
    from dotenv import load_dotenv
except ImportError:  # 兼容未安装 python-dotenv 的环境
    load_dotenv = None
try:
    from postgrest.exceptions import APIError
except ImportError:  # 兼容未安装 postgrest 的环境
    APIError = Exception
from aliyun_token_ingest_daily import (
    bj_day_range,
    fetch_usage,
    parse_total_tokens,
    supabase_client,
    insert_daily_row,
    sum_token_daily,
    upsert_monthly_summary,
    upsert_weekly_summary,
    week_bounds,
)


BJ = timezone(timedelta(hours=8))
STEPFUN_API = os.getenv(
    "STEPFUN_USAGE_URL",
    "https://platform.stepfun.com/api/step.openapi.devcenter.Dashboard/DevQueryUsageHistory",
)
VOLCENGINE_API = os.getenv(
    "VOLCENGINE_BILLING_URL",
    "https://open.volcengineapi.com?Action=ListBillDetail&Version=2022-01-01",
)
TIANYANCHA_API = os.getenv(
    "TIANYANCHA_BILL_URL",
    "https://open.tianyancha.com/open-admin/org/order.json",
)
KIMI_API = os.getenv(
    "KIMI_BILL_URL",
    "https://platform.moonshot.cn/api",
)
TEXTIN_API = os.getenv(
    "TEXTIN_BILL_URL",
    "https://web-api.textin.com/user/finance/consume",
)

# 加载本地 .env（若存在），方便读取配置
if load_dotenv:
    load_dotenv()

def yesterday_bj() -> str:
    return (datetime.now(tz=BJ) - timedelta(days=1)).date().isoformat()


def read_body(handler: BaseHTTPRequestHandler) -> bytes:
    length = int(handler.headers.get("content-length", "0") or "0")
    return handler.rfile.read(length)


def send_plain_error(handler: BaseHTTPRequestHandler, code: int, message: str) -> None:
    handler.send_response(code)
    handler.send_header("content-type", "text/plain; charset=utf-8")
    handler.end_headers()
    handler.wfile.write(message.encode("utf-8", "replace"))


def render_form(default_start: str, default_end: str) -> str:
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>数据拉取</title>
  <style>
    body {{ font-family: Arial, sans-serif; padding: 24px; max-width: 900px; margin: 0 auto; }}
    label {{ display: block; margin-top: 14px; font-weight: 600; }}
    input, textarea, select {{ width: 100%; padding: 8px; box-sizing: border-box; }}
    textarea {{ height: 140px; font-family: monospace; }}
    button {{ margin-top: 16px; padding: 10px 16px; }}
    .hint {{ color: #555; font-size: 12px; }}
    .warn {{ color: #a00; font-size: 12px; }}
  </style>
</head>
<body>
  <h2>用量拉取（临时 Cookie）</h2>
  <p class="warn">Cookie 仅本次请求使用，不会落库或写日志。建议仅在内网使用。</p>
  <p class="hint">
    百炼依赖环境变量：ALIYUN_BAILIAN_WORKSPACE_ID / ALIYUN_BAILIAN_REGION / ALIYUN_BAILIAN_USAGE_URL
    （可选：ALIYUN_BAILIAN_SEC_TOKEN / ALIYUN_BAILIAN_CSRF_TOKEN）
  </p>
  <p class="hint">
    阶跃星辰依赖环境变量：STEPFUN_OASIS_APPID / STEPFUN_OASIS_WEBID
    （可选：STEPFUN_OASIS_PLATFORM / STEPFUN_PROJECT_ID / STEPFUN_COST_KEYS / STEPFUN_COST_CURRENCY）
  </p>
  <p class="hint">
    阿里云账单依赖环境变量：ALIYUN_ACCESS_KEY_ID / ALIYUN_ACCESS_KEY_SECRET
  </p>
  <p class="hint">
    AWS 账单依赖环境变量：AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
    （可选：AWS_SESSION_TOKEN / AWS_REGION / AWS_DEFAULT_REGION）
  </p>
  <p class="hint">
    火山引擎账单依赖环境变量：VOLCENGINE_ACCESS_KEY / VOLCENGINE_SECRET_KEY
    （可选：VOLCENGINE_REGION / VOLCENGINE_BILLING_URL）
  </p>
  <p class="hint">
    天眼查账单依赖环境变量：TIANYANCHA_AUTH_SECRET
    （可选：TIANYANCHA_BILL_URL / TIANYANCHA_COOKIE）
  </p>
  <p class="hint">
    月之暗面账单（Web 接口）依赖：Bearer Token / 组织 ID / Cookie
    （环境变量：MOONSHOT_BEARER_TOKEN / MOONSHOT_ORG_ID / MOONSHOT_COOKIE）
  </p>
  <form method="post" action="/fetch">
    <label>供应商（必选）</label>
    <select name="vendor" required>
      <option value="bailian" selected>阿里云百炼（Token）</option>
      <option value="stepfun">阶跃星辰（Token + 消费金额）</option>
      <option value="aliyun_bill">阿里云账单金额（日粒度）</option>
      <option value="aws_bill">亚马逊账单金额（日粒度）</option>
      <option value="volcengine_bill">火山引擎账单金额（日粒度）</option>
      <option value="tianyancha_bill">天眼查账单金额（日粒度，非 AI）</option>
      <option value="moonshot_bill">月之暗面账单金额（日粒度，Web 接口）</option>
      <option value="textin_bill">TextIn 账单金额（日粒度，非 AI）</option>
    </select>

    <label>Cookie（百炼/阶跃必填）</label>
    <textarea name="cookie" placeholder="粘贴完整 Cookie"></textarea>

    <label>天眼查 authSecret（天眼查必填）</label>
    <input type="text" name="auth_secret" placeholder="可留空，默认读取 TIANYANCHA_AUTH_SECRET" />

    <label>天眼查 Cookie（可选，提示“请先登录”时填写）</label>
    <textarea name="tian_cookie" placeholder="粘贴天眼查 Cookie（可留空，默认读取 TIANYANCHA_COOKIE）"></textarea>

    <label>月之暗面 Bearer Token（月之暗面必填）</label>
    <input type="text" name="moonshot_token" placeholder="从浏览器开发者工具复制 authorization 头的 Bearer 值" />

    <label>月之暗面组织 ID（月之暗面必填）</label>
    <input type="text" name="moonshot_org_id" placeholder="如 org-xxx，从 URL 参数 oid 获取" />

    <label>月之暗面 Cookie（月之暗面必填）</label>
    <textarea name="moonshot_cookie" placeholder="粘贴月之暗面 Cookie"></textarea>

    <label>TextIn Token（TextIn 必填）</label>
    <input type="text" name="textin_token" placeholder="如 76233542bd52608ed869d8c98d402ea8" />

    <label>
      <input type="checkbox" name="aws_dump_raw" value="1" checked />
      AWS 输出完整返回（仅亚马逊）
    </label>

    <label>开始日期（必填）</label>
    <input type="date" name="start_day" value="{html.escape(default_start)}" required />

    <label>结束日期（必填）</label>
    <input type="date" name="end_day" value="{html.escape(default_end)}" required />

    <button type="submit">开始拉取并入库</button>
    <button type="submit" name="retry_day" value="1">只重拉开始日期</button>
  </form>
</body>
</html>
"""


def render_result(start_day: str, end_day: str, results: list[dict]) -> str:
    summary_lines = "\n".join(_render_result_line(item) for item in results)
    last_raw = results[-1]["raw"] if results else {}
    raw_json = json.dumps(last_raw, ensure_ascii=False, indent=2)
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>拉取结果</title>
  <style>
    body {{ font-family: Arial, sans-serif; padding: 24px; max-width: 900px; margin: 0 auto; }}
    pre {{ background: #f6f6f6; padding: 12px; overflow-x: auto; }}
  </style>
</head>
<body>
  <h2>拉取结果</h2>
  <p>日期范围：{html.escape(start_day)} ~ {html.escape(end_day)}</p>
  <ul>
    {summary_lines}
  </ul>
  <p>仅展示最后一天原始返回：</p>
  <pre>{html.escape(raw_json)}</pre>
  <p>服务已完成本次入库并即将退出。如需再次拉取，请重新启动脚本。</p>
</body>
</html>
"""


def iter_days(start_day: str, end_day: str):
    start = datetime.fromisoformat(start_day).date()
    end = datetime.fromisoformat(end_day).date()
    cur = start
    while cur <= end:
        yield cur.isoformat()
        cur += timedelta(days=1)


def stable_bigint(key: str) -> int:
    digest = hashlib.sha256(key.encode("utf-8")).digest()
    value = int.from_bytes(digest[:8], "big", signed=False)
    return value % 9_223_372_036_854_775_807


def _render_result_line(item: dict) -> str:
    day = html.escape(str(item.get("day", "")))
    total_tokens = item.get("total_tokens", 0)
    total_cost = item.get("total_cost")
    cost_currency = item.get("cost_currency") or ""
    bill_amount = item.get("bill_amount")
    bill_gross = item.get("bill_gross_amount")
    bill_currency = item.get("bill_currency") or ""
    if bill_amount is not None:
        suffix = f" {bill_currency}".rstrip()
        if bill_gross is None:
            text = f"{day}: amount={bill_amount}{suffix}"
        else:
            text = f"{day}: amount={bill_amount} gross={bill_gross}{suffix}"
    elif total_cost is None:
        text = f"{day}: total_tokens={total_tokens}"
    else:
        suffix = f" {cost_currency}".rstrip()
        text = f"{day}: total_tokens={total_tokens} cost={total_cost}{suffix}"
    return f"<li>{html.escape(text)}</li>"


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


def _aliyun_bss_client():
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


def _fetch_aliyun_bill_rows(client, bss_models, billing_date):
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


def _aws_ce_client():
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


def _fetch_volcengine_bill_daily(
    billing_date, *, limit: int = 100, ignore_zero: int = 0, verbose: bool = False
):
    headers = _volcengine_headers()
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
    currencies = set()
    token_total = 0
    token_input = 0
    token_output = 0
    token_rows = []
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
        "currency": currency,
        "rows": len(rows),
        "total": total if isinstance(total, int) else None,
        "token_total": token_total,
        "token_input": token_input,
        "token_output": token_output,
        "token_rows": token_rows,
    }
    return rows, summary


def _fetch_aws_bill_daily(client, billing_date, *, include_raw: bool = False):
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


def _parse_cookie_header(cookie_header: str) -> dict:
    out = {}
    for part in cookie_header.split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        k, v = part.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def _bj_date_from_ms(ms: int | float | str | None) -> str | None:
    if ms is None:
        return None
    try:
        ts = int(ms) / 1000
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp(ts, tz=BJ).date().isoformat()


def _moonshot_headers(bearer_token: str, cookie: str | None = None) -> dict:
    # 自动去掉 "Bearer " 前缀（如果用户带上了）
    token = bearer_token.strip()
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    headers = {
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Authorization": f"Bearer {token}",
        "Referer": "https://platform.moonshot.cn/console/fee-detail",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    }
    if cookie:
        headers["Cookie"] = cookie
    return headers


def _fetch_moonshot_daily_bills(
    bearer_token: str,
    org_id: str,
    start_day: str,
    end_day: str,
    cookie: str | None = None,
) -> list[dict]:
    start_date = datetime.fromisoformat(start_day).replace(hour=0, minute=0, second=0, tzinfo=BJ)
    end_date = datetime.fromisoformat(end_day).replace(hour=23, minute=59, second=59, tzinfo=BJ)
    start_ms = int(start_date.timestamp() * 1000)
    end_ms = int(end_date.timestamp() * 1000)
    params = {
        "start": start_ms,
        "end": end_ms,
        "pid": "",
        "oid": org_id,
        "endpoint": "organizationDailyBills",
    }
    headers = _moonshot_headers(bearer_token, cookie=cookie)
    resp = requests.get(KIMI_API, params=params, headers=headers, timeout=20)
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("code") != 0:
        raise RuntimeError(payload.get("message") or f"moonshot api error: code={payload.get('code')}")
    return payload.get("data") or []


# ==================== TextIn ====================

def _textin_headers(token: str) -> dict:
    return {
        "Accept": "application/json",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Cache-Control": "no-cache",
        "Content-Type": "application/json;charset=UTF-8",
        "Origin": "https://www.textin.com",
        "Pragma": "no-cache",
        "Referer": "https://www.textin.com/",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "Token": token,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    }


def _fetch_textin_consume(
    token: str,
    start_day: str,
    end_day: str,
) -> list[dict]:
    """
    获取 TextIn 消费记录
    返回 item 列表，每个 item 包含 time (Unix秒) 和 t_coin (消费金额)
    """
    start_date = datetime.fromisoformat(start_day).replace(hour=0, minute=0, second=0, tzinfo=BJ)
    end_date = datetime.fromisoformat(end_day).replace(hour=23, minute=59, second=59, tzinfo=BJ)
    start_ts = int(start_date.timestamp())
    end_ts = int(end_date.timestamp())

    headers = _textin_headers(token)
    all_items = []
    page_num = 1
    page_size = 50

    while True:
        body = {
            "page_num": page_num,
            "page_size": page_size,
            "start_time": start_ts,
            "end_time": end_ts,
        }
        resp = requests.post(TEXTIN_API, json=body, headers=headers, timeout=20)
        resp.raise_for_status()
        payload = resp.json()

        if payload.get("msg") != "success":
            raise RuntimeError(payload.get("msg") or f"textin api error")

        data = payload.get("data") or {}
        items = data.get("item") or []
        if not items:
            break

        all_items.extend(items)

        # 如果返回的数量小于 page_size，说明没有更多了
        if len(items) < page_size:
            break
        page_num += 1

    return all_items


def _aggregate_textin_daily(items: list[dict]) -> dict[str, dict]:
    """
    将 TextIn 消费记录按天聚合
    返回 {日期: {amount, gross, currency, raw}}
    """
    daily = {}
    for item in items:
        ts = int(item.get("time") or 0)
        if ts == 0:
            continue
        # 转换为北京时间日期
        dt = datetime.fromtimestamp(ts, tz=BJ)
        date_str = dt.strftime("%Y-%m-%d")
        t_coin = float(item.get("t_coin") or 0)

        if date_str not in daily:
            daily[date_str] = {
                "amount": 0.0,
                "gross": 0.0,
                "currency": "CNY",
                "raw": [],
            }
        daily[date_str]["amount"] += t_coin
        daily[date_str]["gross"] += t_coin
        daily[date_str]["raw"].append(item)

    return daily


def _tianyancha_headers(cookie: str | None = None) -> dict:
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Connection": "keep-alive",
        "Referer": "https://open.tianyancha.com/console/data_order",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    }
    if cookie:
        headers["Cookie"] = cookie
    return headers


def _extract_tianyancha_list(payload: dict) -> list[dict]:
    data = payload.get("data")
    if isinstance(data, dict):
        for key in ("list", "items", "records", "rows"):
            if isinstance(data.get(key), list):
                return data.get(key) or []
    if isinstance(data, list):
        return data
    for key in ("list", "items", "records", "rows"):
        if isinstance(payload.get(key), list):
            return payload.get(key) or []
    return []


def _tianyancha_raise_if_error(payload: dict):
    if payload.get("success") is False:
        raise RuntimeError(payload.get("message") or "tianyancha api error")
    state = payload.get("state")
    if state == "ok":
        return  # 天眼查成功返回 state=ok
    code = payload.get("code")
    if code not in (None, 0, "0", 200, "200", 2000, "2000"):
        raise RuntimeError(payload.get("message") or f"tianyancha api error: code={code}")


def _fetch_tianyancha_orders(
    auth_secret: str,
    start_day: str,
    end_day: str,
    *,
    page_size: int = 50,
    cookie: str | None = None,
) -> list[dict]:
    page_num = 1
    results: list[dict] = []
    start_date = datetime.fromisoformat(start_day).date()
    end_date = datetime.fromisoformat(end_day).date()
    while True:
        params = {"pn": page_num, "ps": page_size, "billingMode": 0, "authSecret": auth_secret}
        try:
            headers = _tianyancha_headers(cookie=cookie)
            resp = requests.get(
                TIANYANCHA_API,
                params=params,
                headers=headers,
                timeout=20,
            )
            resp.raise_for_status()
        except requests.HTTPError as exc:
            message = ""
            if exc.response is not None:
                message = exc.response.text or ""
            raise RuntimeError(f"{exc} {message}".strip()) from exc
        payload = resp.json()
        _tianyancha_raise_if_error(payload)
        items = _extract_tianyancha_list(payload)
        if not items:
            break
        for item in items:
            detail = item.get("orderDetail") or item.get("orderDesc")
            if isinstance(detail, str):
                try:
                    detail = json.loads(detail)
                except json.JSONDecodeError:
                    detail = None
            item["_parsed_detail"] = detail
            usage_day = None
            if isinstance(detail, dict) and detail.get("startTime"):
                usage_day = _bj_date_from_ms(detail.get("startTime"))
            if not usage_day:
                usage_day = _bj_date_from_ms(item.get("createTime"))
                if usage_day:
                    usage_day = (
                        datetime.fromisoformat(usage_day) - timedelta(days=1)
                    ).date().isoformat()
            if not usage_day:
                continue
            item["_usage_day"] = usage_day
            usage_date = datetime.fromisoformat(usage_day).date()
            if usage_date < start_date or usage_date > end_date:
                continue
            results.append(item)
        if len(items) < page_size:
            break
        page_num += 1
        if page_num > 200:
            break
    return results


def _aggregate_tianyancha_daily(orders: list[dict]) -> dict[str, dict]:
    daily = {}
    for item in orders:
        detail = item.get("_parsed_detail")
        usage_day = item.get("_usage_day")
        if not usage_day:
            if isinstance(detail, dict) and detail.get("startTime"):
                usage_day = _bj_date_from_ms(detail.get("startTime"))
            if not usage_day:
                usage_day = _bj_date_from_ms(item.get("createTime"))
                if usage_day:
                    usage_day = (
                        datetime.fromisoformat(usage_day) - timedelta(days=1)
                    ).date().isoformat()
        if not usage_day:
            continue
        cents_total = 0
        if isinstance(detail, dict):
            counts = detail.get("iCountList") or []
            if isinstance(counts, list):
                for c in counts:
                    cents_total += int(c.get("cost") or 0)
        if cents_total <= 0:
            cents_total = abs(int(item.get("cost") or 0))
        amount = cents_total / 100
        currency = item.get("currency") or "CNY"
        if usage_day not in daily:
            daily[usage_day] = {
                "amount": 0.0,
                "gross": 0.0,
                "currency": currency,
                "rows": [],
            }
        daily[usage_day]["amount"] += amount
        daily[usage_day]["gross"] += amount
        daily[usage_day]["rows"].append(item)
    return daily


def _parse_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_amount(value: float, *, threshold: float = 0.005) -> float:
    if value is None:
        return 0.0
    try:
        val = float(value)
    except (TypeError, ValueError):
        return 0.0
    if abs(val) < threshold:
        return 0.0
    return round(val, 2)


def _safe_float(value):
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


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
    if r.status_code != 200:
        print("HTTP", r.status_code)
        try:
            print("Response:", r.text[:500])
        except Exception:
            pass
        r.raise_for_status()
    return r.json()


def _sum_stepfun_metrics(records: list[dict]) -> dict:
    total_input = 0
    total_output = 0
    total_tokens = 0
    total_cache = 0
    total_image = 0
    total_websearch = 0
    total_tts = 0
    total_asr = 0
    total_cost = 0.0
    cost_keys = _stepfun_cost_keys()
    cost_hits = 0

    for r in records:
        total_input += int(r.get("inAmount", "0") or 0)
        total_output += int(r.get("outAmount", "0") or 0)
        total_tokens += int(r.get("amount", "0") or 0)
        total_cache += int(r.get("cacheAmount", "0") or 0)
        total_image += int(r.get("imageCount", "0") or 0)
        total_websearch += int(r.get("websearchCount", "0") or 0)
        total_tts += int(r.get("ttsWordCount", "0") or 0)
        total_asr += int(r.get("asrDurationSeconds", 0) or 0)
        cost_value = _first_cost_value(r, cost_keys)
        if cost_value is not None:
            total_cost += cost_value
            cost_hits += 1

    return {
        "input": total_input,
        "output": total_output,
        "tokens": total_tokens,
        "cache": total_cache,
        "image": total_image,
        "websearch": total_websearch,
        "tts": total_tts,
        "asr": total_asr,
        "cost": total_cost,
        "cost_hits": cost_hits,
    }


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path not in ("/", "/index.html"):
            self.send_error(404)
            return
        default_day = yesterday_bj()
        page = render_form(default_day, default_day)
        self.send_response(200)
        self.send_header("content-type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(page.encode("utf-8"))

    def do_POST(self):
        if self.path != "/fetch":
            self.send_error(404)
            return
        raw = read_body(self)
        form = parse_qs(raw.decode("utf-8"))
        vendor = (form.get("vendor") or ["bailian"])[0].strip() or "bailian"
        cookie = (form.get("cookie") or [""])[0].strip()
        auth_secret = (form.get("auth_secret") or [""])[0].strip()
        tian_cookie = (form.get("tian_cookie") or [""])[0].strip()
        moonshot_token = (form.get("moonshot_token") or [""])[0].strip()
        moonshot_org_id = (form.get("moonshot_org_id") or [""])[0].strip()
        moonshot_cookie = (form.get("moonshot_cookie") or [""])[0].strip()
        textin_token = (form.get("textin_token") or [""])[0].strip()
        start_day = (form.get("start_day") or [""])[0].strip()
        end_day = (form.get("end_day") or [""])[0].strip()
        retry_day = (form.get("retry_day") or [""])[0].strip()
        aws_dump_raw = (form.get("aws_dump_raw") or [""])[0].strip() == "1"

        if vendor in ("bailian", "stepfun") and not cookie:
            self.send_error(400, "cookie is required")
            return
        if vendor == "tianyancha_bill":
            auth_secret = auth_secret or os.getenv("TIANYANCHA_AUTH_SECRET", "").strip()
            if not auth_secret:
                send_plain_error(self, 400, "missing authSecret (form or env: TIANYANCHA_AUTH_SECRET)")
                return
        if vendor == "moonshot_bill":
            moonshot_token = moonshot_token or os.getenv("MOONSHOT_BEARER_TOKEN", "").strip()
            moonshot_org_id = moonshot_org_id or os.getenv("MOONSHOT_ORG_ID", "").strip()
            moonshot_cookie = moonshot_cookie or os.getenv("MOONSHOT_COOKIE", "").strip()
            if not moonshot_token or not moonshot_org_id:
                send_plain_error(self, 400, "missing moonshot token or org_id")
                return
        if vendor == "textin_bill":
            textin_token = textin_token or os.getenv("TEXTIN_TOKEN", "").strip()
            if not textin_token:
                send_plain_error(self, 400, "missing TextIn token (form or env: TEXTIN_TOKEN)")
                return
        if not start_day or not end_day:
            self.send_error(400, "start_day/end_day are required")
            return

        sb = supabase_client()
        results = []
        write_summary = os.getenv("BAILIAN_WRITE_SUMMARY", "1").strip() == "1"
        if retry_day == "1":
            day_list = [start_day]
        else:
            day_list = list(iter_days(start_day, end_day))

        bill_client = None
        bill_models = None
        aws_client = None
        volc_ready = False
        tian_orders = None
        tian_daily = None
        if vendor == "aliyun_bill":
            try:
                bill_client, bill_models = _aliyun_bss_client()
            except RuntimeError as exc:
                self.send_error(400, str(exc))
                return
        if vendor == "aws_bill":
            try:
                aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
                aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
                if not aws_access_key_id or not aws_secret_access_key:
                    self.send_error(400, "missing env: AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY")
                    return
                aws_client = _aws_ce_client()
            except RuntimeError as exc:
                self.send_error(400, str(exc))
                return
        if vendor == "volcengine_bill":
            try:
                _volcengine_headers()
                volc_ready = True
            except RuntimeError as exc:
                self.send_error(400, str(exc))
                return
        if vendor == "tianyancha_bill":
            tian_cookie = (tian_cookie or os.getenv("TIANYANCHA_COOKIE", "")).strip() or None
            try:
                tian_orders = _fetch_tianyancha_orders(
                    auth_secret,
                    start_day,
                    end_day,
                    cookie=tian_cookie,
                )
                tian_daily = _aggregate_tianyancha_daily(tian_orders)
            except Exception as exc:
                send_plain_error(self, 400, f"tianyancha fetch failed: {exc}")
                return
        moonshot_daily = None
        if vendor == "moonshot_bill":
            try:
                moonshot_bills = _fetch_moonshot_daily_bills(
                    moonshot_token,
                    moonshot_org_id,
                    start_day,
                    end_day,
                    cookie=moonshot_cookie or None,
                )
                # 按日期建立映射
                moonshot_daily = {}
                for item in moonshot_bills:
                    date_str = item.get("date", "")[:10]  # "2025-12-31T00:00:00+08:00" -> "2025-12-31"
                    recharge_fee = int(item.get("recharge_fee") or 0)
                    voucher_fee = int(item.get("voucher_fee") or 0)
                    # recharge_fee 单位是 1/100000 元，转元
                    amount = recharge_fee / 100000
                    moonshot_daily[date_str] = {
                        "amount": amount,
                        "gross": amount,
                        "voucher": voucher_fee / 100000,
                        "currency": "CNY",
                        "raw": item,
                    }
            except Exception as exc:
                send_plain_error(self, 400, f"moonshot fetch failed: {exc}")
                return

        textin_daily = None
        if vendor == "textin_bill":
            try:
                textin_items = _fetch_textin_consume(textin_token, start_day, end_day)
                textin_daily = _aggregate_textin_daily(textin_items)
            except Exception as exc:
                send_plain_error(self, 400, f"textin fetch failed: {exc}")
                return

        for day in day_list:
            day_date = datetime.fromisoformat(day).date()
            start_ms, end_ms = bj_day_range(day_date)

            if vendor == "stepfun":
                if "STEPFUN_OASIS_APPID" not in os.environ or "STEPFUN_OASIS_WEBID" not in os.environ:
                    self.send_error(400, "missing env: STEPFUN_OASIS_APPID/STEPFUN_OASIS_WEBID")
                    return
                data = fetch_stepfun_usage(cookie, start_ms, end_ms)
                records = data.get("records", []) or []
                metrics = _sum_stepfun_metrics(records)
                total_tokens = metrics["tokens"]
                cost_total = metrics["cost"]
                cost_currency = os.getenv("STEPFUN_COST_CURRENCY", "CNY")
                project_id = os.getenv("STEPFUN_PROJECT_ID") or None

                row = {
                    "day": day,
                    "vendor": "stepfun",
                    "model_id": "total",
                    "project_id": project_id,
                    "input_tokens": metrics["input"],
                    "output_tokens": metrics["output"],
                    "cache_tokens": metrics["cache"],
                    "total_tokens": total_tokens,
                    "request_count": 0,
                    "image_count": metrics["image"],
                    "websearch_count": metrics["websearch"],
                    "tts_word_count": metrics["tts"],
                    "asr_duration_seconds": metrics["asr"],
                    "extra_metrics": {
                        "records_count": len(records),
                        "fromTime": start_ms,
                        "toTime": end_ms,
                        "cost_amount": cost_total,
                        "cost_currency": cost_currency,
                        "cost_hits": metrics["cost_hits"],
                    },
                    "raw": records,
                    "remark": "stepfun aggregated by day (web cookie)",
                    "updated_at": datetime.now(tz=timezone.utc).isoformat(),
                }
                delete_existing_daily(sb, day, "stepfun", project_id)
                insert_daily_row(sb, row)
                print(f"[OK] stepfun day={day} total_tokens={total_tokens} cost={cost_total}")
                results.append(
                    {
                        "day": day,
                        "total_tokens": total_tokens,
                        "total_cost": cost_total,
                        "cost_currency": cost_currency,
                        "raw": records,
                    }
                )
                if cost_total or metrics["cost_hits"] > 0:
                    upsert_bill_daily_summary(
                        sb,
                        "stepfun",
                        day,
                        _normalize_amount(cost_total),
                        _normalize_amount(cost_total),
                        cost_currency,
                        is_ai_cost=True,
                    )
                if write_summary:
                    try:
                        week_start, week_end = week_bounds(day_date)
                        weekly_total = sum_token_daily(sb, "stepfun", week_start.isoformat(), week_end.isoformat())
                        if weekly_total >= 0:
                            upsert_weekly_with_id(sb, "stepfun", week_start.isoformat(), week_end.isoformat(), weekly_total)

                        month_str = day_date.strftime("%Y-%m")
                        month_start = day_date.replace(day=1).isoformat()
                        month_end = (day_date.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
                        monthly_total = sum_token_daily(sb, "stepfun", month_start, month_end.isoformat())
                        if monthly_total >= 0:
                            upsert_monthly_with_id(sb, "stepfun", month_str, monthly_total)
                    except APIError as exc:
                        print(f"[WARN] summary upsert failed: {exc}")
            elif vendor == "aliyun_bill":
                rows, summary = _fetch_aliyun_bill_rows(bill_client, bill_models, day_date)
                if rows:
                    delete_aliyun_bill_daily(sb, day)
                    try:
                        sb.schema("financial_hub_prod").table("aliyun_bill_daily").upsert(rows).execute()
                        print(f"[OK] aliyun bill day={day} rows={len(rows)} amount={summary['amount']}")
                    except APIError as exc:
                        if _is_missing_gross_error(exc):
                            rows_without_gross = _strip_pretax_gross(rows)
                            sb.schema("financial_hub_prod").table("aliyun_bill_daily").upsert(rows_without_gross).execute()
                            summary["gross"] = None
                            print(
                                f"[WARN] aliyun bill day={day} missing pretax_gross_amount column; gross skipped"
                            )
                        else:
                            raise
                else:
                    print(f"[WARN] aliyun bill day={day} rows=0")
                if rows:
                    agg = {}
                    for row in rows:
                        is_ai = bool(row.get("is_ai_cost"))
                        amount = float(row.get("pretax_amount") or 0)
                        gross = float(row.get("pretax_gross_amount") or row.get("pretax_amount") or 0)
                        currency = row.get("currency") or "CNY"
                        if is_ai not in agg:
                            agg[is_ai] = {"amount": 0.0, "gross": 0.0, "currencies": set()}
                        agg[is_ai]["amount"] += amount
                        agg[is_ai]["gross"] += gross
                        agg[is_ai]["currencies"].add(currency)
                    for is_ai_cost, info in agg.items():
                        currencies = info["currencies"]
                        if len(currencies) == 1:
                            currency = next(iter(currencies))
                        elif len(currencies) > 1:
                            currency = "MIXED"
                        else:
                            currency = "CNY"
                        upsert_bill_daily_summary(
                            sb,
                            "aliyun",
                            day,
                            _normalize_amount(info["amount"]),
                            _normalize_amount(info["gross"]),
                            currency,
                            is_ai_cost=is_ai_cost,
                        )
                results.append(
                    {
                        "day": day,
                        "bill_amount": summary["amount"],
                        "bill_gross_amount": summary["gross"],
                        "bill_currency": summary["currency"],
                        "raw": rows,
                    }
                )
            elif vendor == "aws_bill":
                summary = _fetch_aws_bill_daily(aws_client, day_date, include_raw=aws_dump_raw)
                print(
                    f"[OK] aws bill day={day} net={summary['amount']} gross={summary['gross']} currency={summary['currency']}"
                )
                if summary.get("record_type_totals"):
                    print(
                        f"[OK] aws record_type usage={summary.get('usage_amount')} credit={summary.get('credit_amount')}"
                    )
                raw_for_page = summary.get("raw_response") if aws_dump_raw else summary
                upsert_bill_daily_summary(
                    sb,
                    "aws",
                    day,
                    summary["amount"],
                    summary["gross"],
                    summary["currency"],
                    is_ai_cost=False,
                )
                if write_summary:
                    week_start, week_end = week_bounds(day_date)
                    weekly = sum_bill_daily(sb, "aws", False, week_start.isoformat(), week_end.isoformat())
                    if weekly:
                        upsert_bill_weekly_summary(
                            sb,
                            "aws",
                            week_start.isoformat(),
                            week_end.isoformat(),
                            weekly["amount"],
                            weekly["gross"],
                            weekly["currency"],
                            is_ai_cost=False,
                        )
                    month_str = day_date.strftime("%Y-%m")
                    month_start = day_date.replace(day=1).isoformat()
                    month_end = (day_date.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
                    monthly = sum_bill_daily(sb, "aws", False, month_start, month_end.isoformat())
                    if monthly:
                        upsert_bill_monthly_summary(
                            sb,
                            "aws",
                            month_str,
                            monthly["amount"],
                            monthly["gross"],
                            monthly["currency"],
                            is_ai_cost=False,
                        )
                results.append(
                    {
                        "day": day,
                        "bill_amount": summary["amount"],
                        "bill_gross_amount": summary["gross"],
                        "bill_currency": summary["currency"],
                        "raw": raw_for_page,
                    }
                )
            elif vendor == "volcengine_bill":
                if not volc_ready:
                    self.send_error(400, "volcengine client not ready")
                    return
                rows, summary = _fetch_volcengine_bill_daily(
                    day_date, ignore_zero=0, verbose=(day == end_day)
                )
                print(
                    f"[OK] volcengine bill day={day} net={summary['amount']} gross={summary['gross']} currency={summary['currency']} rows={summary['rows']}"
                )
                # 入库账单金额
                upsert_bill_daily_summary(
                    sb,
                    "volcengine",
                    day,
                    summary["amount"],
                    summary["gross"],
                    summary["currency"],
                    is_ai_cost=False,
                )
                # 入库 Token 用量（如果有）
                token_total = summary.get("token_total", 0)
                if token_total > 0:
                    token_row = {
                        "day": day,
                        "vendor": "volcengine",
                        "model_id": "doubao",
                        "project_id": None,
                        "input_tokens": summary.get("token_input", 0),
                        "output_tokens": summary.get("token_output", 0),
                        "cache_tokens": 0,
                        "total_tokens": token_total,
                        "extra_metrics": {
                            "token_rows": summary.get("token_rows", []),
                        },
                        "raw": None,
                        "remark": "from volcengine bill api (doubao)",
                        "updated_at": datetime.now(tz=timezone.utc).isoformat(),
                    }
                    delete_existing_daily(sb, day, "volcengine", None)
                    insert_daily_row(sb, token_row)
                    print(f"[OK] volcengine token day={day} total={token_total} input={summary.get('token_input', 0)} output={summary.get('token_output', 0)}")
                if write_summary:
                    week_start, week_end = week_bounds(day_date)
                    # 账单周/月汇总
                    weekly = sum_bill_daily(sb, "volcengine", False, week_start.isoformat(), week_end.isoformat())
                    if weekly:
                        upsert_bill_weekly_summary(
                            sb,
                            "volcengine",
                            week_start.isoformat(),
                            week_end.isoformat(),
                            weekly["amount"],
                            weekly["gross"],
                            weekly["currency"],
                            is_ai_cost=False,
                        )
                    month_str = day_date.strftime("%Y-%m")
                    month_start = day_date.replace(day=1).isoformat()
                    month_end = (day_date.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
                    monthly = sum_bill_daily(sb, "volcengine", False, month_start, month_end.isoformat())
                    if monthly:
                        upsert_bill_monthly_summary(
                            sb,
                            "volcengine",
                            month_str,
                            monthly["amount"],
                            monthly["gross"],
                            monthly["currency"],
                            is_ai_cost=False,
                        )
                    # Token 周/月汇总
                    if token_total > 0:
                        weekly_token = sum_token_daily(sb, "volcengine", week_start.isoformat(), week_end.isoformat())
                        if weekly_token >= 0:
                            upsert_weekly_with_id(sb, "volcengine", week_start.isoformat(), week_end.isoformat(), weekly_token)
                        monthly_token = sum_token_daily(sb, "volcengine", month_start, month_end.isoformat())
                        if monthly_token >= 0:
                            upsert_monthly_with_id(sb, "volcengine", month_str, monthly_token)
                results.append(
                    {
                        "day": day,
                        "bill_amount": summary["amount"],
                        "bill_gross_amount": summary["gross"],
                        "bill_currency": summary["currency"],
                        "total_tokens": token_total,
                        "raw": rows if aws_dump_raw else summary,
                    }
                )
            elif vendor == "tianyancha_bill":
                daily_info = (tian_daily or {}).get(day, None)
                if daily_info:
                    amount = _normalize_amount(daily_info["amount"])
                    gross = _normalize_amount(daily_info["gross"])
                    currency = daily_info["currency"]
                    raw_rows = daily_info["rows"]
                else:
                    amount = 0.0
                    gross = 0.0
                    currency = "CNY"
                    raw_rows = []
                upsert_bill_daily_summary(
                    sb,
                    "tianyancha",
                    day,
                    amount,
                    gross,
                    currency,
                    is_ai_cost=False,
                )
                print(f"[OK] tianyancha bill day={day} amount={amount} gross={gross} currency={currency}")
                if write_summary:
                    week_start, week_end = week_bounds(day_date)
                    weekly = sum_bill_daily(sb, "tianyancha", False, week_start.isoformat(), week_end.isoformat())
                    if weekly:
                        upsert_bill_weekly_summary(
                            sb,
                            "tianyancha",
                            week_start.isoformat(),
                            week_end.isoformat(),
                            weekly["amount"],
                            weekly["gross"],
                            weekly["currency"],
                            is_ai_cost=False,
                        )
                    month_str = day_date.strftime("%Y-%m")
                    month_start = day_date.replace(day=1).isoformat()
                    month_end = (day_date.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
                    monthly = sum_bill_daily(sb, "tianyancha", False, month_start, month_end.isoformat())
                    if monthly:
                        upsert_bill_monthly_summary(
                            sb,
                            "tianyancha",
                            month_str,
                            monthly["amount"],
                            monthly["gross"],
                            monthly["currency"],
                            is_ai_cost=False,
                        )
                results.append(
                    {
                        "day": day,
                        "bill_amount": amount,
                        "bill_gross_amount": gross,
                        "bill_currency": currency,
                        "raw": raw_rows,
                    }
                )
            elif vendor == "moonshot_bill":
                daily_info = (moonshot_daily or {}).get(day, None)
                if daily_info:
                    amount = _normalize_amount(daily_info["amount"])
                    gross = _normalize_amount(daily_info["gross"])
                    currency = daily_info["currency"]
                    raw_item = daily_info["raw"]
                else:
                    amount = 0.0
                    gross = 0.0
                    currency = "CNY"
                    raw_item = {}
                upsert_bill_daily_summary(
                    sb,
                    "moonshot",
                    day,
                    amount,
                    gross,
                    currency,
                    is_ai_cost=True,
                )
                print(f"[OK] moonshot bill day={day} amount={amount} gross={gross} currency={currency}")
                if write_summary:
                    week_start, week_end = week_bounds(day_date)
                    weekly = sum_bill_daily(sb, "moonshot", True, week_start.isoformat(), week_end.isoformat())
                    if weekly:
                        upsert_bill_weekly_summary(
                            sb,
                            "moonshot",
                            week_start.isoformat(),
                            week_end.isoformat(),
                            weekly["amount"],
                            weekly["gross"],
                            weekly["currency"],
                            is_ai_cost=True,
                        )
                    month_str = day_date.strftime("%Y-%m")
                    month_start = day_date.replace(day=1).isoformat()
                    month_end = (day_date.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
                    monthly = sum_bill_daily(sb, "moonshot", True, month_start, month_end.isoformat())
                    if monthly:
                        upsert_bill_monthly_summary(
                            sb,
                            "moonshot",
                            month_str,
                            monthly["amount"],
                            monthly["gross"],
                            monthly["currency"],
                            is_ai_cost=True,
                        )
                results.append(
                    {
                        "day": day,
                        "bill_amount": amount,
                        "bill_gross_amount": gross,
                        "bill_currency": currency,
                        "raw": raw_item,
                    }
                )
            elif vendor == "textin_bill":
                daily_info = (textin_daily or {}).get(day, None)
                if daily_info:
                    amount = _normalize_amount(daily_info["amount"])
                    gross = _normalize_amount(daily_info["gross"])
                    currency = daily_info["currency"]
                    raw_items = daily_info["raw"]
                else:
                    amount = 0.0
                    gross = 0.0
                    currency = "CNY"
                    raw_items = []
                upsert_bill_daily_summary(
                    sb,
                    "textin",
                    day,
                    amount,
                    gross,
                    currency,
                    is_ai_cost=False,
                )
                print(f"[OK] textin bill day={day} amount={amount} gross={gross} currency={currency}")
                if write_summary:
                    week_start, week_end = week_bounds(day_date)
                    weekly = sum_bill_daily(sb, "textin", False, week_start.isoformat(), week_end.isoformat())
                    if weekly:
                        upsert_bill_weekly_summary(
                            sb,
                            "textin",
                            week_start.isoformat(),
                            week_end.isoformat(),
                            weekly["amount"],
                            weekly["gross"],
                            weekly["currency"],
                            is_ai_cost=False,
                        )
                    month_str = day_date.strftime("%Y-%m")
                    month_start = day_date.replace(day=1).isoformat()
                    month_end = (day_date.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
                    monthly = sum_bill_daily(sb, "textin", False, month_start, month_end.isoformat())
                    if monthly:
                        upsert_bill_monthly_summary(
                            sb,
                            "textin",
                            month_str,
                            monthly["amount"],
                            monthly["gross"],
                            monthly["currency"],
                            is_ai_cost=False,
                        )
                results.append(
                    {
                        "day": day,
                        "bill_amount": amount,
                        "bill_gross_amount": gross,
                        "bill_currency": currency,
                        "raw": raw_items,
                    }
                )
            else:
                workspace_id = os.getenv("ALIYUN_BAILIAN_WORKSPACE_ID", "")
                region = os.getenv("ALIYUN_BAILIAN_REGION", "")
                url = os.getenv("ALIYUN_BAILIAN_USAGE_URL") or None
                sec_token = os.getenv("ALIYUN_BAILIAN_SEC_TOKEN") or None
                csrf_token = os.getenv("ALIYUN_BAILIAN_CSRF_TOKEN") or None
                if not workspace_id or not region or not url:
                    self.send_error(400, "missing env: WORKSPACE_ID/REGION/USAGE_URL")
                    return

                data = fetch_usage(
                    workspace_id,
                    start_ms,
                    end_ms,
                    url=url,
                    cookie=cookie,
                    sec_token=sec_token,
                    csrf_token=csrf_token,
                    region=region,
                )
                total_tokens = parse_total_tokens(data)
                print(f"[OK] bailian day={day} workspace={workspace_id} total_tokens={total_tokens}")

                row = {
                    "day": day,
                    "vendor": "aliyun",
                    "model_id": "total",
                    "project_id": workspace_id,
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "cache_tokens": 0,
                    "total_tokens": total_tokens,
                    "raw": data,
                    "remark": "from bailian web usage-statistic",
                    "updated_at": datetime.now(tz=timezone.utc).isoformat(),
                }
                delete_existing_daily(sb, day, "aliyun", workspace_id)
                insert_daily_row(sb, row)

                weekly_total = None
                monthly_total = None
                if write_summary:
                    try:
                        week_start, week_end = week_bounds(day_date)
                        weekly_total = sum_token_daily(sb, "aliyun", week_start.isoformat(), week_end.isoformat())
                        if weekly_total >= 0:
                            upsert_weekly_with_id(sb, "aliyun", week_start.isoformat(), week_end.isoformat(), weekly_total)

                        month_str = day_date.strftime("%Y-%m")
                        month_start = day_date.replace(day=1).isoformat()
                        month_end = (day_date.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
                        monthly_total = sum_token_daily(sb, "aliyun", month_start, month_end.isoformat())
                        if monthly_total >= 0:
                            upsert_monthly_with_id(sb, "aliyun", month_str, monthly_total)
                    except APIError as exc:
                        print(f"[WARN] summary upsert failed: {exc}")

                if write_summary:
                    print(f"[OK] supabase day={day} weekly={weekly_total} monthly={monthly_total}")
                else:
                    print(f"[OK] supabase day={day} (summary skipped)")
                results.append({"day": day, "total_tokens": total_tokens, "raw": data})

        page = render_result(start_day, end_day, results)
        self.send_response(200)
        self.send_header("content-type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(page.encode("utf-8"))
        threading.Thread(target=_shutdown_server, args=(self.server,), daemon=True).start()


def main():
    host = os.getenv("BAILIAN_WEB_HOST", "127.0.0.1")
    port = int(os.getenv("BAILIAN_WEB_PORT", "8000"))
    server = ThreadingHTTPServer((host, port), Handler)
    url = f"http://{host}:{port}"
    print(f"[OK] Bailian web running at {url}")
    try:
        webbrowser.open(url)
    except Exception:
        pass
    server.serve_forever()


def _shutdown_server(server: ThreadingHTTPServer):
    time.sleep(0.2)
    server.shutdown()


if __name__ == "__main__":
    main()
