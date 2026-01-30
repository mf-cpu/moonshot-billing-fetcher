"""
天眼查账单 API 模块
"""

import json
import os
from datetime import datetime, timedelta, timezone

import requests


BJ = timezone(timedelta(hours=8))
TIANYANCHA_API = os.getenv(
    "TIANYANCHA_BILL_URL",
    "https://open.tianyancha.com/open-admin/org/order.json",
)


def _bj_date_from_ms(ms: int | float | str | None) -> str | None:
    if ms is None:
        return None
    try:
        ts = int(ms) / 1000
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp(ts, tz=BJ).date().isoformat()


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


def fetch_tianyancha_orders(
    auth_secret: str,
    start_day: str,
    end_day: str,
    *,
    page_size: int = 50,
    cookie: str | None = None,
) -> list[dict]:
    """
    获取天眼查账单订单
    返回订单列表
    """
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


def aggregate_tianyancha_daily(orders: list[dict]) -> dict[str, dict]:
    """
    将天眼查订单按天聚合
    返回 {日期: {amount, gross, currency, rows}}
    """
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
