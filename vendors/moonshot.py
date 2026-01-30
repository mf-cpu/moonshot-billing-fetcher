"""
月之暗面 (Moonshot/Kimi) 账单 API 模块
"""

import os
from datetime import datetime, timedelta, timezone

import requests


BJ = timezone(timedelta(hours=8))
KIMI_API = os.getenv(
    "KIMI_BILL_URL",
    "https://platform.moonshot.cn/api",
)


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


def fetch_moonshot_daily_bills(
    bearer_token: str,
    org_id: str,
    start_day: str,
    end_day: str,
    cookie: str | None = None,
) -> list[dict]:
    """
    获取月之暗面日账单
    返回账单列表
    """
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
