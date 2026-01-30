"""
TextIn 账单 API 模块
"""

import os
from datetime import datetime, timedelta, timezone

import requests


BJ = timezone(timedelta(hours=8))
TEXTIN_API = os.getenv(
    "TEXTIN_BILL_URL",
    "https://web-api.textin.com/user/finance/consume",
)


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


def fetch_textin_consume(
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
            raise RuntimeError(payload.get("msg") or "textin api error")

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


def aggregate_textin_daily(items: list[dict]) -> dict[str, dict]:
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
