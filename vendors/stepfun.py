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
    """费用字段优先级列表，cost 单位是万分之一元"""
    override = os.getenv("STEPFUN_COST_KEYS", "").strip()
    if override:
        return [k.strip() for k in override.split(",") if k.strip()]
    return ["cost"]  # 阶跃星辰 API 返回的费用字段是 cost，单位万分之一元


def _fetch_stepfun_page(
    cookie_header: str,
    from_ms: int,
    to_ms: int,
    page: int,
    page_size: int,
    quota_type: str,
    cookies: dict,
):
    """获取单页数据"""
    headers = {
        "accept": "*/*",
        "accept-language": "zh-CN,zh;q=0.9",
        "connect-protocol-version": "1",
        "content-type": "application/json",
        "oasis-appid": os.environ["STEPFUN_OASIS_APPID"],
        "oasis-platform": os.environ.get("STEPFUN_OASIS_PLATFORM", "web"),
        "oasis-webid": os.environ["STEPFUN_OASIS_WEBID"],
        "origin": "https://platform.stepfun.com",
        "referer": "https://platform.stepfun.com/account-overview",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
    }

    payload = {
        "fromTime": str(from_ms),
        "toTime": str(to_ms),
        "pageSize": int(page_size),
        "page": int(page),
        "quotaType": str(quota_type),
    }

    r = requests.post(STEPFUN_API, headers=headers, data=json.dumps(payload), timeout=30, cookies=cookies)
    r.raise_for_status()
    return r.json()


def fetch_stepfun_usage(
    cookie_header: str,
    from_ms: int,
    to_ms: int,
    *,
    page_size: int = 100,
    quota_type: str = "1",
):
    """
    获取阶跃星辰用量数据（自动分页获取全部）
    返回 API 响应 JSON，records 包含所有页的数据
    """
    cookies = _parse_cookie_header(cookie_header)
    
    # 第一页
    print(f"[DEBUG] stepfun fetching page 1...")
    result = _fetch_stepfun_page(cookie_header, from_ms, to_ms, 1, page_size, quota_type, cookies)
    
    if result.get("status") != 1:
        print(f"[WARN] stepfun API status: {result.get('status')}, desc: {result.get('desc')}")
        return result
    
    total = int(result.get("total") or 0)
    all_records = result.get("records") or []
    
    print(f"[DEBUG] stepfun total={total}, page 1 got {len(all_records)} records")
    
    # 计算总页数并获取剩余页
    total_pages = (total + page_size - 1) // page_size
    for page in range(2, total_pages + 1):
        print(f"[DEBUG] stepfun fetching page {page}/{total_pages}...")
        page_result = _fetch_stepfun_page(cookie_header, from_ms, to_ms, page, page_size, quota_type, cookies)
        page_records = page_result.get("records") or []
        all_records.extend(page_records)
        print(f"[DEBUG] stepfun page {page} got {len(page_records)} records, total so far: {len(all_records)}")
    
    result["records"] = all_records
    print(f"[DEBUG] stepfun finished, total records: {len(all_records)}")
    return result


def sum_stepfun_metrics(records: list[dict]) -> dict:
    """
    汇总阶跃星辰指标
    返回 {tokens, cost, currency, input, output, cache, image, websearch, tts, asr, cost_hits}
    
    注意：API 返回的 cost 单位是万分之一元，需要除以 10000 转换为元
    """
    total_input = 0
    total_output = 0
    total_tokens = 0
    total_cache = 0
    total_image = 0
    total_websearch = 0
    total_tts = 0
    total_asr = 0
    total_cost_raw = 0  # 原始值（万分之一元）
    cost_hits = 0
    currency = os.getenv("STEPFUN_COST_CURRENCY", "CNY")

    print(f"[DEBUG] stepfun records count: {len(records)}")
    for i, record in enumerate(records):
        total_input += int(record.get("inAmount") or 0)
        total_output += int(record.get("outAmount") or 0)
        total_tokens += int(record.get("amount") or 0)
        total_cache += int(record.get("cacheAmount") or 0)
        total_image += int(record.get("imageCount") or 0)
        total_websearch += int(record.get("websearchCount") or 0)
        total_tts += int(record.get("ttsWordCount") or 0)
        total_asr += int(record.get("asrDurationSeconds") or 0)

        # cost 字段，单位是万分之一元
        cost_raw = record.get("cost")
        if cost_raw is not None:
            try:
                total_cost_raw += int(cost_raw)
                cost_hits += 1
            except (TypeError, ValueError):
                pass

        # 打印前3条记录的关键字段
        if i < 3:
            print(f"[DEBUG] record[{i}]: model={record.get('modelId')}, amount={record.get('amount')}, cost={record.get('cost')}")

    # 如果 total_tokens 为 0 但有 input/output，则计算总和
    if total_tokens == 0 and (total_input or total_output):
        total_tokens = total_input + total_output

    # 转换为元（除以 10000）
    total_cost_yuan = total_cost_raw / 10000.0

    print(f"[DEBUG] stepfun metrics: tokens={total_tokens}, cost_raw={total_cost_raw}, cost_yuan={total_cost_yuan:.4f}, cost_hits={cost_hits}")

    return {
        "tokens": total_tokens,
        "cost": round(total_cost_yuan, 4),  # 元
        "currency": currency,
        "input": total_input,
        "output": total_output,
        "cache": total_cache,
        "image": total_image,
        "websearch": total_websearch,
        "tts": total_tts,
        "asr": total_asr,
        "cost_hits": cost_hits,
    }
