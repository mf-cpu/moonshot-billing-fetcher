import json
import os
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

import requests
from dotenv import load_dotenv
from supabase import create_client

BJ = timezone(timedelta(hours=8))


def must_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing environment variable: {name}")
    return value


def to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def bj_day_range(day_date):
    start = datetime(day_date.year, day_date.month, day_date.day, 0, 0, 0, 0, tzinfo=BJ)
    end = start + timedelta(days=1)
    return to_ms(start), to_ms(end)


def build_params_json(workspace_id: str, start_ms: int, end_ms: int) -> str:
    params = {
        "Api": "zeldaEasy.bailian-telemetry.model.getModelUsageStatistic",
        "V": "1.0",
        "Data": {
            "reqDTO": {
                "filterWorkspaceId": workspace_id,
                "modelCallSource": os.getenv("ALIYUN_BAILIAN_MODEL_CALL_SOURCE", "Online"),
                "startTime": start_ms,
                "endTime": end_ms,
                "enableAsync": True,
            },
            "cornerstoneParam": {},
        },
    }

    extra_cornerstone = os.getenv("ALIYUN_BAILIAN_CORNERSTONE_JSON")
    if extra_cornerstone:
        try:
            params["Data"]["cornerstoneParam"] = json.loads(extra_cornerstone)
        except json.JSONDecodeError as exc:
            raise RuntimeError("Invalid ALIYUN_BAILIAN_CORNERSTONE_JSON") from exc

    return json.dumps(params, ensure_ascii=False, separators=(",", ":"))


def fetch_usage(
    workspace_id: str,
    start_ms: int,
    end_ms: int,
    *,
    url: str | None = None,
    cookie: str | None = None,
    sec_token: str | None = None,
    csrf_token: str | None = None,
    region: str | None = None,
) -> dict:
    url = url or must_env("ALIYUN_BAILIAN_USAGE_URL")
    cookie = cookie or must_env("ALIYUN_BAILIAN_COOKIE")
    sec_token = sec_token or os.getenv("ALIYUN_BAILIAN_SEC_TOKEN")
    csrf_token = csrf_token or os.getenv("ALIYUN_BAILIAN_CSRF_TOKEN")

    params_str = build_params_json(workspace_id, start_ms, end_ms)
    headers = {
        "accept": "application/json, text/plain, */*",
        "content-type": "application/x-www-form-urlencoded;charset=UTF-8",
        "origin": "https://bailian.console.aliyun.com",
        "referer": "https://bailian.console.aliyun.com/",
        "user-agent": "Mozilla/5.0",
        "cookie": cookie,
    }
    if csrf_token:
        headers["x-csrf-token"] = csrf_token

    payload = {
        "params": params_str,
        "region": region or os.getenv("ALIYUN_BAILIAN_REGION", "cn-beijing"),
        "sec_token": sec_token or "",
    }
    r = requests.post(url, headers=headers, data=urlencode(payload), timeout=30)
    if r.status_code != 200:
        print("HTTP", r.status_code)
        print("Response:", r.text[:500])
        r.raise_for_status()
    return r.json()


def _find_usages(obj):
    if isinstance(obj, dict):
        if isinstance(obj.get("usages"), list):
            return obj.get("usages")
        for value in obj.values():
            found = _find_usages(value)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = _find_usages(item)
            if found is not None:
                return found
    return None


def parse_total_tokens(data: dict) -> int:
    usages = _find_usages(data) or []
    for item in usages:
        if (item.get("key") or "").strip().lower() == "total_token":
            try:
                return int(float(item.get("value") or 0))
            except (TypeError, ValueError):
                return 0
    return 0


def supabase_client():
    return create_client(
        must_env("SUPABASE_URL"),
        must_env("SUPABASE_SERVICE_ROLE_KEY"),
    )


def delete_existing(sb, day_str: str, vendor: str, project_id: str):
    return (
        sb.schema("financial_hub_prod")
        .table("llm_token_daily_usage")
        .delete()
        .eq("day", day_str)
        .eq("vendor", vendor)
        .eq("project_id", project_id)
        .execute()
    )


def insert_daily_row(sb, row: dict):
    return (
        sb.schema("financial_hub_prod")
        .table("llm_token_daily_usage")
        .insert(row)
        .execute()
    )


def sum_token_daily(sb, vendor: str, start_day: str, end_day: str) -> int:
    resp = (
        sb.schema("financial_hub_prod")
        .table("llm_token_daily_usage")
        .select("total_tokens")
        .eq("vendor", vendor)
        .gte("day", start_day)
        .lte("day", end_day)
        .execute()
    )
    rows = resp.data or []
    if not rows:
        return -1
    return int(sum(int(r.get("total_tokens") or 0) for r in rows))


def upsert_weekly_summary(sb, vendor: str, week_start: str, week_end: str, token_total: int):
    row = {
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


def upsert_monthly_summary(sb, vendor: str, month: str, token_total: int):
    row = {
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


def week_bounds(day_date):
    week_start = day_date - timedelta(days=day_date.weekday())
    week_end = week_start + timedelta(days=6)
    return week_start, week_end


def ingest_one_day(
    day_str: str,
    workspace_id: str,
    *,
    url: str | None = None,
    cookie: str | None = None,
    sec_token: str | None = None,
    csrf_token: str | None = None,
    region: str | None = None,
):
    day_date = datetime.fromisoformat(day_str).date()
    start_ms, end_ms = bj_day_range(day_date)
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

    row = {
        "day": day_str,
        "vendor": "aliyun",
        "project_id": workspace_id,
        "input_tokens": 0,
        "output_tokens": 0,
        "cache_tokens": 0,
        "total_tokens": total_tokens,
        "raw": data,
        "remark": "from bailian web usage-statistic",
        "updated_at": datetime.now(tz=timezone.utc).isoformat(),
    }

    sb = supabase_client()
    delete_existing(sb, day_str, "aliyun", workspace_id)
    insert_daily_row(sb, row)

    week_start, week_end = week_bounds(day_date)
    weekly_total = sum_token_daily(sb, "aliyun", week_start.isoformat(), week_end.isoformat())
    if weekly_total >= 0:
        upsert_weekly_summary(sb, "aliyun", week_start.isoformat(), week_end.isoformat(), weekly_total)

    month_str = day_date.strftime("%Y-%m")
    month_start = day_date.replace(day=1).isoformat()
    month_end = (day_date.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    monthly_total = sum_token_daily(sb, "aliyun", month_start, month_end.isoformat())
    if monthly_total >= 0:
        upsert_monthly_summary(sb, "aliyun", month_str, monthly_total)

    print(f"[OK] aliyun {day_str} total_tokens={total_tokens}")
    print(f"[OK] weekly {week_start}~{week_end} token_total={weekly_total}")
    print(f"[OK] monthly {month_str} token_total={monthly_total}")


def ingest_yesterday(
    workspace_id: str,
    *,
    url: str | None = None,
    cookie: str | None = None,
    sec_token: str | None = None,
    csrf_token: str | None = None,
    region: str | None = None,
):
    yesterday = (datetime.now(tz=BJ) - timedelta(days=1)).date().isoformat()
    ingest_one_day(
        yesterday,
        workspace_id,
        url=url,
        cookie=cookie,
        sec_token=sec_token,
        csrf_token=csrf_token,
        region=region,
    )


if __name__ == "__main__":
    # Load stable configs (Supabase, etc.); temporary auth is runtime-only.
    load_dotenv()
    ws = must_env("ALIYUN_BAILIAN_WORKSPACE_ID")
    ingest_yesterday(ws)
