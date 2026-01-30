import os  # 读取环境变量与系统参数所需
import json  # 负责把请求体序列化为 JSON 字符串
import requests  # 用于发起 HTTP 请求获取用量数据
from datetime import datetime, timedelta, timezone  # 处理日期范围与时区换算

from dotenv import load_dotenv  # 从 .env 加载密钥，避免硬编码
from supabase import create_client  # 连接 Supabase 进行数据写入

# 你的本地环境：强制加载 D:\dey_test\.env（避免 cwd 变化导致读不到）
load_dotenv(dotenv_path=r"D:\dey_test\.env")  # 明确路径可避免工作目录变化导致加载失败

API = "https://platform.stepfun.com/api/step.openapi.devcenter.Dashboard/DevQueryUsageHistory"  # Stepfun 用量查询接口
BJ = timezone(timedelta(hours=8))  # 固定使用北京时间，确保按天统计一致

def backfill(start_day: str, end_day: str, project_id: str | None = None):  # 批量回填多天数据
    start = datetime.fromisoformat(start_day).date()  # 起始日期转为 date，便于比较
    end = datetime.fromisoformat(end_day).date()  # 结束日期转为 date，便于循环
    cur = start  # 当前处理日期从 start 开始
    while cur <= end:  # 逐日迭代直到覆盖 end
        ingest_one_day(cur.isoformat(), project_id=project_id)  # 逐日拉取并入库
        cur += timedelta(days=1)  # 日期加一天进入下一次循环

def to_ms(dt: datetime) -> int:  # 将 datetime 转成毫秒时间戳
    return int(dt.timestamp() * 1000)  # API 需要毫秒单位，乘 1000

def bj_day_range(day_date):  # 计算北京时间的整日时间范围
    """
    day_date: datetime.date  # 输入为日期对象，避免时分秒干扰
    返回：北京时间该日 [00:00, 23:59:59.999] 的毫秒时间戳  # 供 API 查询范围使用
    """
    start = datetime(day_date.year, day_date.month, day_date.day, 0, 0, 0, 0, tzinfo=BJ)  # 当天 00:00:00.000
    end = start + timedelta(days=1) - timedelta(milliseconds=1)  # 当天 23:59:59.999
    return to_ms(start), to_ms(end)  # 返回毫秒级起止时间

def parse_cookie_header(cookie_header: str) -> dict:  # 将 Cookie 头解析为 dict
    out = {}  # 存放解析后的键值对
    for part in cookie_header.split(";"):  # 按分号拆分多个 cookie 项
        part = part.strip()  # 去掉前后空白，避免解析错误
        if not part or "=" not in part:  # 跳过空项或非法项
            continue  # 非法项不处理，保证健壮性
        k, v = part.split("=", 1)  # 只按第一个等号切分，避免值中包含等号
        out[k.strip()] = v.strip()  # 去除空白后写入 dict
    return out  # 返回可被 requests 使用的 cookies 字典


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

def fetch_usage(from_ms: int, to_ms: int, page: int = 1, page_size: int = 200, quota_type: str = "1", merge_by_time: int = 1):  # 调用 Stepfun 接口拉取用量
    headers = {  # 构造请求头，满足接口鉴权与浏览器特征
        "accept": "*/*",  # 接受任意响应类型
        "accept-language": "zh-CN,zh;q=0.9",  # 指定中文语言偏好
        "content-type": "application/json",  # 请求体为 JSON
        "oasis-appid": os.environ["STEPFUN_OASIS_APPID"],  # Stepfun 应用 ID，来自环境变量
        "oasis-platform": os.environ.get("STEPFUN_OASIS_PLATFORM", "web"),  # 平台来源，默认 web
        "oasis-webid": os.environ["STEPFUN_OASIS_WEBID"],  # Stepfun Web ID，来自环境变量
        "origin": "https://platform.stepfun.com",  # 保持与官方控制台一致的来源
        "referer": "https://platform.stepfun.com/account-overview",  # 模拟控制台页面来源
        "sec-fetch-dest": "empty",  # 浏览器安全字段，兼容接口校验
        "sec-fetch-mode": "cors",  # 跨域请求模式，符合前端请求行为
        "sec-fetch-site": "same-origin",  # 同源标识，减少被判定为异常请求的风险
        "user-agent": "Mozilla/5.0",  # 模拟常见 UA，避免被简单拦截
    }

    payload = {  # 请求体包含查询的时间范围与分页参数
        "fromTime": str(from_ms),  # 开始时间毫秒，接口要求字符串
        "toTime": str(to_ms),  # 结束时间毫秒，接口要求字符串
        "pageSize": int(page_size),  # 每页数量，确保为整数
        "page": int(page),  # 当前页号，确保为整数
        "quotaType": str(quota_type),  # 配额类型，保持与控制台一致
        "mergeByTime": int(merge_by_time),  # 是否按时间合并，按接口要求设置
    }

    cookie_header = os.environ["STEPFUN_COOKIE"]  # 从环境变量读取原始 Cookie 字符串
    cookies = parse_cookie_header(cookie_header)  # 转换成 requests 可用的 cookies 字典

    r = requests.post(API, headers=headers, data=json.dumps(payload), timeout=30, cookies=cookies)  # 发起 POST 请求

    # 关键：401 时打印返回体（通常会告诉你 token 过期/缺字段）
    if r.status_code != 200:  # 非 200 代表请求失败，需要排查
        print("HTTP", r.status_code)  # 打印状态码，便于诊断
        try:
            print("Response:", r.text[:500])  # 打印前 500 字符，避免输出过大
        except Exception:
            pass  # 避免打印失败导致异常中断
        r.raise_for_status()  # 抛出异常，让调用方感知失败

    return r.json()  # 正常情况返回 JSON 解析结果


def supabase_client():  # 创建 Supabase 客户端
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_ROLE_KEY"])


def delete_existing(sb, day_str: str, vendor: str, project_id: str | None):  # 删除同日同供应商记录
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


def upsert_token_rows(sb, rows: list[dict]):  # 写入或更新 Supabase 表数据
    # 注意：表在 financial_hub_prod schema 下
    return sb.schema("financial_hub_prod").table("llm_token_daily_usage") \
        .upsert(rows, on_conflict="day,vendor,model_id,account_key,project_key")\
        .execute()  # 通过 upsert 避免重复写入并保持幂等


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
        "currency": currency or "CNY",
    }
    return (
        sb.schema("financial_hub_prod")
        .table("bill_daily_summary")
        .upsert(row, on_conflict="vendor_code,billing_date,is_ai_cost")
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

def ingest_one_day(day_str: str, project_id: str | None = None):  # 拉取某一天的数据并写入
    """
    day_str: 'YYYY-MM-DD'（北京时间日期）  # 外部调用统一用字符串格式
    project_id: 可选，你们内部项目标识（比如 lovtalent）；没有就传 None  # 便于业务归属
    """
    day_date = datetime.fromisoformat(day_str).date()  # 把字符串日期转成 date
    from_ms, to_ms = bj_day_range(day_date)  # 得到该日北京时间的毫秒范围

    data = fetch_usage(from_ms, to_ms, page=1, page_size=200, quota_type="1", merge_by_time=1)  # 拉取当天用量
    records = data.get("records", []) or []  # 兼容 records 为空或 None 的情况

    vendor = "stepfun"  # 固定供应商标识，便于多厂商汇总

    total_input = 0
    total_output = 0
    total_tokens = 0
    total_cache = 0
    total_image = 0
    total_websearch = 0
    total_tts = 0
    total_asr = 0
    total_cost = 0.0
    cost_hits = 0
    cost_keys = _stepfun_cost_keys()

    for r in records:
        total_input += int(r.get("inAmount", "0"))
        total_output += int(r.get("outAmount", "0"))
        total_tokens += int(r.get("amount", "0"))
        total_cache += int(r.get("cacheAmount", "0"))
        total_image += int(r.get("imageCount", "0"))
        total_websearch += int(r.get("websearchCount", "0"))
        total_tts += int(r.get("ttsWordCount", "0"))
        total_asr += int(r.get("asrDurationSeconds", 0))
        cost_value = _first_cost_value(r, cost_keys)
        if cost_value is not None:
            total_cost += cost_value
            cost_hits += 1

    if not records:  # 无数据时直接返回，避免无意义写入
        print(f"[WARN] stepfun {day_str} records=0 (可能当天无用量或接口口径未返回)")  # 打印提示便于排查
        return  # 结束当前日期处理

    sb = supabase_client()
    delete_existing(sb, day_str, vendor, project_id)

    row = {
        "day": day_str,
        "vendor": vendor,
        "project_id": project_id,
        "input_tokens": total_input,
        "output_tokens": total_output,
        "cache_tokens": total_cache,
        "total_tokens": total_tokens,
        "request_count": 0,
        "image_count": total_image,
        "websearch_count": total_websearch,
        "tts_word_count": total_tts,
        "asr_duration_seconds": total_asr,
        "extra_metrics": {
            "records_count": len(records),
            "fromTime": from_ms,
            "toTime": to_ms,
            "cost_amount": total_cost,
            "cost_currency": os.getenv("STEPFUN_COST_CURRENCY", "CNY"),
            "cost_hits": cost_hits,
        },
        "raw": records,
        "remark": "stepfun aggregated by day",
        "updated_at": datetime.now(tz=timezone.utc).isoformat(),
    }

    upsert_token_rows(sb, [row])
    print(f"[OK] stepfun {day_str} total_tokens={total_tokens} records={len(records)}")
    if total_cost or cost_hits > 0:
        upsert_bill_daily_summary(
            sb,
            vendor,
            day_str,
            _normalize_amount(total_cost),
            _normalize_amount(total_cost),
            os.getenv("STEPFUN_COST_CURRENCY", "CNY"),
            is_ai_cost=True,
        )

    week_start, week_end = week_bounds(day_date)
    weekly_total = sum_token_daily(sb, vendor, week_start.isoformat(), week_end.isoformat())
    if weekly_total >= 0:
        upsert_weekly_summary(sb, vendor, week_start.isoformat(), week_end.isoformat(), weekly_total)

    month_str = day_date.strftime("%Y-%m")
    month_start = day_date.replace(day=1).isoformat()
    month_end = (day_date.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    monthly_total = sum_token_daily(sb, vendor, month_start, month_end.isoformat())
    if monthly_total >= 0:
        upsert_monthly_summary(sb, vendor, month_str, monthly_total)

    print(f"[OK] weekly {week_start}~{week_end} token_total={weekly_total}")
    print(f"[OK] monthly {month_str} token_total={monthly_total}")

def ingest_yesterday(project_id: str | None = None):  # 便捷函数：处理昨天数据
    yesterday = (datetime.now(tz=BJ) - timedelta(days=1)).date().isoformat()  # 计算北京时间的昨天日期
    ingest_one_day(yesterday, project_id=project_id)  # 复用单日处理逻辑

if __name__ == "__main__":  # 作为脚本直接运行时的入口
    ingest_yesterday(project_id=None)  # 默认拉取昨天数据

    # 日常稳定任务（先注释掉，避免干扰）
    # ingest_yesterday(project_id=None)

