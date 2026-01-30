"""按日粒度查询阿里云账号账单示例。"""  # 模块说明，便于快速了解用途
import os  # 读取环境变量与基础配置
from alibabacloud_bssopenapi20171214.client import Client as BssClient  # 阿里云账单 SDK 客户端
from alibabacloud_tea_openapi import models as open_api_models  # 通用 OpenAPI 配置模型
from alibabacloud_bssopenapi20171214 import models as bss_models  # 账单接口请求模型

# === AI 产品映射（你已确认：sfm=大模型服务平台百炼）===
AI_PRODUCT_SET = {  # 使用集合便于 O(1) 查询
    "sfm",  # 阿里云产品编码，用于识别 AI 相关费用
}

def is_ai_product(product_code: str) -> bool:  # 判断产品编码是否属于 AI 成本
    return (product_code or "").strip().lower() in AI_PRODUCT_SET  # 统一大小写与空值处理


def must_env(name: str) -> str:  # 获取必需环境变量，不存在则直接失败
    """读取必填环境变量，不存在则直接报错。"""  # 明确这是强依赖的配置
    value = os.getenv(name)  # 从系统环境读取变量
    if not value:  # 若为空，说明未配置
        raise RuntimeError(f"Missing environment variable: {name}")  # 立即抛错避免后续报错不清晰
    return value  # 返回读取到的值供调用方使用


def _parse_items(items):  # 解析 SDK items 为 list
    items_list = []  # 统一转换后的列表
    if isinstance(items, list):  # 如果已经是 list，直接使用
        items_list = items  # 保存原始列表
    elif hasattr(items, "to_map"):  # 如果是 Tea SDK 对象，转换为 dict
        mapped = items.to_map()  # 拿到可解析的映射结构
        for key in ["Item", "item", "Items", "items"]:  # 兼容不同字段命名
            if key in mapped:  # 找到目标字段
                value = mapped[key]  # 取出字段值
                if isinstance(value, list):  # 如果是 list，直接使用
                    items_list = value  # 保存列表
                elif isinstance(value, dict):  # 如果是 dict，包装成单元素列表
                    items_list = [value]  # 统一为 list 方便后续处理
                break  # 找到后跳出循环
    return items_list


def _fetch_daily_rows(client, billing_date):  # 获取单日账单行
    billing_cycle = billing_date.strftime("%Y-%m")  # 账期格式要求 YYYY-MM
    request = bss_models.QueryAccountBillRequest(  # 账单按日查询请求
        billing_cycle=billing_cycle,  # 账期必须提供
        billing_date=billing_date.strftime("%Y-%m-%d"),  # 指定要查询的具体日期
        granularity="DAILY",  # 按日粒度
        is_group_by_product=True,  # ⭐ 按产品拆分，便于后续汇总
        page_num=1,  # 分页页码，从 1 开始
        page_size=200,  # 每页条数，按需调整
    )

    response = client.query_account_bill(request)  # 发起接口调用
    body = response.body  # SDK 返回体主要内容在 body
    print("✅ QueryAccountBill DAILY 调用成功")  # 调用成功标识
    print("BillingCycle:", billing_cycle)  # 打印账期用于确认范围
    print("BillingDate:", billing_date.strftime("%Y-%m-%d"))  # 打印账单日期用于确认
    print("RequestId:", body.request_id)  # 打印请求 ID 便于阿里云侧排查

    data = body.data  # 账单数据主体
    if not data:  # 为空时直接退出，避免后续异常
        print("⚠️ 未返回 data")  # 输出提示信息
        return []

    items_list = _parse_items(data.items)  # 解析 items
    if not items_list:  # 未解析到条目时输出提示
        print("⚠️ 没有解析到任何账单条目")  # 便于定位解析问题
        print("items.to_map keys:", data.items.to_map().keys() if hasattr(data.items, "to_map") else None)  # 打印键名协助排查
        return []

    # ========= 过滤金额为 0 的产品 =========
    filtered_items = []  # 保存非零金额产品
    for it in items_list:  # 遍历账单条目
        pretax = float(it.get("PretaxAmount") or 0)  # 税前金额，缺失则为 0
        pretax_gross = float(it.get("PretaxGrossAmount") or 0)  # 优惠前金额，缺失则为 0
        if pretax != 0 or pretax_gross != 0:  # 只保留有实际费用的产品
            filtered_items.append(it)  # 加入过滤后的列表

    print(f"Items parsed count (after filter): {len(filtered_items)}")  # 输出过滤后数量
    print("---- Show first 10 records ----")  # 控制台只展示前 10 条
    for idx, it in enumerate(filtered_items[:10], start=1):  # 展示前 10 条的摘要
        code = it.get("ProductCode")  # 读取产品编码
        print(
            f"[{idx}]",
            "ProductCode:", code,
            "ProductName:", it.get("ProductName"),
            "is_ai_cost:", is_ai_product(code),
            "PretaxAmount:", it.get("PretaxAmount"),
            "PretaxGrossAmount:", it.get("PretaxGrossAmount"),
            "Currency:", it.get("Currency"),
        )

    # 汇总去重：避免同一主键重复导致 upsert 报错
    aggregated = {}  # key -> row
    billing_date_str = billing_date.strftime("%Y-%m-%d")
    for it in filtered_items:  # 遍历过滤后的账单条目
        product_code = it.get("ProductCode")  # 先取出 ProductCode 供多处使用
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
        gross_amount = float(it.get("PretaxGrossAmount") or 0)
        if key not in aggregated:
            aggregated[key] = {
                "billing_date": billing_date_str,
                "billing_cycle": billing_cycle,
                "product_code": product_code,
                "product_name": product_name,
                "currency": currency,
                "pretax_amount": amount,
                "pretax_gross_amount": gross_amount,
                "is_ai_cost": is_ai,
                "raw": [it],  # 保留原始记录便于追溯
            }
        else:
            aggregated[key]["pretax_amount"] += amount
            aggregated[key]["pretax_gross_amount"] += gross_amount
            aggregated[key]["raw"].append(it)

    rows = list(aggregated.values())
    if len(rows) != len(filtered_items):
        print(f"⚠️ 去重后行数: {len(rows)}，原始行数: {len(filtered_items)}")
    return rows


def _date_range(start_date, end_date):  # 生成日期区间（含首尾）
    from datetime import timedelta

    cur = start_date
    while cur <= end_date:
        yield cur
        cur += timedelta(days=1)


def main():  # 主流程入口函数
    """主流程：创建客户端 -> 批量拉取 -> 写入 Supabase。"""
    from datetime import date, timedelta  # 局部导入避免全局污染

    # ========= 读取日期范围 =========
    end_date = date.today() - timedelta(days=1)  # 默认到昨天
    days = int(os.getenv("BILLING_DAYS", "30"))  # 默认近 30 天
    start_date = end_date - timedelta(days=days - 1)

    env_start = os.getenv("BILLING_START_DATE")  # 可选：YYYY-MM-DD
    env_end = os.getenv("BILLING_END_DATE")  # 可选：YYYY-MM-DD
    if env_start and env_end:
        start_date = date.fromisoformat(env_start)
        end_date = date.fromisoformat(env_end)

    print("DEBUG today:", date.today())
    print("DEBUG range:", start_date, "->", end_date)

    # ========= 读取凭证 =========
    access_key_id = must_env("ALIYUN_ACCESS_KEY_ID")  # AK 从环境变量读取避免硬编码
    access_key_secret = must_env("ALIYUN_ACCESS_KEY_SECRET")  # SK 从环境变量读取避免泄露

    # ========= 创建客户端 =========
    config = open_api_models.Config(  # 构造 SDK 客户端配置
        access_key_id=access_key_id,  # 设置 AccessKeyId
        access_key_secret=access_key_secret,  # 设置 AccessKeySecret
        endpoint="business.aliyuncs.com",  # 账单服务固定 endpoint
    )
    client = BssClient(config)  # 用配置创建账单客户端

    # === 写入 Supabase：aliyun_bill_daily（按天&产品汇总）===
    from supabase import create_client  # 读取 supabase 客户端
    from dotenv import load_dotenv  # 加载 .env 中的密钥

    load_dotenv()  # 加载默认 .env，确保连接信息可用
    supabase = create_client(
        os.environ["SUPABASE_URL"],
        os.environ["SUPABASE_SERVICE_ROLE_KEY"],
    )

    total_rows = 0
    for day in _date_range(start_date, end_date):
        rows = _fetch_daily_rows(client, day)
        if not rows:
            print("⚠️ 当日无可写入行:", day)
            continue
        supabase.schema("financial_hub_prod").table("aliyun_bill_daily").upsert(rows).execute()
        total_rows += len(rows)
        print(f"✅ 已写入 {day} 行数: {len(rows)}")

    print(f"✅ 近区间累计写入行数: {total_rows}")


# ========= 程序入口（一定要有） =========
if __name__ == "__main__":  # 脚本直接运行时的入口
    main()  # 调用主流程
