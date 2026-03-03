"""
飞书周报机器人
每周五下午 5:00 自动从 Supabase 查询数据并发送运维成本周报到飞书群。

用法:
  python feishu_weekly_report.py          # 自动计算上周范围
  python feishu_weekly_report.py 2026-01-27 2026-02-02  # 指定日期范围

环境变量:
  FEISHU_WEBHOOK_URL    — 飞书群机器人 Webhook 地址（必填）
  FEISHU_WEBHOOK_SECRET — 签名密钥（可选）
  SUPABASE_URL          — Supabase 地址
  SUPABASE_SERVICE_ROLE_KEY — Supabase 密钥
  USD_TO_CNY            — 美元兑人民币汇率（默认 7.25）
"""

import hashlib
import hmac
import base64
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from supabase import create_client

# ---------- 配置 ----------
BJ = timezone(timedelta(hours=8))
USD_TO_CNY = float(os.getenv("USD_TO_CNY", "7.25"))
WEBHOOK_URL = os.getenv("FEISHU_WEBHOOK_URL", "")
WEBHOOK_SECRET = os.getenv("FEISHU_WEBHOOK_SECRET", "")

VENDOR_PURPOSE = {
    "aliyun_bailian": "人岗匹配 / JD 解析",
    "volcengine": "AI 手机",
    "stepfun": "简历解析",
    "deepseek": "小麦招聘",
    "moonshot": "月之暗面",
    "dmxapi": "推荐报告",
    "aws": "AWS 服务",
    "aliyun": "ECS 服务",
    "textin": "简历解析",
    "tianyancha": "人才 / 企业识别",
    "authing": "C 端登录系统",
}


# ---------- 工具函数 ----------
def sb_client():
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
    return create_client(url, key)


def get_last_week_range():
    """获取上周一到上周日的日期范围"""
    today = datetime.now(BJ).date()
    day_of_week = today.isoweekday()  # 1=周一, 7=周日
    last_sunday = today - timedelta(days=day_of_week)
    last_monday = last_sunday - timedelta(days=6)
    return last_monday.isoformat(), last_sunday.isoformat()


def get_month_range(date_str):
    """获取日期所在月的完整范围"""
    d = datetime.fromisoformat(date_str).date()
    start = d.replace(day=1)
    if d.month == 12:
        end = d.replace(year=d.year + 1, month=1, day=1) - timedelta(days=1)
    else:
        end = d.replace(month=d.month + 1, day=1) - timedelta(days=1)
    return start.isoformat(), end.isoformat()


def fmt_amount(v):
    """格式化金额"""
    if v is None or v == 0:
        return "—"
    return f"{v:,.2f}"


def fmt_token_yi(tokens):
    """Token 转亿"""
    yi = tokens / 1e8
    return f"{yi:.2f}" if yi >= 0.01 else f"{yi:.4f}"


# ---------- 数据查询 ----------
def query_non_ai_monthly(sb, end_date):
    """查询非 AI 月度费用"""
    m_start, m_end = get_month_range(end_date)
    month_label = end_date[:7]

    # 阿里云(非AI)
    resp = sb.schema("financial_hub_prod").table("aliyun_bill_daily") \
        .select("pretax_amount").eq("is_ai_cost", False) \
        .gte("billing_date", m_start).lte("billing_date", m_end).execute()
    aliyun = sum(float(r["pretax_amount"] or 0) for r in (resp.data or []))

    # AWS
    resp = sb.schema("financial_hub_prod").table("bill_daily_summary") \
        .select("amount,currency").eq("vendor_code", "aws") \
        .gte("billing_date", m_start).lte("billing_date", m_end).execute()
    aws_usd = sum(float(r["amount"] or 0) for r in (resp.data or []))
    aws_cny = aws_usd * USD_TO_CNY

    # 火山(非AI)
    resp = sb.schema("financial_hub_prod").table("bill_daily_summary") \
        .select("amount").eq("vendor_code", "volcengine").eq("is_ai_cost", False) \
        .gte("billing_date", m_start).lte("billing_date", m_end).execute()
    volc = sum(float(r["amount"] or 0) for r in (resp.data or []))

    # TextIn
    resp = sb.schema("financial_hub_prod").table("bill_daily_summary") \
        .select("amount").eq("vendor_code", "textin") \
        .gte("billing_date", m_start).lte("billing_date", m_end).execute()
    textin = sum(float(r["amount"] or 0) for r in (resp.data or []))

    # 天眼查
    resp = sb.schema("financial_hub_prod").table("bill_daily_summary") \
        .select("amount").eq("vendor_code", "tianyancha") \
        .gte("billing_date", m_start).lte("billing_date", m_end).execute()
    tian = sum(float(r["amount"] or 0) for r in (resp.data or []))

    # Authing
    resp = sb.schema("financial_hub_prod").table("bill_monthly_summary") \
        .select("amount").eq("vendor_code", "authing").eq("month", month_label).execute()
    authing = sum(float(r["amount"] or 0) for r in (resp.data or []))

    items = [
        {"name": "亚马逊科技（AWS）", "purpose": "AWS 服务", "amount": aws_cny, "note": f"截止当前，${fmt_amount(aws_usd)}"},
        {"name": "阿里云科技（ECS）", "purpose": "ECS 服务", "amount": aliyun, "note": "不含 AI，含退款"},
        {"name": "火山引擎", "purpose": "语音转写 / 云搜索", "amount": volc, "note": ""},
        {"name": "天眼查", "purpose": "人才 / 企业识别", "amount": tian, "note": ""},
        {"name": "Authing", "purpose": "C 端登录系统", "amount": authing, "note": "月账单"},
        {"name": "TextIn", "purpose": "简历解析", "amount": textin, "note": "月账单"},
    ]
    return items


def query_ai_weekly(sb, start_date, end_date):
    """查询 AI 周费用和 Token"""
    # Token
    resp = sb.schema("financial_hub_prod").table("llm_token_daily_usage") \
        .select("vendor,total_tokens") \
        .gte("day", start_date).lte("day", end_date).execute()
    token_map = {}
    for r in (resp.data or []):
        v = (r["vendor"] or "").lower()
        if v == "aliyun":
            v = "aliyun_bailian"
        token_map[v] = token_map.get(v, 0) + int(r["total_tokens"] or 0)

    # DMXAPI tokens
    resp = sb.schema("financial_hub_prod").table("llm_token_weekly_usage") \
        .select("token_total").eq("vendor_code", "dmxapi") \
        .gte("week_start", start_date).lte("week_end", end_date).execute()
    token_map["dmxapi"] = sum(int(r["token_total"] or 0) for r in (resp.data or []))

    # 账单
    bill_map = {}
    for vc in ["stepfun", "deepseek", "moonshot"]:
        resp = sb.schema("financial_hub_prod").table("bill_daily_summary") \
            .select("amount").eq("vendor_code", vc) \
            .gte("billing_date", start_date).lte("billing_date", end_date).execute()
        bill_map[vc] = sum(float(r["amount"] or 0) for r in (resp.data or []))

    # 阿里云百炼 (sfm)
    resp = sb.schema("financial_hub_prod").table("aliyun_bill_daily") \
        .select("pretax_amount,product_code") \
        .gte("billing_date", start_date).lte("billing_date", end_date).execute()
    bill_map["aliyun_bailian"] = sum(
        float(r["pretax_amount"] or 0) for r in (resp.data or [])
        if (r.get("product_code") or "").lower() == "sfm"
    )

    # 火山(AI)
    resp = sb.schema("financial_hub_prod").table("bill_daily_summary") \
        .select("amount").eq("vendor_code", "volcengine").eq("is_ai_cost", True) \
        .gte("billing_date", start_date).lte("billing_date", end_date).execute()
    bill_map["volcengine"] = sum(float(r["amount"] or 0) for r in (resp.data or []))

    # DMXAPI 金额
    resp = sb.schema("financial_hub_prod").table("bill_weekly_summary") \
        .select("amount").eq("vendor_code", "dmxapi") \
        .gte("week_start", start_date).lte("week_end", end_date).execute()
    bill_map["dmxapi"] = sum(float(r["amount"] or 0) for r in (resp.data or []))

    items = [
        {"name": "通义千问（阿里云）", "code": "aliyun_bailian", "tokens": token_map.get("aliyun_bailian", 0), "amount": bill_map.get("aliyun_bailian", 0), "purpose": "人岗匹配 / JD 解析"},
        {"name": "阶跃星辰", "code": "stepfun", "tokens": token_map.get("stepfun", 0), "amount": bill_map.get("stepfun", 0), "purpose": "简历解析"},
        {"name": "火山模型（开源 LLM）", "code": "volcengine", "tokens": token_map.get("volcengine", 0), "amount": bill_map.get("volcengine", 0), "purpose": "AI 手机"},
        {"name": "DeepSeek", "code": "deepseek", "tokens": token_map.get("deepseek", 0), "amount": bill_map.get("deepseek", 0), "purpose": "小麦招聘"},
        {"name": "DMXAPI-VIP", "code": "dmxapi", "tokens": token_map.get("dmxapi", 0), "amount": bill_map.get("dmxapi", 0), "purpose": "推荐报告"},
    ]
    # 月之暗面（如果有数据）
    if token_map.get("moonshot", 0) > 0 or bill_map.get("moonshot", 0) > 0:
        items.append({"name": "月之暗面", "code": "moonshot", "tokens": token_map.get("moonshot", 0), "amount": bill_map.get("moonshot", 0), "purpose": "月之暗面"})

    return items


# ---------- 飞书消息卡片 ----------
def build_feishu_card(start_date, end_date, non_ai_items, ai_items):
    """构建飞书消息卡片（interactive 格式）"""
    s_m, s_d = int(start_date[5:7]), int(start_date[8:10])
    e_m, e_d = int(end_date[5:7]), int(end_date[8:10])
    range_text = f"{s_m}.{s_d}–{e_m}.{e_d}"

    non_ai_total = sum(i["amount"] for i in non_ai_items)
    ai_total_amount = sum(i["amount"] for i in ai_items)
    ai_total_tokens = sum(i["tokens"] for i in ai_items)
    grand_total = non_ai_total + ai_total_amount

    def md(text):
        return {"tag": "div", "text": {"tag": "lark_md", "content": text}}

    def hr():
        return {"tag": "hr"}

    def note(text):
        return {"tag": "note", "elements": [{"tag": "lark_md", "content": text}]}

    # --- 非 AI 单行列表 ---
    non_ai_lines = [f"{item['name']}　**{fmt_amount(item['amount'])}** 元" for item in non_ai_items if item["amount"] > 0]

    # --- AI 单行列表 ---
    ai_lines = [f"{item['name']}　{fmt_token_yi(item['tokens'])}亿　**¥{fmt_amount(item['amount'])}**　{item['purpose']}" for item in ai_items if item["tokens"] > 0 or item["amount"] > 0]

    elements = [
        # 总览大数字
        md(f"**💰 本期成本合计**\n\n# ¥{fmt_amount(grand_total)}"),
        note(f"非 AI 运维 {fmt_amount(non_ai_total)} 元（月度累计） + AI 模型 {fmt_amount(ai_total_amount)} 元（{range_text}）"),
        hr(),

        # 非 AI
        md("**☁️ 非 AI 运维费用（月度累计）**\n\n" + "\n".join(non_ai_lines)),
        hr(),

        # AI
        md("**🤖 AI 模型费用（" + range_text + "）**\n\n" + "\n".join(ai_lines) + f"\n\nToken 合计 **{fmt_token_yi(ai_total_tokens)} 亿**"),
        hr(),

        # 按钮
        {
            "tag": "action",
            "actions": [{
                "tag": "button",
                "text": {"tag": "plain_text", "content": "查看完整数据面板"},
                "type": "primary",
                "url": "https://ops.aittc.cn",
            }],
        },
        note("数据自动生成，详细明细请查看面板"),
    ]

    payload = {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True},
            "header": {
                "template": "purple",
                "title": {"tag": "plain_text", "content": f"运维成本周报 | {range_text}"},
            },
            "elements": elements,
        },
    }
    return payload


# ---------- 发送 ----------
def gen_sign(secret, timestamp):
    """飞书签名"""
    string_to_sign = f"{timestamp}\n{secret}"
    hmac_code = hmac.new(string_to_sign.encode("utf-8"), digestmod=hashlib.sha256).digest()
    return base64.b64encode(hmac_code).decode("utf-8")


def send_to_feishu(card_payload):
    """发送消息到飞书"""
    if not WEBHOOK_URL:
        print("[ERROR] FEISHU_WEBHOOK_URL 未配置")
        return False

    if WEBHOOK_SECRET:
        timestamp = str(int(time.time()))
        sign = gen_sign(WEBHOOK_SECRET, timestamp)
        card_payload["timestamp"] = timestamp
        card_payload["sign"] = sign

    resp = requests.post(WEBHOOK_URL, json=card_payload, timeout=10)
    result = resp.json()
    if result.get("code") == 0 or result.get("StatusCode") == 0:
        print(f"[OK] 飞书消息发送成功")
        return True
    else:
        print(f"[ERROR] 飞书发送失败: {result}")
        return False


# ---------- 主流程 ----------
def main():
    # 日期范围
    if len(sys.argv) >= 3:
        start_date, end_date = sys.argv[1], sys.argv[2]
    else:
        start_date, end_date = get_last_week_range()

    print(f"[INFO] 周报范围: {start_date} ~ {end_date}")

    sb = sb_client()

    # 查询数据
    print("[INFO] 查询非 AI 月度费用...")
    non_ai_items = query_non_ai_monthly(sb, end_date)

    print("[INFO] 查询 AI 周费用...")
    ai_items = query_ai_weekly(sb, start_date, end_date)

    # 打印摘要
    non_ai_total = sum(i["amount"] for i in non_ai_items)
    ai_total = sum(i["amount"] for i in ai_items)
    print(f"[INFO] 非 AI 合计: ¥{non_ai_total:,.2f}")
    print(f"[INFO] AI 合计: ¥{ai_total:,.2f}")
    print(f"[INFO] 总计: ¥{non_ai_total + ai_total:,.2f}")

    # 构建消息卡片
    card = build_feishu_card(start_date, end_date, non_ai_items, ai_items)

    # 发送
    print("[INFO] 发送到飞书...")
    success = send_to_feishu(card)

    if success:
        print("[DONE] 周报发送完成")
    else:
        print("[FAIL] 周报发送失败")
        sys.exit(1)


if __name__ == "__main__":
    main()
