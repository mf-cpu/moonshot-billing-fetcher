"""查询阿里云账单概览（QueryBillOverview）示例。"""
import os
from alibabacloud_bssopenapi20171214.client import Client as BssClient
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_bssopenapi20171214 import models as bss_models


def must_env(name: str) -> str:
    """读取必填环境变量，不存在则直接报错。"""
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


def main():
    """主流程：创建客户端 -> 发起请求 -> 打印关键字段。"""
    ak = must_env("ALIYUN_ACCESS_KEY_ID")
    sk = must_env("ALIYUN_ACCESS_KEY_SECRET")

    billing_cycle = os.getenv("BILLING_CYCLE", "2025-12")

    config = open_api_models.Config(
        access_key_id=ak,
        access_key_secret=sk,
        endpoint="business.aliyuncs.com",
    )
    client = BssClient(config)

    req = bss_models.QueryBillOverviewRequest(
        billing_cycle=billing_cycle
    )
    resp = client.query_bill_overview(req)

    body = resp.body
    print("✅ QueryBillOverview 调用成功")
    print("BillingCycle:", billing_cycle)
    print("RequestId:", body.request_id)

    data = body.data
    if not data:
        print("⚠️ 未返回 data")
        return

    print("Data type:", type(data))
    print("Currency:", getattr(data, "currency", None))
    print("Items type:", type(getattr(data, "items", None)))


if __name__ == "__main__":
    main()
