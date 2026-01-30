"""查询阿里云账单明细（QueryBill）示例。"""
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
    """主流程：创建客户端 -> 发起请求 -> 解析返回结果。"""
    ak = must_env("ALIYUN_ACCESS_KEY_ID")
    sk = must_env("ALIYUN_ACCESS_KEY_SECRET")
    billing_cycle = os.getenv("BILLING_CYCLE", "2025-12")

    config = open_api_models.Config(
        access_key_id=ak,
        access_key_secret=sk,
        endpoint="business.aliyuncs.com",
    )
    client = BssClient(config)

    # QueryBill：账单明细（分页）
    page_num = 1
    page_size = 20

    req = bss_models.QueryBillRequest(
        billing_cycle=billing_cycle,
        page_num=page_num,
        page_size=page_size,
        # bill_owner_id 可选：不填=当前账号下可见范围
        # product_code 可选：不填=所有产品
    )

    resp = client.query_bill(req)
    body = resp.body

    print("✅ QueryBill 调用成功")
    print("BillingCycle:", billing_cycle)
    print("RequestId:", body.request_id)

    data = body.data
    if not data:
        print("⚠️ 未返回 data")
        return

    # 注意：这里 items 也可能不是 list，我们只做安全打印
    items = getattr(data, "items", None)
    print("Data type:", type(data))
    print("Items type:", type(items))

    # 尝试把 items 转成可遍历的 Python list（兼容 SDK 对象）
    items_list = None
    if isinstance(items, list):
        items_list = items
    else:
        # 阿里云 Tea 模型对象通常可以通过 to_map() 拿到 dict
        if hasattr(items, "to_map"):
            m = items.to_map()
            # 常见结构：{"Item":[{...},{...}]}
            for k in ["item", "Item", "items", "Items"]:
                if k in m:
                    items_list = m[k]
                    break

    if not items_list:
        print("⚠️ items 无法直接解析成 list，但 API 已成功返回。")
        # 打印 items 的 map 结构，便于我们适配
        if hasattr(items, "to_map"):
            print("items.to_map keys:", list(items.to_map().keys()))
        return

    print("Items parsed count:", len(items_list))
    print("---- Show first 3 records ----")
    for i, it in enumerate(items_list[:3], start=1):
        # 只打印我们关心的金额字段 & 基础识别字段
        def g(key):
            return it.get(key) if isinstance(it, dict) else None

        print(f"[{i}] ProductName:", g("ProductName"), "ProductCode:", g("ProductCode"))
        print("    PretaxGrossAmount(优惠前):", g("PretaxGrossAmount"))
        print("    PretaxAmount(优惠后):", g("PretaxAmount"))
        print("    PaymentAmount:", g("PaymentAmount"))
        print("    Currency:", g("Currency"))
        print("    RecordID:", g("RecordID"))
        print("    BillingDate:", g("BillingDate"))
        print("    UsageStartTime:", g("UsageStartTime"), "UsageEndTime:", g("UsageEndTime"))


if __name__ == "__main__":
    main()
