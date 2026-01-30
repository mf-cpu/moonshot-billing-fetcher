"""
供应商 API 模块集合
每个供应商一个独立模块，便于维护和扩展
"""

from .aliyun import fetch_aliyun_bill_rows, aliyun_bss_client
from .aws import fetch_aws_bill_daily, aws_ce_client
from .volcengine import fetch_volcengine_bill_daily
from .moonshot import fetch_moonshot_daily_bills
from .textin import fetch_textin_consume, aggregate_textin_daily
from .tianyancha import fetch_tianyancha_orders, aggregate_tianyancha_daily
from .stepfun import fetch_stepfun_usage, sum_stepfun_metrics

__all__ = [
    # Aliyun
    "aliyun_bss_client",
    "fetch_aliyun_bill_rows",
    # AWS
    "aws_ce_client",
    "fetch_aws_bill_daily",
    # Volcengine
    "fetch_volcengine_bill_daily",
    # Moonshot
    "fetch_moonshot_daily_bills",
    # TextIn
    "fetch_textin_consume",
    "aggregate_textin_daily",
    # Tianyancha
    "fetch_tianyancha_orders",
    "aggregate_tianyancha_daily",
    # Stepfun
    "fetch_stepfun_usage",
    "sum_stepfun_metrics",
]
