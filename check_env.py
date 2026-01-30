"""检查并加载当前目录的 .env 配置。"""  # 模块说明，提示脚本用途
import os  # 用于读取环境变量
from pathlib import Path  # 用于拼接与检查路径

from dotenv import load_dotenv  # 读取 .env 文件到环境变量

env_path = Path(__file__).parent / ".env"  # 约定 .env 与脚本同目录，避免依赖 cwd

# 基础信息输出，便于定位 .env
print("📄 当前目录:", Path(__file__).parent)  # 打印脚本目录，确认位置
print("📄 .env 路径:", env_path)  # 打印 .env 具体路径，方便核对
print("📄 .env 是否存在:", env_path.exists())  # 打印存在性，快速判断是否缺失

# 加载 .env 到环境变量
load_dotenv(env_path)  # 指定路径加载，避免读取错位置

# 需要检查的环境变量清单
keys = [  # 明确需要检查的关键配置
    "SUPABASE_URL",  # Supabase 项目地址
    "SUPABASE_SERVICE_ROLE_KEY",  # Supabase 服务端密钥
]

# 逐项检查是否读取成功
for k in keys:  # 遍历所有需要检查的变量
    v = os.getenv(k)  # 从环境变量读取
    if v:  # 变量存在且非空
        print(f"✅ {k} 已读取（长度 {len(v)}）")  # 输出长度确认读取成功
    else:  # 变量为空或不存在
        print(f"❌ {k} 未读取到")  # 提示未读取到，便于修复
