# ------------------------------------------------------------------------------------
# 在 Openai官方库 中使用 DMXAPI KEY 的例子
# 需要先 pip install openai
# ------------------------------------------------------------------------------------
"""演示如何通过 DMXAPI 中转调用 OpenAI Chat Completions。"""
from openai import OpenAI # type: ignore

# 初始化客户端配置（API Key + 中转地址）
client = OpenAI(
    api_key="sk-PXz2OiMkos0bVwlZIAJZnl0YpVsV8E2dHMOk8Aqb2nHX73ha",  # 替换成你的 DMXapi 令牌key
    base_url="https://vip.dmxapi.com/v1",  # 需要改成DMXAPI的中转 https://www.dmxapi.com/v1 ，这是已经改好的。
)

# 发起一次对话请求
chat_completion = client.chat.completions.create(
    messages=[
        {
            "role": "user",
            "content": "周树人和鲁迅是兄弟吗？",
        }
    ],
    model="gpt-5",    #  替换成你先想用的模型全称， 模型全称可以在DMXAPI 模型价格页面找到并复制。
)

# 打印返回结果
print(chat_completion)
