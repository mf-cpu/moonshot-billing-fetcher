# 运维前端页面

一个轻量级静态页面，用于查看账单与 AI/非AI 用量数据（从 Supabase 读取）。

## 使用方式

1) 复制配置文件：

```
copy config.example.js config.js
```

2) 编辑 `config.js`，填入你的 Supabase 信息：

```
window.SUPABASE_URL = "https://your-project.supabase.co";
window.SUPABASE_ANON_KEY = "your-anon-key";
```

3) 直接双击 `index.html`，或用任意静态服务器打开。

## 说明

- 默认查询近 30 天（不含今天），可修改日期范围。
- 页面包含「运维页面」与「数据获取」两部分。
- 天眼查账单统一标记为非 AI（使用 `bill_daily_summary` 表）。
- 数据来源：
  - `financial_hub_prod.aliyun_bill_daily`
  - `financial_hub_prod.llm_token_daily_usage`
  - `financial_hub_prod.bill_daily_summary`
