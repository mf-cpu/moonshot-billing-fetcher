# 账单与 Token 规则文档

## 1) AI 类（阶跃星辰 / DeepSeek 等）

### Token 日数据
- 表：`financial_hub_prod.llm_token_daily_usage`
- 字段：`vendor` + `day` + `total_tokens`（以及输入/输出/缓存等）
- 说明：统一 token 口径，所有 AI 供应商都写入此表。

### 金额（若供应商能给）
- 表：`financial_hub_prod.bill_daily_summary`
- 字段：`vendor_code` + `billing_date` + `amount` / `gross_amount` + `currency`
- 约束：`is_ai_cost = true`
- 说明：AI 金额与 token 分表，避免混淆。

## 2) 非 AI 类（火山引擎 / 月之暗面 / 百度云等）

### 账单日金额
- 表：`financial_hub_prod.bill_daily_summary`
- 字段：`vendor_code` 对应供应商
- 约束：`is_ai_cost = false`
- 说明：仅账单金额，不进入 token 表。

## 3) 周/月汇总（统一从日表聚合）

### Token 周/月
- 来源：`llm_token_daily_usage`
- 写入：`llm_token_weekly_usage`、`llm_token_monthly_usage`

### 账单周/月
- 来源：`bill_daily_summary`
- 写入：`bill_weekly_summary`、`bill_monthly_summary`
- 聚合粒度：`vendor_code` + `is_ai_cost` + `week/month`
- 金额口径：`amount` / `gross_amount`

## 规则总结（防混乱）

- Token 永远只进 `llm_token_daily_usage`
- 金额永远进 `bill_daily_summary`
- AI / 非 AI 只靠 `is_ai_cost` 区分
- 供应商只靠 `vendor` / `vendor_code` 区分
- 周/月一律从日表聚合
