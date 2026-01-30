# Supabase 表记录

用于在修改代码前快速核对表结构。

## 供应商维表

- Schema: `financial_hub_prod`
- Table: `vendor_dim`

创建 SQL：

```sql
create table if not exists financial_hub_prod.vendor_dim (
  vendor_code text primary key,          -- aliyun / aws / volcengine / stepfun ...
  vendor_name text not null,             -- 阿里云 / AWS / 火山引擎 / 阶跃星辰
  display_order int default 100,
  is_active boolean default true,
  remark text,
  created_at timestamptz default now()
);
```

## 表 1：阿里云【日粒度】账单表

- Schema: `financial_hub_prod`
- Table: `aliyun_bill_daily`

创建 SQL：

```sql
create schema if not exists financial_hub_prod;
create table if not exists financial_hub_prod.aliyun_bill_daily (
  id bigserial primary key,
  billing_date date not null,
  billing_cycle text not null,
  product_code text,
  product_name text,
  pretax_amount numeric,
  pretax_gross_amount numeric,
  currency text,
  is_ai_cost boolean default false,
  created_at timestamptz default now()
);
```

## 表 2：阿里云【月粒度】账单表

- Schema: `financial_hub_prod`
- Table: `aliyun_bill_monthly`

创建 SQL：

```sql
create table if not exists financial_hub_prod.aliyun_bill_monthly (
  id bigserial primary key,
  billing_cycle text not null,
  product_code text,
  product_name text,
  total_amount numeric,
  total_gross_amount numeric,
  currency text,
  is_ai_cost boolean default false,
  created_at timestamptz default now()
);
```

## 月汇总思路（关键）

```sql
insert into financial_hub_prod.aliyun_bill_monthly (
  billing_cycle,
  product_code,
  product_name,
  total_amount,
  total_gross_amount,
  currency,
  is_ai_cost
)
select
  billing_cycle,
  product_code,
  product_name,
  sum(pretax_amount) as total_amount,
  sum(pretax_gross_amount) as total_gross_amount,
  currency,
  is_ai_cost
from financial_hub_prod.aliyun_bill_daily
group by
  billing_cycle,
  product_code,
  product_name,
  currency,
  is_ai_cost;
```

## AI Token【日粒度】表

- Schema: `financial_hub_prod`
- Table: `llm_token_daily_usage`

创建 SQL：

```sql
create table if not exists financial_hub_prod.llm_token_daily_usage (
  id uuid primary key default gen_random_uuid(),

  day date not null,                     -- 统计日期（T+1：昨日）
  vendor text not null,                  -- 供应商标识（aliyun / volcengine / stepfun / aws ...）

  model_id text,                         -- 模型 ID（可空）
  account_id text,                       -- 账号 ID（可空）
  project_id text,                       -- 项目 ID（可空）

  input_tokens bigint default 0,          -- 输入 token
  output_tokens bigint default 0,         -- 输出 token
  cache_tokens bigint default 0,          -- cache 命中 token
  total_tokens bigint default 0,          -- token 总量（周/月汇总主指标）

  request_count bigint default 0,         -- 请求数
  image_count bigint default 0,           -- 图像调用次数
  websearch_count bigint default 0,       -- web search 次数
  tts_word_count bigint default 0,        -- TTS 字数
  asr_duration_seconds bigint default 0,  -- ASR 时长（秒）

  extra_metrics jsonb,                   -- 扩展指标（供应商自定义）
  raw jsonb,                             -- 原始返回数据
  remark text,

  account_key text,                      -- 账号标识（非密钥）
  project_key text,                      -- 项目标识（非密钥）

  created_at timestamptz default now(),
  updated_at timestamptz default now()
);
```

## AI Token【周汇总】表

- Schema: `financial_hub_prod`
- Table: `llm_token_weekly_usage`

创建 SQL：

```sql
create table if not exists financial_hub_prod.llm_token_weekly_usage (
  id bigserial primary key,
  vendor_code text not null,
  week_start date not null,
  week_end date not null,
  token_total numeric not null default 0,
  created_at timestamptz default now(),
  constraint uq_llm_token_week unique (vendor_code, week_start, week_end)
);
```

## AI Token【月汇总】表

- Schema: `financial_hub_prod`
- Table: `llm_token_monthly_usage`

创建 SQL：

```sql
create table if not exists financial_hub_prod.llm_token_monthly_usage (
  id bigserial primary key,
  vendor_code text not null,
  month text not null,                   -- YYYY-MM
  token_total numeric not null default 0,
  created_at timestamptz default now(),
  constraint uq_llm_token_month unique (vendor_code, month)
);
```

## 账单【周汇总】通用表

- Schema: `financial_hub_prod`
- Table: `bill_weekly_summary`

创建 SQL：

```sql
create table if not exists financial_hub_prod.bill_weekly_summary (
  id bigserial primary key,
  vendor_code text not null,
  week_start date not null,
  week_end date not null,
  is_ai_cost boolean not null default false,

  amount numeric not null default 0,          -- 主口径（pretax_amount）
  gross_amount numeric not null default 0,    -- 辅助口径（pretax_gross_amount）
  currency text default 'CNY',

  created_at timestamptz default now(),
  constraint uq_bill_week unique (
    vendor_code, week_start, week_end, is_ai_cost
  )
);
```

## 账单【月汇总】通用表

- Schema: `financial_hub_prod`
- Table: `bill_monthly_summary`

创建 SQL：

```sql
create table if not exists financial_hub_prod.bill_monthly_summary (
  id bigserial primary key,
  vendor_code text not null,
  month text not null,                         -- YYYY-MM
  is_ai_cost boolean not null default false,

  amount numeric not null default 0,           -- 主口径
  gross_amount numeric not null default 0,     -- 辅助口径
  currency text default 'CNY',

  created_at timestamptz default now(),
  constraint uq_bill_month unique (
    vendor_code, month, is_ai_cost
  )
);
```

## 账单【日汇总】通用表（新增建议）

- Schema: `financial_hub_prod`
- Table: `bill_daily_summary`

创建 SQL：

```sql
create table if not exists financial_hub_prod.bill_daily_summary (
  id bigserial primary key,
  vendor_code text not null,
  billing_date date not null,
  is_ai_cost boolean not null default false,

  amount numeric not null default 0,          -- 主口径
  gross_amount numeric not null default 0,    -- 辅助口径
  currency text default 'CNY',

  created_at timestamptz default now(),
  constraint uq_bill_day unique (
    vendor_code, billing_date, is_ai_cost
  )
);
```
