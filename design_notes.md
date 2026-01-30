设计思路记录（多运维供应商 · AI/非AI 成本 + Token · 日入库 · 周/月汇总 · 运维页面）
1. 目标与范围

目标（你想实现什么）：

多供应商（阿里云、AWS、火山引擎、阶跃星辰等）每天自动获取昨日账单金额与 token（如有）

有 API 优先用 API；无 API 的供应商用 Computer Use 工程化兜底

数据按日写入 Supabase；并汇总生成周/月数据

创建运维数据页面用于周报/月报、趋势对比与异常发现

自动化运行在服务器（定时任务），不在本机执行

业务范围（哪些模块/流程包含在内）：

供应商账单采集（金额）

LLM token 用量采集（token）

AI/非AI 分类（至少对阿里云、火山引擎需要）

周/月汇总写表

运维页面查询与展示

不做的范围（明确不覆盖的点）：

不做“每次调用级别 usage 明细入库”（过重）

不做财务开票/税务对账系统

不追求实时数据（按 T+1 日粒度）

2. 背景与动机

为什么要做（现有痛点/问题）：

供应商多、口径分散，人工统计成本高且容易错

部分供应商没有直接 API，需要可持续的自动化兜底方案

需要将 AI 与非AI 成本拆分并可周/月维度汇总，支撑管理决策与汇报

关键约束（成本、时间、合规、性能等）：

必须服务器可跑（定时稳定、可重试、可观测）

token/账单可能存在 T+2 延迟、或当天为 0 的情况

凭证（Key/Cookie）必须安全存储，不落库

Supabase 作为中心库，字段口径需要稳定可追溯

3. 数据与表结构（Supabase 实际表结构）
3.1 Schema

Schema：financial_hub_prod

3.2 Token【日粒度】表（已存在）

表名：llm_token_daily_usage
用途：统一存放各 AI 供应商的每日 token 与相关使用指标

字段说明（以真实表为准）

id (uuid)：主键

day (date)：统计日期（T+1 场景下写“昨日”）

vendor (text)：供应商标识（如 aliyun / volcengine / stepfun）

model_id (text)：模型标识（可空）

account_id (text)：供应商账号 ID（可空）

project_id (text)：项目 ID（可空）

Token 相关核心字段

input_tokens (bigint)

output_tokens (bigint)

cache_tokens (bigint)

total_tokens (bigint) ← 周/月汇总主指标

调用与资源指标（扩展）

request_count (bigint)

image_count (bigint)

websearch_count (bigint)

tts_word_count (bigint)

asr_duration_seconds (bigint)

扩展与审计

extra_metrics (jsonb)：供应商扩展指标

raw (jsonb)：原始返回数据

remark (text)

created_at / updated_at

account_key / project_key（标识用，不作为权限凭证）

数据特点

日粒度、可多行（同一天、同 vendor、不同 model/project 可多条）

后续周/月汇总 必须按 day + vendor 聚合

3.3 Token【周汇总】表（已存在）

表名：llm_token_weekly_usage

vendor_code

week_start

week_end

token_total

created_at

数据来源：llm_token_daily_usage.total_tokens 按周聚合

3.4 Token【月汇总】表（已存在）

表名：llm_token_monthly_usage

vendor_code

month（YYYY-MM）

token_total

created_at

数据来源：llm_token_daily_usage.total_tokens 按月聚合

3.5 账单相关表（已存在）

日表：aliyun_bill_daily

周表：bill_weekly_summary

月表：bill_monthly_summary

补充：建议新增通用日汇总表 bill_daily_summary（按 vendor + 日期汇总）

（金额口径：主口径 + gross 口径并存，前文已确认）

4. 流程与逻辑（Token 维度）

本章只描述逻辑结构，不涉及具体代码实现。

4.1 Token 日数据写入逻辑

定时任务在服务器触发（每日 T+1）

针对每个 AI 供应商：

有 API：直接拉取昨日用量

无 API：Computer Use 自动化抓取页面统计值

阿里云百炼 token 采集（无官方 API）：

先通过自动化登录阿里云控制台

再进入百炼“模型用量统计”页面触发请求

捕获临时登录态（cookie / sec_token / csrf）

用临时登录态调用 Web 接口拉取数据

将供应商返回数据映射到 llm_token_daily_usage

按以下维度写入：

day = 昨日

vendor = 供应商标识

model_id / project_id / account_id（如有）

token & 计数类字段

raw 存原始数据（便于追溯）

支持 同日重复执行（允许覆盖 / 删除重写）

4.2 Token 周汇总计算逻辑

聚合规则：

粒度：vendor + week

核心指标：

token_total = sum(llm_token_daily_usage.total_tokens)


周定义：

自然周（周一 ~ 周日）

4.3 Token 月汇总计算逻辑

聚合规则：

粒度：vendor + month(YYYY-MM)

核心指标：

token_total = sum(llm_token_daily_usage.total_tokens)

5. 接口与输入输出（Token 相关）
输入

vendor

target_day（默认昨日）

供应商认证信息（API Key / Cookie）

Computer Use 登录参数（仅在服务器环境）

输出

日数据写入：llm_token_daily_usage

周汇总写入：llm_token_weekly_usage

月汇总写入：llm_token_monthly_usage

6. 异常与边界（Token 场景）
已知边界

某日 token 为 0

页面统计延迟（T+2）

Computer Use 登录失效 / 页面改版

单日数据缺失（允许后补）

错误处理策略

日表支持 删除当日后重跑

周/月汇总支持 整周期重算

失败不阻断其他供应商

7. 权限与安全（补充）

account_key / project_key 仅用于标识，不作为真实密钥

真实 Key / Cookie：

仅存在于服务器环境变量

不写入 Supabase

raw 字段如含敏感信息需脱敏或裁剪

8. 测试与验证（Token）

对账方式：

Supabase 汇总 vs 供应商后台展示

校验项：

input + output + cache ≈ total_tokens

连续多日趋势是否合理

幂等测试：

同一天重复跑是否重复入库

9. 备注与后续计划

当前设计已支持：

多供应商

多模型 / 多项目

非 token 型 AI 指标（image / tts / asr）

后续可扩展：

Token → 金额映射（模型单价表）

异常激增告警

页面 drill-down 到 model / project 级别

10. 天眼查账单接口解析（技术说明）
10.1 接口来源
使用天眼查开放平台控制台的浏览器接口（非官方文档 API）：
GET https://open.tianyancha.com/open-admin/org/order.json
通过 authSecret 鉴权，不依赖 Cookie / 登录态，适合后端定时拉取。

10.2 页面字段与接口字段映射
页面字段与接口字段对应：
每日账单编号 → orderCode
账单金额 → cost / 100（单位为分，负数表示扣费）
账单余额 → balance / 100（单位为分）
账单时间 → createTime（毫秒时间戳，需按北京时间 +08 展示）
页面显示时间与 createTime 转北京时间一致。

10.3 用量统计解析（关键）
用量不在顶层字段中，而在 orderDetail（或 orderDesc）。
该字段为 JSON 字符串，需先 JSON.parse / json.loads。
解析后结构：
startTime / endTime：本次账单覆盖的统计周期（按天），毫秒时间戳。
iCountList[]：该天内按接口/功能维度的调用与扣费明细。

10.4 iCountList 字段含义
每个 iCountList 元素：
fName：功能名（如搜索、企业联系方式）
parentFName：功能大类
fUrl：接口路径
fPrice：单价
iCount：总请求次数
paidCount：计费次数（核心用量指标）
noDataCount：无数据次数
dataCount：有数据次数
cost：该功能扣费金额（单位：分，正数）
每条账单总扣费满足：sum(iCountList.cost) = abs(顶层 cost)。

10.5 统一口径
1) 金额字段统一以“分”为原始值，展示/入库时再 /100。
2) 账单时间使用 createTime，按北京时间 (+08)。
3) 每日消耗金额口径：sum(iCountList.cost) / 100（正数）。
4) 每日用量统计口径：按 iCountList.paidCount 汇总。
5) 每条账单对应的是前一天的用量结算。

10.6 示例验证
页面账单金额：-29.98
接口返回：cost = -2998
明细合计：2928 + 70 = 2998
页面账单时间：2026-01-29 01:32:47
接口 createTime 转北京时间一致
数据与页面展示一致，可工程化使用。