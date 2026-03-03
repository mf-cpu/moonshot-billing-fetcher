import hashlib
import hmac
import html
import json
import os
import threading
import time
import webbrowser
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, parse_qsl, quote, urlencode, urlparse, urlunparse

import requests
try:
    from dotenv import load_dotenv
except ImportError:  # 兼容未安装 python-dotenv 的环境
    load_dotenv = None
try:
    from postgrest.exceptions import APIError
except ImportError:  # 兼容未安装 postgrest 的环境
    APIError = Exception
from aliyun_token_ingest_daily import (
    bj_day_range,
    fetch_usage,
    parse_total_tokens,
    supabase_client,
    insert_daily_row,
    sum_token_daily,
    upsert_monthly_summary,
    upsert_weekly_summary,
    week_bounds,
)


BJ = timezone(timedelta(hours=8))

# ========== 登录保护 ==========
LOGIN_PASSWORD = os.getenv("OPS_PASSWORD", "ops2026")
_login_tokens = {}  # token -> expire_time

def _generate_login_token():
    """生成登录令牌"""
    import secrets
    token = secrets.token_hex(32)
    _login_tokens[token] = time.time() + 86400  # 24小时有效
    # 清理过期 token
    now = time.time()
    expired = [k for k, v in _login_tokens.items() if v < now]
    for k in expired:
        del _login_tokens[k]
    return token

def _verify_login_token(token):
    """验证登录令牌"""
    if not token:
        return False
    expire = _login_tokens.get(token)
    if not expire:
        return False
    if time.time() > expire:
        del _login_tokens[token]
        return False
    return True

def _get_cookie_token(handler):
    """从请求 Cookie 中获取 login_token"""
    cookie_header = handler.headers.get("Cookie", "")
    for part in cookie_header.split(";"):
        part = part.strip()
        if part.startswith("ops_token="):
            return part[len("ops_token="):]
    return None

def render_login_page(error=""):
    """渲染登录页面"""
    error_html = f'<div class="login-error">{html.escape(error)}</div>' if error else ''
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/><title>数据拉取工具 - 登录</title>
<style>
*{{box-sizing:border-box}}
body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif;background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);margin:0;min-height:100vh;display:flex;align-items:center;justify-content:center}}
.login-card{{background:#fff;border-radius:12px;padding:40px;width:100%;max-width:400px;box-shadow:0 25px 50px -12px rgba(0,0,0,0.25)}}
.login-card h2{{margin:0 0 8px;font-size:24px;font-weight:700;color:#111827;text-align:center}}
.login-card p{{margin:0 0 32px;color:#6b7280;font-size:14px;text-align:center}}
.login-card input[type=password]{{width:100%;padding:14px 16px;font-size:16px;border:2px solid #e5e7eb;border-radius:8px;background:#f9fafb;transition:border-color .2s,background .2s}}
.login-card input[type=password]:focus{{outline:none;border-color:#4f46e5;background:#fff}}
.login-card button{{width:100%;padding:14px;font-size:16px;font-weight:600;background:#4f46e5;color:#fff;border:none;border-radius:8px;cursor:pointer;margin-top:20px;transition:background .2s}}
.login-card button:hover{{background:#4338ca}}
.login-error{{margin-top:16px;padding:12px;background:#fef2f2;color:#ef4444;border-radius:8px;font-size:14px;text-align:center}}
</style>
</head>
<body>
<div class="login-card">
<h2>数据拉取工具</h2>
<p>请输入访问密码</p>
<form method="post" action="/login">
<input type="password" name="password" placeholder="输入密码" autocomplete="current-password" required/>
<button type="submit">登 录</button>
</form>
{error_html}
</div>
</body>
</html>"""
STEPFUN_API = os.getenv(
    "STEPFUN_USAGE_URL",
    "https://platform.stepfun.com/api/step.openapi.devcenter.Dashboard/DevQueryUsageHistory",
)
VOLCENGINE_API = os.getenv(
    "VOLCENGINE_BILLING_URL",
    "https://open.volcengineapi.com?Action=ListBillDetail&Version=2022-01-01",
)
TIANYANCHA_API = os.getenv(
    "TIANYANCHA_BILL_URL",
    "https://open.tianyancha.com/open-admin/org/order.json",
)
KIMI_API = os.getenv(
    "KIMI_BILL_URL",
    "https://platform.moonshot.cn/api",
)
TEXTIN_API = os.getenv(
    "TEXTIN_BILL_URL",
    "https://web-api.textin.com/user/finance/consume",
)
DEEPSEEK_COST_API = os.getenv(
    "DEEPSEEK_COST_URL",
    "https://platform.deepseek.com/api/v0/usage/cost",
)
DEEPSEEK_AMOUNT_API = os.getenv(
    "DEEPSEEK_AMOUNT_URL",
    "https://platform.deepseek.com/api/v0/usage/amount",
)

# 加载本地 .env（若存在），方便读取配置
if load_dotenv:
    load_dotenv()

# 执行控制状态（按 session_id 独立管理）
_exec_sessions = {}
_exec_lock = threading.Lock()

def _get_session(session_id: str) -> dict:
    """获取或创建 session 状态"""
    with _exec_lock:
        if session_id not in _exec_sessions:
            _exec_sessions[session_id] = {"paused": False, "stopped": False}
        return _exec_sessions[session_id]

def _reset_exec_state(session_id: str):
    with _exec_lock:
        if session_id in _exec_sessions:
            _exec_sessions[session_id] = {"paused": False, "stopped": False}

def _cleanup_session(session_id: str):
    """清理已完成的 session"""
    with _exec_lock:
        if session_id in _exec_sessions:
            del _exec_sessions[session_id]

def _is_paused(session_id: str) -> bool:
    with _exec_lock:
        session = _exec_sessions.get(session_id, {})
        return session.get("paused", False)

def _is_stopped(session_id: str) -> bool:
    with _exec_lock:
        session = _exec_sessions.get(session_id, {})
        return session.get("stopped", False)

def _set_paused(session_id: str, val: bool):
    with _exec_lock:
        if session_id in _exec_sessions:
            _exec_sessions[session_id]["paused"] = val

def _set_stopped(session_id: str, val: bool):
    with _exec_lock:
        if session_id in _exec_sessions:
            _exec_sessions[session_id]["stopped"] = val

def yesterday_bj() -> str:
    return (datetime.now(tz=BJ) - timedelta(days=1)).date().isoformat()


def read_body(handler: BaseHTTPRequestHandler) -> bytes:
    length = int(handler.headers.get("content-length", "0") or "0")
    return handler.rfile.read(length)


def send_plain_error(handler: BaseHTTPRequestHandler, code: int, message: str) -> None:
    handler.send_response(code)
    handler.send_header("content-type", "text/plain; charset=utf-8")
    handler.end_headers()
    handler.wfile.write(message.encode("utf-8", "replace"))


def render_form(default_start: str, default_end: str) -> str:
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/><title>数据拉取工具</title>
<style>
:root{{--primary:#4f46e5;--primary-hover:#4338ca;--success:#10b981;--gray-50:#f9fafb;--gray-100:#f3f4f6;--gray-200:#e5e7eb;--gray-300:#d1d5db;--gray-500:#6b7280;--gray-600:#4b5563;--gray-700:#374151;--gray-800:#1f2937;--gray-900:#111827;--radius:8px;--shadow:0 1px 3px rgba(0,0,0,.1)}}
*{{box-sizing:border-box}}body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:var(--gray-100);margin:0;padding:20px;min-height:100vh}}
.container{{max-width:800px;margin:0 auto}}.header{{text-align:center;margin-bottom:24px}}.header h1{{font-size:24px;color:var(--gray-900);margin:0 0 8px}}.header p{{color:var(--gray-500);margin:0;font-size:14px}}
.tabs{{display:flex;gap:4px;background:var(--gray-200);padding:4px;border-radius:var(--radius);margin-bottom:20px}}.tab{{flex:1;padding:10px 16px;border:none;background:0 0;font-size:14px;font-weight:500;color:var(--gray-600);cursor:pointer;border-radius:6px;transition:all .2s}}.tab:hover{{color:var(--gray-900)}}.tab.active{{background:#fff;color:var(--primary);box-shadow:var(--shadow)}}
.card{{background:#fff;border-radius:var(--radius);box-shadow:var(--shadow);padding:24px;margin-bottom:20px}}.vendor-grid{{display:grid;grid-template-columns:repeat(2,1fr);gap:12px}}.vendor-item{{position:relative}}.vendor-item input[type=radio]{{position:absolute;opacity:0}}.vendor-item label{{display:block;padding:16px;border:2px solid var(--gray-200);border-radius:var(--radius);cursor:pointer;transition:all .2s}}.vendor-item label:hover{{border-color:var(--gray-300);background:var(--gray-50)}}.vendor-item input:checked+label{{border-color:var(--primary);background:#eef2ff}}.vendor-name{{font-weight:600;color:var(--gray-800);margin-bottom:4px}}.vendor-desc{{font-size:12px;color:var(--gray-500)}}.vendor-tag{{display:inline-block;font-size:10px;padding:2px 6px;border-radius:4px;margin-left:6px;font-weight:500}}.tag-ai{{background:#dbeafe;color:#1d4ed8}}.tag-non-ai{{background:#fef3c7;color:#92400e}}
.form-section{{display:none;margin-top:20px}}.form-section.active{{display:block}}.form-group{{margin-bottom:16px}}.form-group label{{display:block;font-size:14px;font-weight:500;color:var(--gray-700);margin-bottom:6px}}.form-group input[type=text],.form-group input[type=date],.form-group textarea{{width:100%;padding:10px 12px;border:1px solid var(--gray-300);border-radius:6px;font-size:14px;transition:border-color .2s}}.form-group input:focus,.form-group textarea:focus{{outline:0;border-color:var(--primary)}}.form-group textarea{{height:100px;font-family:monospace;font-size:13px;resize:vertical}}.form-hint{{font-size:12px;color:var(--gray-500);margin-top:4px}}.date-row{{display:grid;grid-template-columns:1fr 1fr;gap:16px}}.btn-row{{display:flex;gap:12px;margin-top:24px}}.btn{{padding:12px 24px;border:none;border-radius:6px;font-size:14px;font-weight:500;cursor:pointer;transition:all .2s}}.btn-primary{{background:var(--primary);color:#fff;flex:1}}.btn-primary:hover{{background:var(--primary-hover)}}.btn-secondary{{background:var(--gray-100);color:var(--gray-700)}}.btn-secondary:hover{{background:var(--gray-200)}}.checkbox-group{{display:flex;align-items:center;gap:8px}}.checkbox-group input{{width:16px;height:16px}}.env-info{{background:var(--gray-50);border:1px solid var(--gray-200);border-radius:6px;padding:12px;margin-top:12px;font-size:12px;color:var(--gray-600)}}.env-info code{{background:var(--gray-200);padding:1px 4px;border-radius:3px;font-size:11px}}.hidden{{display:none!important}}
.exec-header{{display:flex;justify-content:space-between;align-items:center;margin-bottom:16px}}.exec-header h3{{margin:0;font-size:16px}}.exec-controls{{display:flex;gap:8px}}.btn-ctrl{{padding:8px 16px;font-size:12px;background:var(--gray-100);color:var(--gray-700)}}.btn-ctrl:hover{{background:var(--gray-200)}}.btn-ctrl:disabled{{opacity:0.5;cursor:not-allowed}}.btn-danger{{padding:8px 16px;font-size:12px;background:#ef4444;color:#fff}}.btn-danger:hover{{background:#dc2626}}
.progress-bar{{height:8px;background:var(--gray-200);border-radius:4px;overflow:hidden;margin-bottom:12px}}.progress-fill{{height:100%;background:var(--primary);width:0%;transition:width 0.3s}}
.exec-status{{display:flex;justify-content:space-between;font-size:14px;color:var(--gray-600);margin-bottom:12px}}
.exec-log{{background:var(--gray-900);color:#10b981;padding:16px;border-radius:6px;font-family:monospace;font-size:12px;max-height:300px;overflow-y:auto;white-space:pre-wrap}}
</style>
</head>
<body>
<div class="container">
<div class="header"><h1>数据拉取工具</h1><p>选择供应商，填写信息后一键入库</p></div>
<div class="tabs"><button type="button" class="tab active" data-tab="cookie">需要 Cookie</button><button type="button" class="tab" data-tab="api">API 密钥</button></div>
<form method="post" action="/fetch" id="fetchForm"><input type="hidden" name="vendor" id="vendorInput" value="bailian"/>
<div class="card" id="panel-cookie"><div class="vendor-grid">
<div class="vendor-item"><input type="radio" name="vendor_select" id="v_bailian" value="bailian" checked/><label for="v_bailian"><div class="vendor-name">阿里云百炼<span class="vendor-tag tag-ai">AI</span></div><div class="vendor-desc">Token 用量</div></label></div>
<div class="vendor-item"><input type="radio" name="vendor_select" id="v_stepfun" value="stepfun"/><label for="v_stepfun"><div class="vendor-name">阶跃星辰<span class="vendor-tag tag-ai">AI</span></div><div class="vendor-desc">Token+金额</div></label></div>
<div class="vendor-item"><input type="radio" name="vendor_select" id="v_moonshot" value="moonshot_bill"/><label for="v_moonshot"><div class="vendor-name">月之暗面<span class="vendor-tag tag-ai">AI</span></div><div class="vendor-desc">日粒度账单</div></label></div>
<div class="vendor-item"><input type="radio" name="vendor_select" id="v_textin" value="textin_bill"/><label for="v_textin"><div class="vendor-name">TextIn<span class="vendor-tag tag-non-ai">非AI</span></div><div class="vendor-desc">OCR消费</div></label></div>
<div class="vendor-item"><input type="radio" name="vendor_select" id="v_tianyancha" value="tianyancha_bill"/><label for="v_tianyancha"><div class="vendor-name">天眼查<span class="vendor-tag tag-non-ai">非AI</span></div><div class="vendor-desc">API账单</div></label></div>
<div class="vendor-item"><input type="radio" name="vendor_select" id="v_deepseek" value="deepseek"/><label for="v_deepseek"><div class="vendor-name">DeepSeek<span class="vendor-tag tag-ai">AI</span></div><div class="vendor-desc">消费+Token</div></label></div>
<div class="vendor-item"><input type="radio" name="vendor_select" id="v_dmxapi_manual" value="dmxapi_manual"/><label for="v_dmxapi_manual"><div class="vendor-name">DMXAPI<span class="vendor-tag tag-ai">AI</span></div><div class="vendor-desc">手动录入周Token</div></label></div>
</div>
<div class="form-section" id="form_bailian"><div class="form-group"><label>百炼 Cookie *</label><textarea name="cookie" placeholder="从浏览器复制"></textarea></div><div class="env-info">需要: <code>ALIYUN_BAILIAN_WORKSPACE_ID</code> <code>ALIYUN_BAILIAN_REGION</code> <code>ALIYUN_BAILIAN_USAGE_URL</code></div></div>
<div class="form-section" id="form_stepfun"><div class="form-group"><label>阶跃星辰 Cookie *</label><textarea name="stepfun_cookie" placeholder="从浏览器复制"></textarea></div><div class="env-info">需要: <code>STEPFUN_OASIS_APPID</code> <code>STEPFUN_OASIS_WEBID</code></div></div>
<div class="form-section" id="form_moonshot_bill"><div class="form-group"><label>Bearer Token *</label><input type="text" name="moonshot_token" placeholder="从authorization头复制"/></div><div class="form-group"><label>组织 ID *</label><input type="text" name="moonshot_org_id" placeholder="如 org-xxx"/></div><div class="form-group"><label>Cookie</label><textarea name="moonshot_cookie" placeholder="可选"></textarea></div></div>
<div class="form-section" id="form_textin_bill"><div class="form-group"><label>TextIn Token *</label><input type="text" name="textin_token" placeholder="如 76233542..."/></div></div>
<div class="form-section" id="form_tianyancha_bill"><div class="form-group"><label>authSecret *</label><input type="text" name="auth_secret" placeholder="70669ae1-06d8-..."/></div><div class="form-group"><label>Cookie *</label><textarea name="tian_cookie" placeholder="从 open.tianyancha.com 复制（必填）"></textarea></div><div class="form-hint">从浏览器开发者工具复制 Cookie，用于验证登录状态</div></div>
<div class="form-section" id="form_deepseek"><div class="form-group"><label>Authorization Token *</label><input type="text" name="deepseek_auth" placeholder="从浏览器开发者工具复制 authorization 头的值"/></div><div class="form-group"><label>Cookie</label><textarea name="deepseek_cookie" placeholder="可选，从浏览器复制"></textarea></div><div class="form-hint">从 platform.deepseek.com 控制台网络请求中复制，同时获取消费和Token数据</div></div>
<div class="form-section" id="form_dmxapi_manual"><div class="form-group"><label>周开始日期 *</label><input type="date" name="dmx_week_start"/></div><div class="form-group"><label>周结束日期 *</label><input type="date" name="dmx_week_end"/></div><div class="form-group"><label>Input Tokens *</label><input type="number" name="dmx_input_tokens" placeholder="输入Token数"/></div><div class="form-group"><label>Output Tokens *</label><input type="number" name="dmx_output_tokens" placeholder="输出Token数"/></div><div class="form-group"><label>消耗金额 ($)</label><input type="number" name="dmx_amount" step="0.01" placeholder="消耗金额（美元）"/></div><div class="form-hint">手动录入周汇总数据，直接写入 llm_token_weekly_summary</div></div>
</div>
<div class="card hidden" id="panel-api"><div class="vendor-grid">
<div class="vendor-item"><input type="radio" name="vendor_select" id="v_aliyun_bill" value="aliyun_bill"/><label for="v_aliyun_bill"><div class="vendor-name">阿里云账单<span class="vendor-tag tag-ai">含AI</span></div><div class="vendor-desc">BSS API</div></label></div>
<div class="vendor-item"><input type="radio" name="vendor_select" id="v_aws_bill" value="aws_bill"/><label for="v_aws_bill"><div class="vendor-name">亚马逊 AWS<span class="vendor-tag tag-non-ai">非AI</span></div><div class="vendor-desc">Cost Explorer</div></label></div>
<div class="vendor-item"><input type="radio" name="vendor_select" id="v_volcengine" value="volcengine_bill"/><label for="v_volcengine"><div class="vendor-name">火山引擎<span class="vendor-tag tag-ai">含AI</span></div><div class="vendor-desc">账单+Token</div></label></div>
</div>
<div class="form-section" id="form_aliyun_bill"><div class="env-info">需要: <code>ALIYUN_ACCESS_KEY_ID</code> <code>ALIYUN_ACCESS_KEY_SECRET</code></div></div>
<div class="form-section" id="form_aws_bill"><div class="checkbox-group"><input type="checkbox" name="aws_dump_raw" value="1" id="aws_dump_raw" checked/><label for="aws_dump_raw">输出原始返回</label></div><div class="env-info">需要: <code>AWS_ACCESS_KEY_ID</code> <code>AWS_SECRET_ACCESS_KEY</code></div></div>
<div class="form-section" id="form_volcengine_bill"><div class="env-info">需要: <code>VOLCENGINE_ACCESS_KEY</code> <code>VOLCENGINE_SECRET_KEY</code></div></div>
</div>
<div class="card" id="dateCard"><div class="date-row"><div class="form-group"><label>开始日期 *</label><input type="date" name="start_day" value="{html.escape(default_start)}" required/></div><div class="form-group"><label>结束日期 *</label><input type="date" name="end_day" value="{html.escape(default_end)}" required/></div></div><div class="form-hint">数据将按天入库</div></div>
<div class="btn-row"><button type="button" class="btn btn-primary" id="btnStart">开始拉取并入库</button><button type="button" id="btnRetry" class="btn btn-secondary">只拉开始日</button></div>
</form>
<div class="card" id="execPanel" style="display:none;">
<div class="exec-header"><h3>执行状态</h3><div class="exec-controls"><button type="button" class="btn btn-ctrl" id="btnPause">暂停</button><button type="button" class="btn btn-ctrl" id="btnResume" disabled>继续</button><button type="button" class="btn btn-danger" id="btnStop">停止</button></div></div>
<div class="progress-bar"><div class="progress-fill" id="progressFill"></div></div>
<div class="exec-status"><span id="statusText">准备中...</span><span id="progressText">0/0</span></div>
<div class="exec-log" id="execLog"></div>
</div>
</div>
<script>
const tabs=document.querySelectorAll('.tab'),cookiePanel=document.getElementById('panel-cookie'),apiPanel=document.getElementById('panel-api'),vendorInput=document.getElementById('vendorInput'),radios=document.querySelectorAll('input[name="vendor_select"]'),formSections=document.querySelectorAll('.form-section');
const execPanel=document.getElementById('execPanel'),btnStart=document.getElementById('btnStart'),btnRetry=document.getElementById('btnRetry'),btnPause=document.getElementById('btnPause'),btnResume=document.getElementById('btnResume'),btnStop=document.getElementById('btnStop');
const progressFill=document.getElementById('progressFill'),statusText=document.getElementById('statusText'),progressText=document.getElementById('progressText'),execLog=document.getElementById('execLog');
let eventSource=null,isPaused=false,currentSessionId=null;
function genSessionId(){{return 'sess_'+Date.now()+'_'+Math.random().toString(36).substr(2,9);}}
tabs.forEach(tab=>{{tab.addEventListener('click',()=>{{tabs.forEach(t=>t.classList.remove('active'));tab.classList.add('active');if(tab.dataset.tab==='cookie'){{cookiePanel.classList.remove('hidden');apiPanel.classList.add('hidden');document.getElementById('v_bailian').checked=true;updateFormSection('bailian');}}else{{cookiePanel.classList.add('hidden');apiPanel.classList.remove('hidden');document.getElementById('v_aliyun_bill').checked=true;updateFormSection('aliyun_bill');}}}});}});
radios.forEach(radio=>{{radio.addEventListener('change',()=>updateFormSection(radio.value));}});
function updateFormSection(vendor){{vendorInput.value=vendor;formSections.forEach(sec=>sec.classList.remove('active'));const target=document.getElementById('form_'+vendor);if(target)target.classList.add('active');const dateCard=document.getElementById('dateCard');if(dateCard)dateCard.style.display=(vendor==='dmxapi_manual')?'none':'block';}}
updateFormSection('bailian');
function addLog(msg){{execLog.textContent+=msg+'\\n';execLog.scrollTop=execLog.scrollHeight;}}
function startFetch(retryOnly){{
  const form=document.getElementById('fetchForm');const formData=new FormData(form);
  if(retryOnly)formData.append('retry_day','1');
  currentSessionId=genSessionId();formData.append('session_id',currentSessionId);
  execPanel.style.display='block';execLog.textContent='';progressFill.style.width='0%';statusText.textContent='正在连接...';progressText.textContent='';
  isPaused=false;btnPause.disabled=false;btnResume.disabled=true;btnStart.disabled=true;btnRetry.disabled=true;btnStop.disabled=false;
  const params=new URLSearchParams(formData).toString();
  eventSource=new EventSource('/fetch_stream?'+params);
  eventSource.onmessage=function(e){{const data=JSON.parse(e.data);
    if(data.type==='progress'){{progressFill.style.width=data.percent+'%';statusText.textContent=data.status;progressText.textContent=data.current+'/'+data.total;}}
    else if(data.type==='log'){{addLog(data.message);}}
    else if(data.type==='done'){{statusText.textContent='完成';btnPause.disabled=true;btnResume.disabled=true;btnStop.disabled=true;btnStart.disabled=false;btnRetry.disabled=false;eventSource.close();}}
    else if(data.type==='error'){{statusText.textContent='错误: '+data.message;addLog('[ERROR] '+data.message);btnStart.disabled=false;btnRetry.disabled=false;eventSource.close();}}
    else if(data.type==='paused'){{statusText.textContent='已暂停';}}
    else if(data.type==='stopped'){{statusText.textContent='已停止';btnStart.disabled=false;btnRetry.disabled=false;eventSource.close();}}
  }};
  eventSource.onerror=function(){{statusText.textContent='连接断开';btnStart.disabled=false;btnRetry.disabled=false;eventSource.close();}};
}}
btnStart.addEventListener('click',()=>startFetch(false));
btnRetry.addEventListener('click',()=>startFetch(true));
btnPause.addEventListener('click',()=>{{if(currentSessionId)fetch('/fetch_control?action=pause&session_id='+currentSessionId);btnPause.disabled=true;btnResume.disabled=false;}});
btnResume.addEventListener('click',()=>{{if(currentSessionId)fetch('/fetch_control?action=resume&session_id='+currentSessionId);btnPause.disabled=false;btnResume.disabled=true;}});
btnStop.addEventListener('click',()=>{{if(currentSessionId)fetch('/fetch_control?action=stop&session_id='+currentSessionId);btnPause.disabled=true;btnResume.disabled=true;btnStop.disabled=true;}});
</script></body></html>
"""



def render_result(start_day: str, end_day: str, results: list[dict], vendor: str = "") -> str:
    summary_lines = "".join(_render_result_line(item) for item in results)
    last_raw = results[-1]["raw"] if results else {}
    raw_json = json.dumps(last_raw, ensure_ascii=False, indent=2)
    total_days = len(results)
    success_count = sum(1 for r in results if r.get("total_tokens", 0) > 0 or r.get("bill_amount") is not None)
    return f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/><title>拉取结果</title>
<style>
:root{{--primary:#4f46e5;--success:#10b981;--gray-50:#f9fafb;--gray-100:#f3f4f6;--gray-200:#e5e7eb;--gray-500:#6b7280;--gray-700:#374151;--gray-900:#111827;--radius:8px}}
*{{box-sizing:border-box}}body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:var(--gray-100);margin:0;padding:20px;min-height:100vh}}
.container{{max-width:800px;margin:0 auto}}.header{{text-align:center;margin-bottom:24px}}.header h1{{font-size:24px;color:var(--gray-900);margin:0 0 8px}}.card{{background:#fff;border-radius:var(--radius);box-shadow:0 1px 3px rgba(0,0,0,.1);padding:24px;margin-bottom:20px}}
.stats{{display:flex;gap:20px;margin-bottom:20px}}.stat{{flex:1;text-align:center;padding:16px;background:var(--gray-50);border-radius:var(--radius)}}.stat-value{{font-size:24px;font-weight:700;color:var(--primary)}}.stat-label{{font-size:12px;color:var(--gray-500);margin-top:4px}}
.result-list{{list-style:none;padding:0;margin:0}}.result-list li{{padding:12px 16px;border-bottom:1px solid var(--gray-100);font-size:14px}}.result-list li:last-child{{border-bottom:none}}
pre{{background:var(--gray-50);padding:16px;border-radius:var(--radius);overflow-x:auto;font-size:12px;max-height:400px}}
.btn{{padding:12px 24px;border:none;border-radius:6px;font-size:14px;font-weight:500;cursor:pointer;text-decoration:none;display:inline-block}}.btn-primary{{background:var(--primary);color:#fff}}.btn-primary:hover{{opacity:.9}}.btn-row{{display:flex;gap:12px;margin-top:24px}}.success{{color:var(--success)}}.top-bar{{margin-bottom:20px}}
</style>
</head><body>
<div class="container">
<div class="top-bar"><a href="/" class="btn btn-primary">返回继续拉取</a></div>
<div class="header"><h1 class="success">拉取完成</h1><p>日期范围：{html.escape(start_day)} ~ {html.escape(end_day)}</p></div>
<div class="stats"><div class="stat"><div class="stat-value">{total_days}</div><div class="stat-label">总天数</div></div><div class="stat"><div class="stat-value">{success_count}</div><div class="stat-label">成功入库</div></div></div>
<div class="card"><h3>入库明细</h3><ul class="result-list">{summary_lines}</ul></div>
<div class="card"><h3>最后一天原始返回</h3><pre>{html.escape(raw_json)}</pre></div>
</div></body></html>
"""



def iter_days(start_day: str, end_day: str):
    start = datetime.fromisoformat(start_day).date()
    end = datetime.fromisoformat(end_day).date()
    cur = start
    while cur <= end:
        yield cur.isoformat()
        cur += timedelta(days=1)


def stable_bigint(key: str) -> int:
    digest = hashlib.sha256(key.encode("utf-8")).digest()
    value = int.from_bytes(digest[:8], "big", signed=False)
    return value % 9_223_372_036_854_775_807


def _render_result_line(item: dict) -> str:
    day = html.escape(str(item.get("day", "")))
    total_tokens = item.get("total_tokens", 0)
    total_cost = item.get("total_cost")
    cost_currency = item.get("cost_currency") or ""
    bill_amount = item.get("bill_amount")
    bill_gross = item.get("bill_gross_amount")
    bill_currency = item.get("bill_currency") or ""
    if bill_amount is not None:
        suffix = f" {bill_currency}".rstrip()
        if bill_gross is None:
            text = f"{day}: amount={bill_amount}{suffix}"
        else:
            text = f"{day}: amount={bill_amount} gross={bill_gross}{suffix}"
    elif total_cost is None:
        text = f"{day}: total_tokens={total_tokens}"
    else:
        suffix = f" {cost_currency}".rstrip()
        text = f"{day}: total_tokens={total_tokens} cost={total_cost}{suffix}"
    return f"<li>{html.escape(text)}</li>"


def upsert_weekly_with_id(sb, vendor: str, week_start: str, week_end: str, token_total: int):
    row = {
        "id": stable_bigint(f"week:{vendor}:{week_start}:{week_end}"),
        "vendor_code": vendor,
        "week_start": week_start,
        "week_end": week_end,
        "token_total": token_total,
    }
    return (
        sb.schema("financial_hub_prod")
        .table("llm_token_weekly_usage")
        .upsert(row, on_conflict="vendor_code,week_start,week_end")
        .execute()
    )


def upsert_bill_weekly(sb, vendor: str, week_start: str, week_end: str, amount: float, is_ai: bool = True, currency: str = "CNY"):
    """写入周账单汇总表"""
    row = {
        "id": stable_bigint(f"bill_week:{vendor}:{week_start}:{week_end}:{is_ai}"),
        "vendor_code": vendor,
        "week_start": week_start,
        "week_end": week_end,
        "is_ai_cost": is_ai,
        "amount": amount,
        "gross_amount": amount,
        "currency": currency,
    }
    return (
        sb.schema("financial_hub_prod")
        .table("bill_weekly_summary")
        .upsert(row, on_conflict="vendor_code,week_start,week_end,is_ai_cost")
        .execute()
    )


def upsert_monthly_with_id(sb, vendor: str, month: str, token_total: int):
    row = {
        "id": stable_bigint(f"month:{vendor}:{month}"),
        "vendor_code": vendor,
        "month": month,
        "token_total": token_total,
    }
    return (
        sb.schema("financial_hub_prod")
        .table("llm_token_monthly_usage")
        .upsert(row, on_conflict="vendor_code,month")
        .execute()
    )


def delete_existing_daily(sb, day_str: str, vendor: str, project_id: str | None):
    query = (
        sb.schema("financial_hub_prod")
        .table("llm_token_daily_usage")
        .delete()
        .eq("day", day_str)
        .eq("vendor", vendor)
    )
    if project_id:
        query = query.eq("project_id", project_id)
    return query.execute()


def delete_bill_daily_by_vendor(sb, vendor_code: str, billing_date: str):
    """删除指定 vendor 和日期的所有账单记录（不论 is_ai_cost）"""
    return (
        sb.schema("financial_hub_prod")
        .table("bill_daily_summary")
        .delete()
        .eq("vendor_code", vendor_code)
        .eq("billing_date", billing_date)
        .execute()
    )


def delete_aliyun_bill_daily(sb, billing_date: str):
    return (
        sb.schema("financial_hub_prod")
        .table("aliyun_bill_daily")
        .delete()
        .eq("billing_date", billing_date)
        .execute()
    )


def upsert_bill_daily_summary(
    sb,
    vendor_code: str,
    billing_date: str,
    amount: float,
    gross_amount: float,
    currency: str,
    is_ai_cost: bool = False,
):
    row = {
        "vendor_code": vendor_code,
        "billing_date": billing_date,
        "is_ai_cost": is_ai_cost,
        "amount": float(amount or 0),
        "gross_amount": float(gross_amount or 0),
        "currency": currency or "USD",
    }
    return (
        sb.schema("financial_hub_prod")
        .table("bill_daily_summary")
        .upsert(row, on_conflict="vendor_code,billing_date,is_ai_cost")
        .execute()
    )


def upsert_bill_weekly_summary(
    sb,
    vendor_code: str,
    week_start: str,
    week_end: str,
    amount: float,
    gross_amount: float,
    currency: str,
    is_ai_cost: bool,
):
    row = {
        "vendor_code": vendor_code,
        "week_start": week_start,
        "week_end": week_end,
        "is_ai_cost": is_ai_cost,
        "amount": float(amount or 0),
        "gross_amount": float(gross_amount or 0),
        "currency": currency or "CNY",
    }
    return (
        sb.schema("financial_hub_prod")
        .table("bill_weekly_summary")
        .upsert(row, on_conflict="vendor_code,week_start,week_end,is_ai_cost")
        .execute()
    )


def upsert_bill_monthly_summary(
    sb,
    vendor_code: str,
    month: str,
    amount: float,
    gross_amount: float,
    currency: str,
    is_ai_cost: bool,
):
    row = {
        "vendor_code": vendor_code,
        "month": month,
        "is_ai_cost": is_ai_cost,
        "amount": float(amount or 0),
        "gross_amount": float(gross_amount or 0),
        "currency": currency or "CNY",
    }
    return (
        sb.schema("financial_hub_prod")
        .table("bill_monthly_summary")
        .upsert(row, on_conflict="vendor_code,month,is_ai_cost")
        .execute()
    )


def sum_bill_daily(sb, vendor_code: str, is_ai_cost: bool, start_date: str, end_date: str):
    resp = (
        sb.schema("financial_hub_prod")
        .table("bill_daily_summary")
        .select("amount,gross_amount,currency")
        .eq("vendor_code", vendor_code)
        .eq("is_ai_cost", is_ai_cost)
        .gte("billing_date", start_date)
        .lte("billing_date", end_date)
        .execute()
    )
    rows = resp.data or []
    if not rows:
        return None
    amount_total = float(sum(float(r.get("amount") or 0) for r in rows))
    gross_total = float(sum(float(r.get("gross_amount") or 0) for r in rows))
    currencies = {r.get("currency") for r in rows if r.get("currency")}
    if len(currencies) == 1:
        currency = next(iter(currencies))
    elif len(currencies) > 1:
        currency = "MIXED"
    else:
        currency = "CNY"
    return {
        "amount": _normalize_amount(amount_total),
        "gross": _normalize_amount(gross_total),
        "currency": currency,
    }


def _strip_pretax_gross(rows: list[dict]) -> list[dict]:
    cleaned = []
    for row in rows:
        item = dict(row)
        item.pop("pretax_gross_amount", None)
        cleaned.append(item)
    return cleaned


def _is_missing_gross_error(exc: Exception) -> bool:
    message = ""
    if isinstance(exc, APIError):
        try:
            message = (exc.args[0] or {}).get("message", "")
        except Exception:
            message = str(exc)
    else:
        message = str(exc)
    return "pretax_gross_amount" in message


AI_PRODUCT_SET = {
    "sfm",
}


def is_ai_product(product_code: str) -> bool:
    return (product_code or "").strip().lower() in AI_PRODUCT_SET


def _parse_items(items):
    items_list = []
    if isinstance(items, list):
        items_list = items
    elif hasattr(items, "to_map"):
        mapped = items.to_map()
        for key in ["Item", "item", "Items", "items"]:
            if key in mapped:
                value = mapped[key]
                if isinstance(value, list):
                    items_list = value
                elif isinstance(value, dict):
                    items_list = [value]
                break
    return items_list


def _aliyun_bss_client():
    try:
        from alibabacloud_bssopenapi20171214.client import Client as BssClient
        from alibabacloud_tea_openapi import models as open_api_models
        from alibabacloud_bssopenapi20171214 import models as bss_models
    except ImportError as exc:
        raise RuntimeError("missing aliyun bss sdk") from exc

    ak = os.getenv("ALIYUN_ACCESS_KEY_ID")
    sk = os.getenv("ALIYUN_ACCESS_KEY_SECRET")
    if not ak or not sk:
        raise RuntimeError("missing env: ALIYUN_ACCESS_KEY_ID/ALIYUN_ACCESS_KEY_SECRET")

    config = open_api_models.Config(
        access_key_id=ak,
        access_key_secret=sk,
        endpoint="business.aliyuncs.com",
    )
    return BssClient(config), bss_models


def _fetch_aliyun_bill_rows(client, bss_models, billing_date):
    billing_cycle = billing_date.strftime("%Y-%m")
    request = bss_models.QueryAccountBillRequest(
        billing_cycle=billing_cycle,
        billing_date=billing_date.strftime("%Y-%m-%d"),
        granularity="DAILY",
        is_group_by_product=True,
        page_num=1,
        page_size=200,
    )
    response = client.query_account_bill(request)
    body = response.body
    data = body.data
    if not data:
        return [], {"amount": 0.0, "gross": 0.0, "currency": "CNY"}

    items_list = _parse_items(data.items)
    if not items_list:
        return [], {"amount": 0.0, "gross": 0.0, "currency": "CNY"}

    aggregated = {}
    billing_date_str = billing_date.strftime("%Y-%m-%d")
    for it in items_list:
        product_code = it.get("ProductCode")
        product_name = it.get("ProductName")
        currency = it.get("Currency")
        is_ai = is_ai_product(product_code)
        key = (
            billing_date_str,
            billing_cycle,
            product_code,
            product_name,
            currency,
            is_ai,
        )
        amount = float(it.get("PretaxAmount") or 0)
        gross = float(it.get("PretaxGrossAmount") or 0)
        if key not in aggregated:
            row = {
                "billing_date": billing_date_str,
                "billing_cycle": billing_cycle,
                "product_code": product_code,
                "product_name": product_name,
                "pretax_amount": amount,
                "pretax_gross_amount": gross,
                "currency": currency,
                "is_ai_cost": is_ai,
            }
            aggregated[key] = row
        else:
            aggregated[key]["pretax_amount"] += amount
            aggregated[key]["pretax_gross_amount"] += gross

    rows = list(aggregated.values())
    amount_total = float(sum(r.get("pretax_amount") or 0 for r in rows))
    gross_total = float(sum(r.get("pretax_gross_amount") or 0 for r in rows))
    currencies = {r.get("currency") for r in rows if r.get("currency")}
    if len(currencies) == 1:
        currency = next(iter(currencies))
    elif len(currencies) > 1:
        currency = "MIXED"
    else:
        currency = "CNY"
    return rows, {"amount": amount_total, "gross": gross_total, "currency": currency}


def _aws_ce_client():
    try:
        import boto3
        from botocore.config import Config
    except ImportError as exc:
        raise RuntimeError("missing aws sdk (boto3)") from exc

    access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    session_token = (os.environ.get("AWS_SESSION_TOKEN") or "").strip() or None
    region_name = (
        os.environ.get("AWS_REGION")
        or os.environ.get("AWS_DEFAULT_REGION")
        or "cn-north-1"
    )
    return boto3.client(
        "ce",
        region_name=region_name,
        aws_access_key_id=access_key_id or None,
        aws_secret_access_key=secret_access_key or None,
        aws_session_token=session_token or None,
        config=Config(retries={"max_attempts": 5, "mode": "standard"}),
    )


def _volcengine_credentials():
    access_key = os.environ.get("VOLCENGINE_ACCESS_KEY")
    secret_key = os.environ.get("VOLCENGINE_SECRET_KEY")
    region = os.environ.get("VOLCENGINE_REGION", "cn-beijing")
    security_token = os.environ.get("VOLCENGINE_SECURITY_TOKEN") or None
    if not access_key or not secret_key:
        raise RuntimeError("missing env: VOLCENGINE_ACCESS_KEY/VOLCENGINE_SECRET_KEY")
    return access_key, secret_key, region, security_token


def _volcengine_headers():
    return {
        "Content-Type": "application/json; charset=utf-8",
        "User-Agent": "volcengine-python-sdk",
    }


def _hmac_sha256(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _volcengine_sign_headers(
    method: str,
    base_url: str,
    params: dict,
    body: str,
    access_key: str,
    secret_key: str,
    region: str,
    service: str = "billing",
    security_token: str | None = None,
):
    parsed = urlparse(base_url)
    host = parsed.netloc or "open.volcengineapi.com"
    base_path = parsed.path or "/"
    existing_params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query_params = {**existing_params, **params}
    canonical_query = "&".join(
        f"{quote(k, safe='-_.~')}={quote(str(query_params[k]), safe='-_.~')}"
        for k in sorted(query_params.keys())
    )

    now = datetime.utcnow()
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    datestamp = now.strftime("%Y%m%d")
    scope = f"{datestamp}/{region}/{service}/request"

    payload_hash = hashlib.sha256(body.encode("utf-8")).hexdigest()
    canonical_headers = (
        f"content-type:application/json; charset=utf-8\n"
        f"host:{host}\n"
        f"x-content-sha256:{payload_hash}\n"
        f"x-date:{amz_date}\n"
    )
    signed_headers = "content-type;host;x-content-sha256;x-date"
    canonical_request = (
        f"{method.upper()}\n"
        f"{base_path}\n"
        f"{canonical_query}\n"
        f"{canonical_headers}\n"
        f"{signed_headers}\n"
        f"{payload_hash}"
    )

    canonical_request_hash = hashlib.sha256(
        canonical_request.encode("utf-8")
    ).hexdigest()
    string_to_sign = f"HMAC-SHA256\n{amz_date}\n{scope}\n{canonical_request_hash}"

    k_date = _hmac_sha256(secret_key.encode("utf-8"), datestamp)
    k_region = _hmac_sha256(k_date, region)
    k_service = _hmac_sha256(k_region, service)
    k_signing = _hmac_sha256(k_service, "request")
    signature = hmac.new(
        k_signing, string_to_sign.encode("utf-8"), hashlib.sha256
    ).hexdigest()

    authorization = (
        f"HMAC-SHA256 Credential={access_key}/{scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )

    headers = _volcengine_headers()
    headers.update(
        {
            "Host": host,
            "X-Date": amz_date,
            "X-Content-Sha256": payload_hash,
            "Authorization": authorization,
        }
    )
    if security_token:
        headers["X-Security-Token"] = security_token

    signed_url = urlunparse(
        (parsed.scheme or "https", host, base_path, "", canonical_query, "")
    )
    return signed_url, headers


def _fetch_volcengine_bill_daily(
    billing_date, *, limit: int = 100, ignore_zero: int = 0, verbose: bool = False
):
    headers = _volcengine_headers()
    access_key, secret_key, region, security_token = _volcengine_credentials()
    bill_period = billing_date.strftime("%Y-%m")
    expense_date = billing_date.strftime("%Y-%m-%d")
    offset = 0
    rows = []
    while True:
        payload = {
            "BillPeriod": bill_period,
            "ExpenseDate": expense_date,
            "GroupPeriod": 1,
            "GroupTerm": 0,
            "Limit": int(limit),
            "Offset": int(offset),
            "NeedRecordNum": 1,
            "IgnoreZero": int(ignore_zero),
        }
        signed_url, signed_headers = _volcengine_sign_headers(
            "POST",
            VOLCENGINE_API,
            {"Action": "ListBillDetail", "Version": "2022-01-01"},
            json.dumps(payload, ensure_ascii=False),
            access_key,
            secret_key,
            region,
            service="billing",
            security_token=security_token,
        )
        if verbose:
            print(f"[VOLCENGINE] request url: {signed_url}")
            print(f"[VOLCENGINE] request body: {json.dumps(payload, ensure_ascii=False)}")
        resp = requests.post(signed_url, headers=signed_headers, json=payload, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(f"volcengine http {resp.status_code}: {resp.text[:200]}")
        body = resp.json()
        err = (body.get("ResponseMetadata") or {}).get("Error")
        if err:
            raise RuntimeError(f"volcengine api error: {err}")
        if verbose and offset == 0:
            print(f"[VOLCENGINE] response: {json.dumps(body, ensure_ascii=False)}")
        result = body.get("Result") or {}
        batch = result.get("List") or []
        rows.extend(batch)
        total = result.get("Total")
        if not batch:
            break
        offset += len(batch)
        if total and isinstance(total, int) and total > 0 and offset >= total:
            break
    # 火山引擎 AI 产品关键词（产品名或元素名包含这些则为 AI）
    VOLC_AI_KEYWORDS = {"豆包", "doubao", "模型推理", "大模型", "ark", "maas", "token"}
    
    amount_total = 0.0
    gross_total = 0.0
    ai_amount = 0.0
    ai_gross = 0.0
    non_ai_amount = 0.0
    non_ai_gross = 0.0
    currencies = set()
    token_total = 0
    token_input = 0
    token_output = 0
    token_rows = []
    for item in rows:
        gross = _safe_float(item.get("OriginalBillAmount"))
        amount = _safe_float(item.get("PayableAmount"))
        if amount == 0.0:
            amount = _safe_float(item.get("PreferentialBillAmount"))
        if amount == 0.0:
            amount = _safe_float(item.get("PretaxAmount"))
        amount_total += amount
        gross_total += gross
        currency = item.get("Currency")
        if currency:
            currencies.add(currency)
        
        # 判断是否为 AI 产品
        product = (item.get("Product") or "").lower()
        element = (item.get("Element") or "").lower()
        expand_field = (item.get("ExpandField") or "").lower()
        subject_name = (item.get("SubjectName") or "").lower()  # 科目名称
        instance_name = (item.get("InstanceName") or "").lower()  # 实例名称
        unit_raw = item.get("Unit") or ""
        unit = unit_raw.lower()
        combined = f"{product} {element} {expand_field} {subject_name} {instance_name} {unit}"
        is_ai = any(kw in combined for kw in VOLC_AI_KEYWORDS)
        # 调试：打印第一条账单的所有字段
        if verbose and amount_total == 0:
            print(f"[DEBUG] volcengine item keys: {list(item.keys())}")
            print(f"[DEBUG] Product={item.get('Product')} Element={item.get('Element')} SubjectName={item.get('SubjectName')} Unit={item.get('Unit')}")
        
        if is_ai:
            ai_amount += amount
            ai_gross += gross
        else:
            non_ai_amount += amount
            non_ai_gross += gross
        
        # 提取 Token 用量
        if "token" in unit:
            count = _safe_float(item.get("Count") or 0)
            # 根据单位转换：千tokens 乘 1000，否则直接取值
            is_kilo = "千" in unit_raw or unit.startswith("k") or "ktoken" in unit
            if is_kilo:
                tokens = int(count * 1000)
            else:
                tokens = int(count)
            token_total += tokens
            element_name = item.get("Element") or ""
            if "输入" in element_name or "input" in element_name.lower():
                token_input += tokens
            elif "输出" in element_name or "output" in element_name.lower():
                token_output += tokens
            token_rows.append({
                "element": element_name,
                "model": item.get("ExpandField") or "",
                "tokens": tokens,
                "count": count,
                "unit": item.get("Unit"),
            })
    if len(currencies) == 1:
        currency = next(iter(currencies))
    elif len(currencies) > 1:
        currency = "MIXED"
    else:
        currency = "CNY"
    summary = {
        "amount": _normalize_amount(amount_total),
        "gross": _normalize_amount(gross_total),
        "ai_amount": _normalize_amount(ai_amount),
        "ai_gross": _normalize_amount(ai_gross),
        "non_ai_amount": _normalize_amount(non_ai_amount),
        "non_ai_gross": _normalize_amount(non_ai_gross),
        "currency": currency,
        "rows": len(rows),
        "total": total if isinstance(total, int) else None,
        "token_total": token_total,
        "token_input": token_input,
        "token_output": token_output,
        "token_rows": token_rows,
    }
    return rows, summary


def _fetch_aws_bill_daily(client, billing_date, *, include_raw: bool = False):
    start = billing_date.strftime("%Y-%m-%d")
    end = (billing_date + timedelta(days=1)).strftime("%Y-%m-%d")
    resp = client.get_cost_and_usage(
        TimePeriod={"Start": start, "End": end},
        Granularity="DAILY",
        Metrics=["UnblendedCost", "NetUnblendedCost"],
        GroupBy=[{"Type": "DIMENSION", "Key": "RECORD_TYPE"}],
    )
    results = resp.get("ResultsByTime", [])
    if not results:
        summary = {
            "amount": 0.0,
            "gross": 0.0,
            "currency": "USD",
            "record_type_totals": {},
            "usage_amount": 0.0,
            "credit_amount": 0.0,
        }
        if include_raw:
            summary["raw_response"] = resp
        return summary
    first = results[0]
    total = first.get("Total") or {}
    gross = total.get("UnblendedCost", {})
    net = total.get("NetUnblendedCost", {})
    gross_amount = float(gross.get("Amount") or 0)
    net_amount = float(net.get("Amount") or 0)
    record_type_totals = {}
    record_type_net_totals = {}
    currency = gross.get("Unit") or net.get("Unit") or ""
    raw_net_sum = 0.0
    for group in first.get("Groups", []) or []:
        keys = group.get("Keys") or []
        record_type = keys[0] if keys else "UNKNOWN"
        metrics = group.get("Metrics", {})
        unblended = metrics.get("UnblendedCost", {}) or {}
        net_unblended = metrics.get("NetUnblendedCost", {}) or {}
        unblended_amount = float(unblended.get("Amount") or 0)
        net_amount_value = float(net_unblended.get("Amount") or 0)
        record_type_totals[record_type] = _normalize_amount(unblended_amount)
        record_type_net_totals[record_type] = _normalize_amount(net_amount_value)
        raw_net_sum += net_amount_value
        if not currency:
            currency = unblended.get("Unit") or net_unblended.get("Unit") or ""
    if not currency:
        currency = "USD"
    if record_type_totals:
        excluded_types = {"Credit", "Discount", "Refund"}
        gross_from_record_types = sum(
            value
            for key, value in record_type_totals.items()
            if key not in excluded_types and value > 0
        )
        if gross_from_record_types > 0:
            gross_amount = float(gross_from_record_types)
    if not gross_amount and record_type_totals:
        gross_amount = float(record_type_totals.get("Usage") or 0)
    if not net_amount and record_type_net_totals:
        net_amount = float(raw_net_sum)
    usage_amount = record_type_totals.get("Usage", 0.0)
    tax_amount = record_type_totals.get("Tax", 0.0)
    credit_amount = record_type_totals.get("Credit", 0.0)
    # gross = Usage + Tax (不含 Credit/Discount/Refund)
    # net = Usage + Tax + Credit (Credit 通常是负数)
    if record_type_totals:
        # 包含费用类型：Usage, Tax, Support, Fee 等（排除 Credit, Discount, Refund）
        excluded_types = {"Credit", "Discount", "Refund"}
        gross_amount = sum(
            value for key, value in record_type_totals.items()
            if key not in excluded_types
        )
        # net = 所有类型的总和
        net_amount = sum(record_type_totals.values())
    summary = {
        "amount": _normalize_amount(net_amount),
        "gross": _normalize_amount(gross_amount),
        "currency": currency,
        "raw_amount": net_amount,
        "raw_gross": gross_amount,
        "record_type_totals": record_type_totals,
        "record_type_net_totals": record_type_net_totals,
        "usage_amount": usage_amount,
        "tax_amount": tax_amount,
        "credit_amount": credit_amount,
    }
    if include_raw:
        summary["raw_response"] = resp
    return summary


def _parse_cookie_header(cookie_header: str) -> dict:
    out = {}
    for part in cookie_header.split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        k, v = part.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def _bj_date_from_ms(ms: int | float | str | None) -> str | None:
    if ms is None:
        return None
    try:
        ts = int(ms) / 1000
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp(ts, tz=BJ).date().isoformat()


def _moonshot_headers(bearer_token: str, cookie: str | None = None) -> dict:
    # 自动去掉 "Bearer " 前缀（如果用户带上了）
    token = bearer_token.strip()
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    headers = {
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Authorization": f"Bearer {token}",
        "Referer": "https://platform.moonshot.cn/console/fee-detail",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    }
    if cookie:
        headers["Cookie"] = cookie
    return headers


def _fetch_moonshot_daily_bills(
    bearer_token: str,
    org_id: str,
    start_day: str,
    end_day: str,
    cookie: str | None = None,
) -> list[dict]:
    start_date = datetime.fromisoformat(start_day).replace(hour=0, minute=0, second=0, tzinfo=BJ)
    end_date = datetime.fromisoformat(end_day).replace(hour=23, minute=59, second=59, tzinfo=BJ)
    start_ms = int(start_date.timestamp() * 1000)
    end_ms = int(end_date.timestamp() * 1000)
    params = {
        "start": start_ms,
        "end": end_ms,
        "pid": "",
        "oid": org_id,
        "endpoint": "organizationDailyBills",
    }
    headers = _moonshot_headers(bearer_token, cookie=cookie)
    resp = requests.get(KIMI_API, params=params, headers=headers, timeout=20)
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("code") != 0:
        raise RuntimeError(payload.get("message") or f"moonshot api error: code={payload.get('code')}")
    return payload.get("data") or []


# ==================== TextIn ====================

def _textin_headers(token: str) -> dict:
    return {
        "accept": "application/json",
        "accept-language": "zh-CN,zh;q=0.9",
        "cache-control": "no-cache",
        "content-type": "application/json;charset=UTF-8",
        "origin": "https://www.textin.com",
        "pragma": "no-cache",
        "referer": "https://www.textin.com/",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "token": token,
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    }


def _fetch_textin_consume(
    token: str,
    start_day: str,
    end_day: str,
) -> list[dict]:
    """
    获取 TextIn 消费记录
    返回 item 列表，每个 item 包含 time (Unix秒) 和 t_coin (消费金额)
    """
    start_date = datetime.fromisoformat(start_day).replace(hour=0, minute=0, second=0, tzinfo=BJ)
    end_date = datetime.fromisoformat(end_day).replace(hour=23, minute=59, second=59, tzinfo=BJ)
    start_ts = int(start_date.timestamp())
    end_ts = int(end_date.timestamp())

    headers = _textin_headers(token)
    all_items = []
    page_num = 1
    page_size = 50

    while True:
        body = {
            "page_num": page_num,
            "page_size": page_size,
            "start_time": start_ts,
            "end_time": end_ts,
        }
        print(f"[DEBUG] textin request: url={TEXTIN_API}, body={body}, token={headers.get('token', '')[:10]}...")
        resp = requests.post(TEXTIN_API, json=body, headers=headers, timeout=20)
        print(f"[DEBUG] textin response: status={resp.status_code}")
        resp.raise_for_status()
        payload = resp.json()

        if payload.get("msg") != "success":
            raise RuntimeError(payload.get("msg") or f"textin api error")

        data = payload.get("data") or {}
        items = data.get("item") or []
        if not items:
            break

        all_items.extend(items)

        # 如果返回的数量小于 page_size，说明没有更多了
        if len(items) < page_size:
            break
        page_num += 1

    return all_items


def _aggregate_textin_daily(items: list[dict]) -> dict[str, dict]:
    """
    将 TextIn 消费记录按天聚合
    返回 {日期: {amount, gross, currency, raw}}
    """
    daily = {}
    for item in items:
        ts = int(item.get("time") or 0)
        if ts == 0:
            continue
        # 转换为北京时间日期
        dt = datetime.fromtimestamp(ts, tz=BJ)
        date_str = dt.strftime("%Y-%m-%d")
        t_coin = float(item.get("t_coin") or 0)

        if date_str not in daily:
            daily[date_str] = {
                "amount": 0.0,
                "gross": 0.0,
                "currency": "CNY",
                "raw": [],
            }
        daily[date_str]["amount"] += t_coin
        daily[date_str]["gross"] += t_coin
        daily[date_str]["raw"].append(item)

    return daily


# ==================== DeepSeek ====================

def _deepseek_headers(auth_token: str, cookie: str = None) -> dict:
    """构建 DeepSeek API 请求头"""
    token = auth_token.strip()
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    headers = {
        "accept": "*/*",
        "authorization": f"Bearer {token}",
        "sec-ch-ua-platform": '"Windows"',
        "sec-ch-ua": '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
        "sec-ch-ua-mobile": "?0",
        "x-app-version": "20240425.0",
        "Referer": "https://platform.deepseek.com/usage",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
    }
    if cookie:
        headers["Cookie"] = cookie.strip()
    return headers


def _fetch_deepseek_cost(auth_token: str, year: int, month: int, cookie: str = None) -> dict:
    """获取 DeepSeek 平台月度消费数据"""
    headers = _deepseek_headers(auth_token, cookie)
    params = {"year": year, "month": month}
    
    resp = requests.get(DEEPSEEK_COST_API, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"DeepSeek API error: code={data.get('code')}, msg={data.get('msg')}")
    
    return data


def _parse_deepseek_cost(data: dict) -> dict:
    """解析 DeepSeek 消费数据，返回 {total_cost, currency, models, daily}"""
    biz_data_list = data.get("data", {}).get("biz_data", [])
    if not biz_data_list:
        return {"total_cost": 0.0, "currency": "CNY", "models": {}, "daily": {}}
    
    biz_data = biz_data_list[0]
    currency = biz_data.get("currency", "CNY")
    
    total_list = biz_data.get("total", [])
    models = {}
    total_cost = 0.0
    
    for model_data in total_list:
        model_name = model_data.get("model", "unknown")
        usage_list = model_data.get("usage", [])
        
        model_cost = {"prompt_cache_hit": 0.0, "prompt_cache_miss": 0.0, "response_token": 0.0}
        
        for usage in usage_list:
            usage_type = usage.get("type", "")
            amount = _safe_float(usage.get("amount"))
            
            if usage_type == "PROMPT_CACHE_HIT_TOKEN":
                model_cost["prompt_cache_hit"] = amount
            elif usage_type == "PROMPT_CACHE_MISS_TOKEN":
                model_cost["prompt_cache_miss"] = amount
            elif usage_type == "RESPONSE_TOKEN":
                model_cost["response_token"] = amount
        
        model_total = model_cost["prompt_cache_hit"] + model_cost["prompt_cache_miss"] + model_cost["response_token"]
        model_cost["total"] = round(model_total, 4)
        models[model_name] = model_cost
        total_cost += model_total
    
    days_list = biz_data.get("days", [])
    daily = {}
    
    for day_data in days_list:
        date_str = day_data.get("date", "")
        day_models = day_data.get("data", [])
        
        day_cost = 0.0
        for model_data in day_models:
            usage_list = model_data.get("usage", [])
            for usage in usage_list:
                usage_type = usage.get("type", "")
                amount = _safe_float(usage.get("amount"))
                if usage_type in ("PROMPT_CACHE_HIT_TOKEN", "PROMPT_CACHE_MISS_TOKEN", "RESPONSE_TOKEN"):
                    day_cost += amount
        
        daily[date_str] = {"amount": round(day_cost, 4), "gross": round(day_cost, 4), "currency": currency}
    
    return {"total_cost": round(total_cost, 4), "currency": currency, "models": models, "daily": daily}


def _fetch_deepseek_amount(auth_token: str, year: int, month: int, cookie: str = None) -> dict:
    """获取 DeepSeek 平台月度 Token 用量数据"""
    headers = _deepseek_headers(auth_token, cookie)
    params = {"year": year, "month": month}
    
    resp = requests.get(DEEPSEEK_AMOUNT_API, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"DeepSeek API error: code={data.get('code')}, msg={data.get('msg')}")
    
    return data


def _parse_deepseek_amount(data: dict) -> dict:
    """解析 DeepSeek Token 用量数据，返回 {total_tokens, models, daily}"""
    biz_data = data.get("data", {}).get("biz_data", {})
    if not biz_data:
        return {"total_tokens": 0, "models": {}, "daily": {}}
    
    total_list = biz_data.get("total", [])
    models = {}
    total_tokens = 0
    
    for model_data in total_list:
        model_name = model_data.get("model", "unknown")
        usage_list = model_data.get("usage", [])
        
        model_tokens = {
            "prompt_cache_hit": 0,
            "prompt_cache_miss": 0,
            "response_token": 0,
            "request_count": 0
        }
        
        for usage in usage_list:
            usage_type = usage.get("type", "")
            amount = int(usage.get("amount", "0") or 0)
            
            if usage_type == "PROMPT_CACHE_HIT_TOKEN":
                model_tokens["prompt_cache_hit"] = amount
            elif usage_type == "PROMPT_CACHE_MISS_TOKEN":
                model_tokens["prompt_cache_miss"] = amount
            elif usage_type == "RESPONSE_TOKEN":
                model_tokens["response_token"] = amount
            elif usage_type == "REQUEST":
                model_tokens["request_count"] = amount
        
        model_total = model_tokens["prompt_cache_hit"] + model_tokens["prompt_cache_miss"] + model_tokens["response_token"]
        model_tokens["total"] = model_total
        models[model_name] = model_tokens
        total_tokens += model_total
    
    days_list = biz_data.get("days", [])
    daily = {}
    
    for day_data in days_list:
        date_str = day_data.get("date", "")
        day_models = day_data.get("data", [])
        
        day_tokens = 0
        day_requests = 0
        for model_data in day_models:
            usage_list = model_data.get("usage", [])
            for usage in usage_list:
                usage_type = usage.get("type", "")
                amount = int(usage.get("amount", "0") or 0)
                if usage_type in ("PROMPT_CACHE_HIT_TOKEN", "PROMPT_CACHE_MISS_TOKEN", "RESPONSE_TOKEN"):
                    day_tokens += amount
                elif usage_type == "REQUEST":
                    day_requests += amount
        
        daily[date_str] = {"total_tokens": day_tokens, "request_count": day_requests}
    
    return {"total_tokens": total_tokens, "models": models, "daily": daily}


def _tianyancha_headers(cookie: str | None = None) -> dict:
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Connection": "keep-alive",
        "Referer": "https://open.tianyancha.com/console/data_order",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    }
    if cookie:
        headers["Cookie"] = cookie
    return headers


def _extract_tianyancha_list(payload: dict) -> list[dict]:
    data = payload.get("data")
    if isinstance(data, dict):
        for key in ("list", "items", "records", "rows"):
            if isinstance(data.get(key), list):
                return data.get(key) or []
    if isinstance(data, list):
        return data
    for key in ("list", "items", "records", "rows"):
        if isinstance(payload.get(key), list):
            return payload.get(key) or []
    return []


def _tianyancha_raise_if_error(payload: dict):
    if payload.get("success") is False:
        raise RuntimeError(payload.get("message") or "tianyancha api error")
    state = payload.get("state")
    if state == "ok":
        return  # 天眼查成功返回 state=ok
    code = payload.get("code")
    if code not in (None, 0, "0", 200, "200", 2000, "2000"):
        raise RuntimeError(payload.get("message") or f"tianyancha api error: code={code}")


def _fetch_tianyancha_orders(
    auth_secret: str,
    start_day: str,
    end_day: str,
    *,
    page_size: int = 50,
    cookie: str | None = None,
) -> list[dict]:
    page_num = 1
    results: list[dict] = []
    start_date = datetime.fromisoformat(start_day).date()
    end_date = datetime.fromisoformat(end_day).date()
    while True:
        params = {"pn": page_num, "ps": page_size, "billingMode": 0, "authSecret": auth_secret}
        try:
            headers = _tianyancha_headers(cookie=cookie)
            resp = requests.get(
                TIANYANCHA_API,
                params=params,
                headers=headers,
                timeout=20,
            )
            resp.raise_for_status()
        except requests.HTTPError as exc:
            message = ""
            if exc.response is not None:
                message = exc.response.text or ""
            raise RuntimeError(f"{exc} {message}".strip()) from exc
        payload = resp.json()
        _tianyancha_raise_if_error(payload)
        items = _extract_tianyancha_list(payload)
        if not items:
            break
        for item in items:
            detail = item.get("orderDetail") or item.get("orderDesc")
            if isinstance(detail, str):
                try:
                    detail = json.loads(detail)
                except json.JSONDecodeError:
                    detail = None
            item["_parsed_detail"] = detail
            usage_day = None
            if isinstance(detail, dict) and detail.get("startTime"):
                usage_day = _bj_date_from_ms(detail.get("startTime"))
            if not usage_day:
                usage_day = _bj_date_from_ms(item.get("createTime"))
                if usage_day:
                    usage_day = (
                        datetime.fromisoformat(usage_day) - timedelta(days=1)
                    ).date().isoformat()
            if not usage_day:
                continue
            item["_usage_day"] = usage_day
            usage_date = datetime.fromisoformat(usage_day).date()
            if usage_date < start_date or usage_date > end_date:
                continue
            results.append(item)
        if len(items) < page_size:
            break
        page_num += 1
        if page_num > 200:
            break
    return results


def _aggregate_tianyancha_daily(orders: list[dict]) -> dict[str, dict]:
    daily = {}
    for item in orders:
        detail = item.get("_parsed_detail")
        usage_day = item.get("_usage_day")
        if not usage_day:
            if isinstance(detail, dict) and detail.get("startTime"):
                usage_day = _bj_date_from_ms(detail.get("startTime"))
            if not usage_day:
                usage_day = _bj_date_from_ms(item.get("createTime"))
                if usage_day:
                    usage_day = (
                        datetime.fromisoformat(usage_day) - timedelta(days=1)
                    ).date().isoformat()
        if not usage_day:
            continue
        cents_total = 0
        if isinstance(detail, dict):
            counts = detail.get("iCountList") or []
            if isinstance(counts, list):
                for c in counts:
                    cents_total += int(c.get("cost") or 0)
        if cents_total <= 0:
            cents_total = abs(int(item.get("cost") or 0))
        amount = cents_total / 100
        currency = item.get("currency") or "CNY"
        if usage_day not in daily:
            daily[usage_day] = {
                "amount": 0.0,
                "gross": 0.0,
                "currency": currency,
                "rows": [],
            }
        daily[usage_day]["amount"] += amount
        daily[usage_day]["gross"] += amount
        daily[usage_day]["rows"].append(item)
    return daily


def _parse_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_amount(value: float, *, threshold: float = 0.005) -> float:
    if value is None:
        return 0.0
    try:
        val = float(value)
    except (TypeError, ValueError):
        return 0.0
    if abs(val) < threshold:
        return 0.0
    return round(val, 2)


def _safe_float(value):
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _first_cost_value(record: dict, cost_keys: list[str]):
    for key in cost_keys:
        if key in record:
            val = _parse_float(record.get(key))
            if val is not None:
                return val
    return None


def _stepfun_cost_keys() -> list[str]:
    override = os.getenv("STEPFUN_COST_KEYS", "").strip()
    if override:
        return [k.strip() for k in override.split(",") if k.strip()]
    return [
        "cost",
        "costAmount",
        "cost_amount",
        "fee",
        "feeAmount",
        "billingAmount",
        "chargeAmount",
        "amountYuan",
        "amountCny",
        "amountRmb",
    ]


def fetch_stepfun_usage(
    cookie_header: str,
    from_ms: int,
    to_ms: int,
    *,
    page: int = 1,
    page_size: int = 200,
    quota_type: str = "1",
    merge_by_time: int = 0,
):
    headers = {
        "accept": "*/*",
        "accept-language": "zh-CN,zh;q=0.9",
        "content-type": "application/json",
        "oasis-appid": os.environ["STEPFUN_OASIS_APPID"],
        "oasis-platform": os.environ.get("STEPFUN_OASIS_PLATFORM", "web"),
        "oasis-webid": os.environ["STEPFUN_OASIS_WEBID"],
        "origin": "https://platform.stepfun.com",
        "referer": "https://platform.stepfun.com/account-overview",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0",
    }

    payload = {
        "fromTime": str(from_ms),
        "toTime": str(to_ms),
        "pageSize": int(page_size),
        "page": int(page),
        "quotaType": str(quota_type),
        "mergeByTime": int(merge_by_time),
    }

    cookies = _parse_cookie_header(cookie_header)
    r = requests.post(STEPFUN_API, headers=headers, data=json.dumps(payload), timeout=30, cookies=cookies)
    if r.status_code != 200:
        print("HTTP", r.status_code)
        try:
            print("Response:", r.text[:500])
        except Exception:
            pass
        r.raise_for_status()
    return r.json()


def _sum_stepfun_metrics(records: list[dict]) -> dict:
    total_input = 0
    total_output = 0
    total_tokens = 0
    total_cache = 0
    total_image = 0
    total_websearch = 0
    total_tts = 0
    total_asr = 0
    total_cost = 0.0
    cost_keys = _stepfun_cost_keys()
    cost_hits = 0

    for r in records:
        total_input += int(r.get("inAmount", "0") or 0)
        total_output += int(r.get("outAmount", "0") or 0)
        total_tokens += int(r.get("amount", "0") or 0)
        total_cache += int(r.get("cacheAmount", "0") or 0)
        total_image += int(r.get("imageCount", "0") or 0)
        total_websearch += int(r.get("websearchCount", "0") or 0)
        total_tts += int(r.get("ttsWordCount", "0") or 0)
        total_asr += int(r.get("asrDurationSeconds", 0) or 0)
        cost_value = _first_cost_value(r, cost_keys)
        if cost_value is not None:
            total_cost += cost_value
            cost_hits += 1

    return {
        "input": total_input,
        "output": total_output,
        "tokens": total_tokens,
        "cache": total_cache,
        "image": total_image,
        "websearch": total_websearch,
        "tts": total_tts,
        "asr": total_asr,
        "cost": total_cost,
        "cost_hits": cost_hits,
    }


class Handler(BaseHTTPRequestHandler):
    def _is_logged_in(self):
        """检查是否已登录"""
        token = _get_cookie_token(self)
        return _verify_login_token(token)

    def _send_login_page(self, error=""):
        """发送登录页面"""
        page = render_login_page(error)
        self.send_response(200)
        self.send_header("content-type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(page.encode("utf-8"))

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        # 登录页面不需要认证
        if path == "/login":
            self._send_login_page()
            return

        # 以下接口需要登录
        if not self._is_logged_in():
            self._send_login_page()
            return
        
        if path == "/fetch_control":
            action = (query.get("action") or [""])[0]
            session_id = (query.get("session_id") or [""])[0]
            if session_id:
                if action == "pause":
                    _set_paused(session_id, True)
                elif action == "resume":
                    _set_paused(session_id, False)
                elif action == "stop":
                    _set_stopped(session_id, True)
            self.send_response(200)
            self.send_header("content-type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"ok":true}')
            return
        
        if path == "/fetch_stream":
            self._handle_stream(query)
            return
        
        if path not in ("/", "/index.html"):
            self.send_error(404)
            return
        default_day = yesterday_bj()
        page = render_form(default_day, default_day)
        self.send_response(200)
        self.send_header("content-type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(page.encode("utf-8"))
    
    def _send_sse(self, data: dict):
        try:
            msg = f"data: {json.dumps(data, ensure_ascii=False)}\n\n"
            self.wfile.write(msg.encode("utf-8"))
            self.wfile.flush()
        except Exception:
            pass
    
    def _handle_stream(self, form: dict):
        # 获取 session_id 用于独立控制
        session_id = (form.get("session_id") or [""])[0].strip()
        if not session_id:
            session_id = f"sess_{int(time.time() * 1000)}"
        self._session_id = session_id
        _get_session(session_id)  # 初始化 session 状态
        _reset_exec_state(session_id)
        
        self.send_response(200)
        self.send_header("content-type", "text/event-stream")
        self.send_header("cache-control", "no-cache")
        self.send_header("connection", "keep-alive")
        self.send_header("access-control-allow-origin", "*")
        self.end_headers()
        
        vendor = (form.get("vendor") or ["bailian"])[0].strip() or "bailian"
        cookie = (form.get("cookie") or [""])[0].strip()
        stepfun_cookie = (form.get("stepfun_cookie") or [""])[0].strip()
        auth_secret = (form.get("auth_secret") or [""])[0].strip()
        tian_cookie = (form.get("tian_cookie") or [""])[0].strip()
        moonshot_token = (form.get("moonshot_token") or [""])[0].strip()
        moonshot_org_id = (form.get("moonshot_org_id") or [""])[0].strip()
        moonshot_cookie = (form.get("moonshot_cookie") or [""])[0].strip()
        textin_token = (form.get("textin_token") or [""])[0].strip()
        # 获取第一个非空的 deepseek_auth 和 deepseek_cookie（因为有两个同名字段）
        deepseek_auth = next((v.strip() for v in (form.get("deepseek_auth") or []) if v.strip()), "")
        deepseek_cookie = next((v.strip() for v in (form.get("deepseek_cookie") or []) if v.strip()), "")
        start_day = (form.get("start_day") or [""])[0].strip()
        end_day = (form.get("end_day") or [""])[0].strip()
        retry_day = (form.get("retry_day") or [""])[0].strip()
        aws_dump_raw = (form.get("aws_dump_raw") or [""])[0].strip() == "1"
        # DMXAPI 手动录入参数
        dmx_week_start = (form.get("dmx_week_start") or [""])[0].strip()
        dmx_week_end = (form.get("dmx_week_end") or [""])[0].strip()
        dmx_input_tokens = (form.get("dmx_input_tokens") or [""])[0].strip()
        dmx_output_tokens = (form.get("dmx_output_tokens") or [""])[0].strip()
        dmx_amount = (form.get("dmx_amount") or [""])[0].strip()
        
        # 验证参数
        if vendor in ("bailian",) and not cookie:
            self._send_sse({"type": "error", "message": "cookie is required"})
            return
        if vendor == "stepfun" and not (stepfun_cookie or cookie):
            self._send_sse({"type": "error", "message": "stepfun cookie is required"})
            return
        if vendor == "tianyancha_bill":
            auth_secret = auth_secret or os.getenv("TIANYANCHA_AUTH_SECRET", "").strip()
            if not auth_secret:
                self._send_sse({"type": "error", "message": "missing authSecret"})
                return
            tian_cookie = tian_cookie or os.getenv("TIANYANCHA_COOKIE", "").strip()
            if not tian_cookie:
                self._send_sse({"type": "error", "message": "missing Cookie (天眼查需要登录Cookie)"})
                return
        if vendor == "moonshot_bill":
            moonshot_token = moonshot_token or os.getenv("MOONSHOT_BEARER_TOKEN", "").strip()
            moonshot_org_id = moonshot_org_id or os.getenv("MOONSHOT_ORG_ID", "").strip()
            moonshot_cookie = moonshot_cookie or os.getenv("MOONSHOT_COOKIE", "").strip()
            if not moonshot_token or not moonshot_org_id:
                self._send_sse({"type": "error", "message": "missing moonshot token or org_id"})
                return
        if vendor == "textin_bill":
            textin_token = textin_token or os.getenv("TEXTIN_TOKEN", "").strip()
            if not textin_token:
                self._send_sse({"type": "error", "message": "missing TextIn token"})
                return
        if vendor == "deepseek":
            deepseek_auth = deepseek_auth or os.getenv("DEEPSEEK_AUTH_TOKEN", "").strip()
            if not deepseek_auth:
                self._send_sse({"type": "error", "message": "missing DeepSeek authorization token"})
                return
        if vendor == "dmxapi_manual":
            if not dmx_week_start or not dmx_week_end:
                self._send_sse({"type": "error", "message": "周开始/结束日期必填"})
                return
            if not dmx_input_tokens and not dmx_output_tokens:
                self._send_sse({"type": "error", "message": "Token数量必填"})
                return
            # DMXAPI 手动录入：直接写入周汇总表，不走日期循环
            try:
                sb = supabase_client()
                input_tokens = int(dmx_input_tokens or 0)
                output_tokens = int(dmx_output_tokens or 0)
                total_tokens = input_tokens + output_tokens
                amount_usd = float(dmx_amount) if dmx_amount else None
                amount_cny = round(amount_usd * 7, 2) if amount_usd is not None else None  # 美元转人民币
                self._send_sse({"type": "log", "message": f"[INFO] 写入 DMXAPI 周汇总: {dmx_week_start} ~ {dmx_week_end}"})
                self._send_sse({"type": "log", "message": f"[INFO] Input: {input_tokens:,} | Output: {output_tokens:,} | Total: {total_tokens:,}"})
                # 写入 Token 周汇总表
                upsert_weekly_with_id(sb, "dmxapi", dmx_week_start, dmx_week_end, total_tokens)
                self._send_sse({"type": "log", "message": "✓ Token 写入成功 (llm_token_weekly_usage)"})
                # 写入账单周汇总表
                if amount_cny is not None:
                    self._send_sse({"type": "log", "message": f"[INFO] 消耗金额: ${amount_usd:.2f} → ¥{amount_cny:.2f}"})
                    upsert_bill_weekly(sb, "dmxapi", dmx_week_start, dmx_week_end, amount_cny, is_ai=True, currency="CNY")
                    self._send_sse({"type": "log", "message": "✓ 金额写入成功 (bill_weekly_summary)"})
                self._send_sse({"type": "done", "results": [{"week_start": dmx_week_start, "week_end": dmx_week_end, "total_tokens": total_tokens, "amount_usd": amount_usd, "amount_cny": amount_cny}]})
                _cleanup_session(self._session_id)
            except Exception as exc:
                self._send_sse({"type": "error", "message": f"写入失败: {exc}"})
                _cleanup_session(self._session_id)
            return
        if not start_day or not end_day:
            self._send_sse({"type": "error", "message": "start_day/end_day are required"})
            return
        
        sb = supabase_client()
        write_summary = os.getenv("BAILIAN_WRITE_SUMMARY", "1").strip() == "1"
        if retry_day == "1":
            day_list = [start_day]
        else:
            day_list = list(iter_days(start_day, end_day))
        
        total_days = len(day_list)
        self._send_sse({"type": "progress", "status": "开始拉取", "current": 0, "total": total_days, "percent": 0})
        self._send_sse({"type": "log", "message": f"[INFO] 开始拉取 {vendor}，共 {total_days} 天"})
        
        # 预处理（部分 vendor 需要预加载数据）
        bill_client = None
        bill_models = None
        aws_client = None
        volc_ready = False
        tian_orders = None
        tian_daily = None
        deepseek_daily = None
        deepseek_token_daily = None
        moonshot_daily = None
        textin_daily = None
        stepfun_daily = None
        
        try:
            if vendor == "aliyun_bill":
                bill_client, bill_models = _aliyun_bss_client()
            elif vendor == "aws_bill":
                aws_client = _aws_ce_client()
            elif vendor == "volcengine_bill":
                volc_ready = True
            elif vendor == "moonshot_bill":
                self._send_sse({"type": "log", "message": "[INFO] 正在获取月之暗面账单数据..."})
                moonshot_bills = _fetch_moonshot_daily_bills(
                    moonshot_token, moonshot_org_id, start_day, end_day, cookie=moonshot_cookie or None
                )
                moonshot_daily = {}
                for item in moonshot_bills:
                    date_str = item.get("date", "")[:10]
                    recharge_fee = int(item.get("recharge_fee") or 0)
                    amount = recharge_fee / 100000  # 单位是 1/100000 元
                    moonshot_daily[date_str] = {
                        "amount": amount, "gross": amount, "currency": "CNY", "raw": item
                    }
                self._send_sse({"type": "log", "message": f"[INFO] 获取到 {len(moonshot_daily)} 天数据"})
            elif vendor == "textin_bill":
                self._send_sse({"type": "log", "message": "[INFO] 正在获取 TextIn 消费数据..."})
                self._send_sse({"type": "log", "message": f"[DEBUG] token={textin_token[:10]}... len={len(textin_token)}"})
                textin_items = _fetch_textin_consume(textin_token, start_day, end_day)
                textin_daily = _aggregate_textin_daily(textin_items)
                self._send_sse({"type": "log", "message": f"[INFO] 获取到 {len(textin_daily)} 天数据"})
            elif vendor == "tianyancha_bill":
                self._send_sse({"type": "log", "message": "[INFO] 正在获取天眼查订单列表..."})
                all_orders = _fetch_tianyancha_orders(auth_secret, start_day, end_day, cookie=tian_cookie or None)
                tian_daily = _aggregate_tianyancha_daily(all_orders)
                self._send_sse({"type": "log", "message": f"[INFO] 获取到 {len(all_orders)} 条订单"})
            elif vendor == "deepseek":
                # 合并获取消费和Token数据
                self._send_sse({"type": "log", "message": "[INFO] 正在获取 DeepSeek 数据（消费+Token）..."})
                self._send_sse({"type": "log", "message": f"[DEBUG] auth={deepseek_auth[:20]}... cookie长度={len(deepseek_cookie or '')}"})
                start_dt = datetime.fromisoformat(start_day)
                end_dt = datetime.fromisoformat(end_day)
                
                # 1. 获取消费数据
                self._send_sse({"type": "log", "message": "[INFO] 获取消费数据..."})
                cost_daily = {}
                seen_months = set()
                cur = start_dt
                while cur <= end_dt:
                    ym = (cur.year, cur.month)
                    if ym not in seen_months:
                        seen_months.add(ym)
                        self._send_sse({"type": "log", "message": f"[DEBUG] 请求消费 {cur.year}年{cur.month}月..."})
                        raw_data = _fetch_deepseek_cost(deepseek_auth, cur.year, cur.month, deepseek_cookie)
                        parsed = _parse_deepseek_cost(raw_data)
                        for d, info in parsed.get("daily", {}).items():
                            cost_daily[d] = info
                    cur += timedelta(days=1)
                self._send_sse({"type": "log", "message": f"[INFO] 消费数据: {len(cost_daily)} 天"})
                
                # 2. 获取Token数据
                self._send_sse({"type": "log", "message": "[INFO] 获取Token数据..."})
                token_daily = {}
                seen_months = set()
                cur = start_dt
                while cur <= end_dt:
                    ym = (cur.year, cur.month)
                    if ym not in seen_months:
                        seen_months.add(ym)
                        self._send_sse({"type": "log", "message": f"[DEBUG] 请求Token {cur.year}年{cur.month}月..."})
                        raw_data = _fetch_deepseek_amount(deepseek_auth, cur.year, cur.month, deepseek_cookie)
                        parsed = _parse_deepseek_amount(raw_data)
                        for d, info in parsed.get("daily", {}).items():
                            token_daily[d] = info
                    cur += timedelta(days=1)
                self._send_sse({"type": "log", "message": f"[INFO] Token数据: {len(token_daily)} 天"})
                
                # 3. 直接入库
                sb = supabase_client()
                all_days = sorted(set(cost_daily.keys()) | set(token_daily.keys()))
                for day_str in all_days:
                    day_date = datetime.fromisoformat(day_str).date()
                    # 消费入库
                    cost_info = cost_daily.get(day_str, {})
                    amount = cost_info.get("amount", 0.0)
                    gross = cost_info.get("gross", 0.0)
                    currency = cost_info.get("currency", "CNY")
                    upsert_bill_daily_summary(sb, "deepseek", day_str, amount, gross, currency, is_ai_cost=True)
                    
                    # Token入库
                    token_info = token_daily.get(day_str, {})
                    total_tokens = token_info.get("total_tokens", 0)
                    delete_existing_daily(sb, day_str, "deepseek", None)
                    row = {
                        "day": day_str,
                        "vendor": "deepseek",
                        "model_id": "total",
                        "total_tokens": total_tokens,
                        "remark": "deepseek token usage",
                    }
                    insert_daily_row(sb, row)
                    
                    self._send_sse({"type": "log", "message": f"✓ {day_str} | Token: {total_tokens:,} | 金额: ¥{amount:.2f}"})
                
                # 4. 写入周/月汇总
                if write_summary:
                    for day_str in all_days:
                        day_date = datetime.fromisoformat(day_str).date()
                        week_start, week_end = week_bounds(day_date)
                        # Token周/月汇总
                        weekly = sum_token_daily(sb, "deepseek", week_start.isoformat(), week_end.isoformat())
                        upsert_weekly_with_id(sb, "deepseek", week_start.isoformat(), week_end.isoformat(), weekly)
                        month = day_str[:7]
                        month_start = f"{month}-01"
                        month_end = (day_date.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
                        monthly = sum_token_daily(sb, "deepseek", month_start, month_end.isoformat())
                        upsert_monthly_with_id(sb, "deepseek", month, monthly)
                        # 账单周/月汇总
                        weekly_bill = sum_bill_daily(sb, "deepseek", True, week_start.isoformat(), week_end.isoformat())
                        if weekly_bill:
                            upsert_bill_weekly_summary(sb, "deepseek", week_start.isoformat(), week_end.isoformat(),
                                                       weekly_bill["amount"], weekly_bill["gross"], weekly_bill["currency"], True)
                        monthly_bill = sum_bill_daily(sb, "deepseek", True, month_start, month_end.isoformat())
                        if monthly_bill:
                            upsert_bill_monthly_summary(sb, "deepseek", month, monthly_bill["amount"], monthly_bill["gross"], monthly_bill["currency"], True)
                
                self._send_sse({"type": "log", "message": f"[DONE] 完成，共处理 {len(all_days)} 天"})
                self._send_sse({"type": "done"})
                return
            elif vendor == "stepfun":
                self._send_sse({"type": "log", "message": "[INFO] 正在获取阶跃星辰 Token 数据..."})
                if not stepfun_cookie:
                    raise ValueError("stepfun_cookie is required")
                # 一次性获取整个日期范围的数据
                start_dt = datetime.fromisoformat(start_day)
                end_dt = datetime.fromisoformat(end_day)
                start_date = start_dt.date()
                end_date = end_dt.date()
                # 计算时间戳范围（北京时间）
                start_ms = int(datetime(start_date.year, start_date.month, start_date.day, 0, 0, 0, tzinfo=BJ).timestamp() * 1000)
                end_ms = int(datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59, 999999, tzinfo=BJ).timestamp() * 1000)
                # 获取所有数据（可能需要分页）
                all_records = []
                page = 1
                while True:
                    raw = fetch_stepfun_usage(stepfun_cookie, start_ms, end_ms, page=page, page_size=200)
                    records = raw.get("records") or []
                    all_records.extend(records)
                    total = int(raw.get("total") or 0)
                    if len(all_records) >= total or not records:
                        break
                    page += 1
                self._send_sse({"type": "log", "message": f"[INFO] 获取到 {len(all_records)} 条记录"})
                # 按日期聚合
                stepfun_daily = {}
                for r in all_records:
                    from_ts = int(r.get("fromTime") or 0) / 1000
                    day_str = datetime.fromtimestamp(from_ts, tz=BJ).strftime("%Y-%m-%d")
                    if day_str not in stepfun_daily:
                        stepfun_daily[day_str] = {"input": 0, "output": 0, "tokens": 0, "cost": 0.0, "cache": 0}
                    stepfun_daily[day_str]["input"] += int(r.get("inAmount") or 0)
                    stepfun_daily[day_str]["output"] += int(r.get("outAmount") or 0)
                    stepfun_daily[day_str]["tokens"] += int(r.get("amount") or 0)
                    stepfun_daily[day_str]["cache"] += int(r.get("cacheAmount") or 0)
                    # cost=充值扣减, totalCost=消费金额，单位都是毫（1/10000 元）
                    cost_hao = int(r.get("cost") or 0) + int(r.get("totalCost") or 0)
                    stepfun_daily[day_str]["cost"] += cost_hao / 10000
                self._send_sse({"type": "log", "message": f"[INFO] 聚合为 {len(stepfun_daily)} 天数据"})
                # 直接入库并打印
                sb = supabase_client()
                for day_str in sorted(stepfun_daily.keys()):
                    day_data = stepfun_daily[day_str]
                    total_tokens = day_data["tokens"] or (day_data["input"] + day_data["output"])
                    total_cost = day_data["cost"]
                    delete_existing_daily(sb, day_str, "stepfun", None)
                    row = {
                        "day": day_str,
                        "vendor": "stepfun",
                        "model_id": "total",
                        "total_tokens": total_tokens,
                    }
                    insert_daily_row(sb, row)
                    # 写入金额到 bill_daily_summary
                    if total_cost > 0:
                        upsert_bill_daily_summary(sb, "stepfun", day_str, total_cost, total_cost, "CNY", is_ai_cost=True)
                    self._send_sse({"type": "log", "message": f"✓ {day_str} | Token: {total_tokens:,} | 金额: ¥{total_cost:.2f}"})
                # 写入周/月汇总
                if write_summary:
                    for day_str in stepfun_daily.keys():
                        day_date = datetime.fromisoformat(day_str).date()
                        week_start, week_end = week_bounds(day_date)
                        # Token 周/月汇总
                        weekly = sum_token_daily(sb, "stepfun", week_start.isoformat(), week_end.isoformat())
                        upsert_weekly_with_id(sb, "stepfun", week_start.isoformat(), week_end.isoformat(), weekly)
                        month = day_str[:7]
                        month_start = f"{month}-01"
                        month_end = (datetime.fromisoformat(month_start).replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
                        monthly = sum_token_daily(sb, "stepfun", month_start, month_end.isoformat())
                        upsert_monthly_with_id(sb, "stepfun", month, monthly)
                        # 账单 周/月汇总
                        weekly_bill = sum_bill_daily(sb, "stepfun", True, week_start.isoformat(), week_end.isoformat())
                        if weekly_bill:
                            upsert_bill_weekly_summary(sb, "stepfun", week_start.isoformat(), week_end.isoformat(), weekly_bill["amount"], weekly_bill["gross"], weekly_bill["currency"], True)
                        monthly_bill = sum_bill_daily(sb, "stepfun", True, month_start, month_end.isoformat())
                        if monthly_bill:
                            upsert_bill_monthly_summary(sb, "stepfun", month, monthly_bill["amount"], monthly_bill["gross"], monthly_bill["currency"], True)
                self._send_sse({"type": "log", "message": f"[DONE] 完成，共处理 {len(stepfun_daily)} 天"})
                self._send_sse({"type": "done"})
                return
        except Exception as exc:
            self._send_sse({"type": "error", "message": f"预处理失败: {exc}"})
            return
        
        results = []
        for idx, day in enumerate(day_list):
            # 检查停止
            if _is_stopped(self._session_id):
                self._send_sse({"type": "stopped"})
                _cleanup_session(self._session_id)
                return
            # 检查暂停
            while _is_paused(self._session_id):
                self._send_sse({"type": "paused"})
                time.sleep(0.5)
                if _is_stopped(self._session_id):
                    self._send_sse({"type": "stopped"})
                    _cleanup_session(self._session_id)
                    return
            
            percent = int((idx + 1) / total_days * 100)
            self._send_sse({"type": "progress", "status": f"正在处理 {day}", "current": idx + 1, "total": total_days, "percent": percent})
            
            try:
                result = self._process_day(
                    sb, vendor, day, cookie, stepfun_cookie, auth_secret, tian_cookie,
                    moonshot_token, moonshot_org_id, moonshot_cookie, textin_token, deepseek_auth,
                    aws_dump_raw, write_summary, bill_client, bill_models, aws_client, volc_ready,
                    tian_daily, deepseek_daily, deepseek_token_daily,
                    moonshot_daily, textin_daily, stepfun_daily
                )
                results.append(result)
                
                if result.get("bill_amount") is not None:
                    amt = result['bill_amount']
                    currency = result.get('bill_currency', 'CNY')
                    if result.get("ai_amount") is not None or result.get("non_ai_amount") is not None:
                        ai = result.get('ai_amount', 0)
                        nai = result.get('non_ai_amount', 0)
                        tk = result.get('token_total', 0)
                        log_msg = f"✓ {day} | AI: {ai:.2f} | 非AI: {nai:.2f} | 合计: {amt:.4f} {currency}"
                        if tk > 0:
                            log_msg += f" | Token: {tk:,}"
                    else:
                        log_msg = f"✓ {day} | 金额: {amt:.4f} {currency}"
                    self._send_sse({"type": "log", "message": log_msg})
                elif "total_tokens" in result:
                    tokens = result['total_tokens']
                    log_msg = f"✓ {day} | Token: {tokens:,}"
                    self._send_sse({"type": "log", "message": log_msg})
                else:
                    self._send_sse({"type": "log", "message": f"✓ {day} | 完成"})
                
                # 防刷机制：对于需要爬取的接口，添加请求间隔
                if vendor in ("bailian", "aliyun_bill"):
                    time.sleep(3)
            except Exception as exc:
                self._send_sse({"type": "log", "message": f"[ERROR] {day}: {exc}"})
                results.append({"day": day, "total_tokens": 0, "raw": {"error": str(exc)}})
        
        self._send_sse({"type": "log", "message": f"[DONE] 完成，共处理 {len(results)} 天"})
        self._send_sse({"type": "done"})
        _cleanup_session(self._session_id)
    
    def _process_day(self, sb, vendor, day, cookie, stepfun_cookie, auth_secret, tian_cookie,
                     moonshot_token, moonshot_org_id, moonshot_cookie, textin_token, deepseek_auth,
                     aws_dump_raw, write_summary, bill_client, bill_models, aws_client, volc_ready,
                     tian_daily, deepseek_daily, deepseek_token_daily=None,
                     moonshot_daily=None, textin_daily=None, stepfun_daily=None):
        """处理单天数据，返回结果字典"""
        if vendor == "bailian":
            day_date = datetime.fromisoformat(day).date()
            start_ms, end_ms = bj_day_range(day_date)
            # 从环境变量获取必要参数
            workspace_id = os.getenv("ALIYUN_BAILIAN_WORKSPACE_ID", "")
            region = os.getenv("ALIYUN_BAILIAN_REGION", "")
            url = os.getenv("ALIYUN_BAILIAN_USAGE_URL") or None
            sec_token = os.getenv("ALIYUN_BAILIAN_SEC_TOKEN") or None
            csrf_token = os.getenv("ALIYUN_BAILIAN_CSRF_TOKEN") or None
            raw = fetch_usage(
                workspace_id,
                start_ms,
                end_ms,
                url=url,
                cookie=cookie,
                sec_token=sec_token,
                csrf_token=csrf_token,
                region=region,
            )
            total_tokens = parse_total_tokens(raw)
            print(f"[OK] bailian day={day} total_tokens={total_tokens}")
            delete_existing_daily(sb, day, "aliyun_bailian", None)
            row = {
                "day": day,
                "vendor": "aliyun_bailian",
                "model_id": "total",
                "project_id": workspace_id,
                "total_tokens": total_tokens,
            }
            insert_daily_row(sb, row)
            if write_summary:
                week_start, week_end = week_bounds(day_date)
                weekly = sum_token_daily(sb, "aliyun_bailian", week_start.isoformat(), week_end.isoformat())
                upsert_weekly_with_id(sb, "aliyun_bailian", week_start.isoformat(), week_end.isoformat(), weekly)
                month = day[:7]
                month_start = f"{month}-01"
                month_end = (datetime.fromisoformat(month_start).replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
                monthly = sum_token_daily(sb, "aliyun_bailian", month_start, month_end.isoformat())
                upsert_monthly_with_id(sb, "aliyun_bailian", month, monthly)
            return {"day": day, "total_tokens": total_tokens, "raw": raw}
        
        elif vendor == "stepfun":
            day_date = datetime.fromisoformat(day).date()
            # 使用预处理的数据（已在预处理阶段一次性获取并按日期聚合）
            day_data = stepfun_daily.get(day, {}) if stepfun_daily else {}
            total_tokens = day_data.get("tokens") or (day_data.get("input", 0) + day_data.get("output", 0))
            total_cost = day_data.get("cost", 0.0)
            cost_currency = "CNY"
            print(f"[OK] stepfun day={day} total_tokens={total_tokens} cost={total_cost}")
            delete_existing_daily(sb, day, "stepfun", None)
            row = {
                "day": day,
                "vendor": "stepfun",
                "model_id": "total",
                "total_tokens": total_tokens,
            }
            insert_daily_row(sb, row)
            # 写入金额到 bill_daily_summary
            if total_cost > 0:
                upsert_bill_daily_summary(sb, "stepfun", day, total_cost, total_cost, cost_currency, is_ai_cost=True)
            if write_summary:
                week_start, week_end = week_bounds(day_date)
                # Token 周/月汇总
                weekly = sum_token_daily(sb, "stepfun", week_start.isoformat(), week_end.isoformat())
                upsert_weekly_with_id(sb, "stepfun", week_start.isoformat(), week_end.isoformat(), weekly)
                month = day[:7]
                month_start = f"{month}-01"
                month_end = (datetime.fromisoformat(month_start).replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
                monthly = sum_token_daily(sb, "stepfun", month_start, month_end.isoformat())
                upsert_monthly_with_id(sb, "stepfun", month, monthly)
                # 账单 周/月汇总
                weekly_bill = sum_bill_daily(sb, "stepfun", True, week_start.isoformat(), week_end.isoformat())
                if weekly_bill:
                    upsert_bill_weekly_summary(sb, "stepfun", week_start.isoformat(), week_end.isoformat(), weekly_bill["amount"], weekly_bill["gross"], weekly_bill["currency"], True)
                monthly_bill = sum_bill_daily(sb, "stepfun", True, month_start, month_end.isoformat())
                if monthly_bill:
                    upsert_bill_monthly_summary(sb, "stepfun", month, monthly_bill["amount"], monthly_bill["gross"], monthly_bill["currency"], True)
            return {"day": day, "total_tokens": total_tokens, "total_cost": total_cost, "cost_currency": cost_currency}
        
        elif vendor == "aliyun_bill":
            day_date = datetime.fromisoformat(day).date()
            rows, summary = _fetch_aliyun_bill_rows(bill_client, bill_models, day_date)
            total_amount = summary.get("amount", 0.0)
            total_gross = summary.get("gross", 0.0)
            currency = summary.get("currency", "CNY")
            
            # 1. 写入产品明细表 aliyun_bill_daily
            if rows:
                delete_aliyun_bill_daily(sb, day)
                try:
                    sb.schema("financial_hub_prod").table("aliyun_bill_daily").upsert(rows).execute()
                    print(f"[OK] aliyun_bill day={day} rows={len(rows)} amount={total_amount}")
                except APIError as exc:
                    if _is_missing_gross_error(exc):
                        rows_without_gross = _strip_pretax_gross(rows)
                        sb.schema("financial_hub_prod").table("aliyun_bill_daily").upsert(rows_without_gross).execute()
                        print(f"[WARN] aliyun_bill day={day} missing pretax_gross_amount column; gross skipped")
                    else:
                        raise
            else:
                print(f"[WARN] aliyun_bill day={day} rows=0")
            
            # 2. 按 AI/非AI 分类写入 bill_daily_summary
            if rows:
                agg = {}
                for row in rows:
                    is_ai = bool(row.get("is_ai_cost"))
                    amount = float(row.get("pretax_amount") or 0)
                    gross = float(row.get("pretax_gross_amount") or row.get("pretax_amount") or 0)
                    curr = row.get("currency") or "CNY"
                    if is_ai not in agg:
                        agg[is_ai] = {"amount": 0.0, "gross": 0.0, "currencies": set()}
                    agg[is_ai]["amount"] += amount
                    agg[is_ai]["gross"] += gross
                    agg[is_ai]["currencies"].add(curr)
                for is_ai_cost, info in agg.items():
                    currencies = info["currencies"]
                    if len(currencies) == 1:
                        curr = next(iter(currencies))
                    elif len(currencies) > 1:
                        curr = "MIXED"
                    else:
                        curr = "CNY"
                    upsert_bill_daily_summary(
                        sb, "aliyun", day,
                        _normalize_amount(info["amount"]),
                        _normalize_amount(info["gross"]),
                        curr, is_ai_cost=is_ai_cost
                    )
            
            # 3. 写入周/月汇总
            if write_summary and rows:
                week_start, week_end = week_bounds(day_date)
                for is_ai_cost in agg.keys():
                    weekly = sum_bill_daily(sb, "aliyun", is_ai_cost, week_start.isoformat(), week_end.isoformat())
                    if weekly:
                        upsert_bill_weekly_summary(sb, "aliyun", week_start.isoformat(), week_end.isoformat(), weekly["amount"], weekly["gross"], weekly["currency"], is_ai_cost)
                    month = day[:7]
                    month_start = f"{month}-01"
                    month_end = (datetime.fromisoformat(month_start).replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
                    monthly = sum_bill_daily(sb, "aliyun", is_ai_cost, month_start, month_end.isoformat())
                    if monthly:
                        upsert_bill_monthly_summary(sb, "aliyun", month, monthly["amount"], monthly["gross"], monthly["currency"], is_ai_cost)
            
            return {"day": day, "bill_amount": total_amount, "bill_gross_amount": total_gross, "bill_currency": currency, "raw": rows}
        
        elif vendor == "aws_bill":
            day_date = datetime.fromisoformat(day).date()
            summary = _fetch_aws_bill_daily(aws_client, day_date, include_raw=aws_dump_raw)
            total_amount = summary.get("amount", 0.0)
            total_gross = summary.get("gross", 0.0)
            currency = summary.get("currency", "USD")
            print(f"[OK] aws_bill day={day} amount={total_amount} gross={total_gross} currency={currency}")
            upsert_bill_daily_summary(sb, "aws", day, total_amount, total_gross, currency, is_ai_cost=False)
            if write_summary:
                week_start, week_end = week_bounds(day_date)
                weekly = sum_bill_daily(sb, "aws", False, week_start.isoformat(), week_end.isoformat())
                if weekly:
                    upsert_bill_weekly_summary(sb, "aws", week_start.isoformat(), week_end.isoformat(), weekly["amount"], weekly["gross"], weekly["currency"], False)
                month = day[:7]
                month_start = f"{month}-01"
                month_end = (datetime.fromisoformat(month_start).replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
                monthly = sum_bill_daily(sb, "aws", False, month_start, month_end.isoformat())
                if monthly:
                    upsert_bill_monthly_summary(sb, "aws", month, monthly["amount"], monthly["gross"], monthly["currency"], False)
            return {"day": day, "bill_amount": total_amount, "bill_gross_amount": total_gross, "bill_currency": currency, "raw": summary}
        
        elif vendor == "volcengine_bill":
            day_date = datetime.fromisoformat(day).date()
            rows, summary = _fetch_volcengine_bill_daily(day_date)
            ai_amount = summary.get("ai_amount", 0.0)
            ai_gross = summary.get("ai_gross", 0.0)
            non_ai_amount = summary.get("non_ai_amount", 0.0)
            non_ai_gross = summary.get("non_ai_gross", 0.0)
            currency = summary.get("currency", "CNY")
            token_total = summary.get("token_total", 0)
            print(f"[OK] volcengine_bill day={day} AI={ai_amount} 非AI={non_ai_amount} gross={summary.get('gross', 0.0)} currency={currency} rows={len(rows)}")
            # 先删除旧数据
            delete_bill_daily_by_vendor(sb, "volcengine", day)
            # AI 部分入库
            if ai_amount > 0 or ai_gross > 0:
                upsert_bill_daily_summary(sb, "volcengine", day, ai_amount, ai_gross, currency, is_ai_cost=True)
            # 非 AI 部分入库
            if non_ai_amount > 0 or non_ai_gross > 0:
                upsert_bill_daily_summary(sb, "volcengine", day, non_ai_amount, non_ai_gross, currency, is_ai_cost=False)
            # 入库 Token 用量
            if token_total > 0:
                token_row = {
                    "day": day,
                    "vendor": "volcengine",
                    "model_id": "doubao",
                    "project_id": None,
                    "input_tokens": summary.get("token_input", 0),
                    "output_tokens": summary.get("token_output", 0),
                    "cache_tokens": 0,
                    "total_tokens": token_total,
                    "extra_metrics": {
                        "token_rows": summary.get("token_rows", []),
                    },
                    "raw": None,
                    "remark": "from volcengine bill api (doubao)",
                    "updated_at": datetime.now(tz=timezone.utc).isoformat(),
                }
                delete_existing_daily(sb, day, "volcengine", None)
                insert_daily_row(sb, token_row)
                print(f"[OK] volcengine token day={day} total={token_total} input={summary.get('token_input', 0)} output={summary.get('token_output', 0)}")
            if write_summary:
                week_start, week_end = week_bounds(day_date)
                # AI 周/月汇总
                weekly_ai = sum_bill_daily(sb, "volcengine", True, week_start.isoformat(), week_end.isoformat())
                if weekly_ai:
                    upsert_bill_weekly_summary(sb, "volcengine", week_start.isoformat(), week_end.isoformat(), weekly_ai["amount"], weekly_ai["gross"], weekly_ai["currency"], True)
                # 非AI 周/月汇总
                weekly_non_ai = sum_bill_daily(sb, "volcengine", False, week_start.isoformat(), week_end.isoformat())
                if weekly_non_ai:
                    upsert_bill_weekly_summary(sb, "volcengine", week_start.isoformat(), week_end.isoformat(), weekly_non_ai["amount"], weekly_non_ai["gross"], weekly_non_ai["currency"], False)
                month = day[:7]
                month_start = f"{month}-01"
                month_end = (datetime.fromisoformat(month_start).replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
                monthly_ai = sum_bill_daily(sb, "volcengine", True, month_start, month_end.isoformat())
                if monthly_ai:
                    upsert_bill_monthly_summary(sb, "volcengine", month, monthly_ai["amount"], monthly_ai["gross"], monthly_ai["currency"], True)
                monthly_non_ai = sum_bill_daily(sb, "volcengine", False, month_start, month_end.isoformat())
                if monthly_non_ai:
                    upsert_bill_monthly_summary(sb, "volcengine", month, monthly_non_ai["amount"], monthly_non_ai["gross"], monthly_non_ai["currency"], False)
                # Token 周/月汇总
                if token_total > 0:
                    weekly_token = sum_token_daily(sb, "volcengine", week_start.isoformat(), week_end.isoformat())
                    if weekly_token >= 0:
                        upsert_weekly_with_id(sb, "volcengine", week_start.isoformat(), week_end.isoformat(), weekly_token)
                    monthly_token = sum_token_daily(sb, "volcengine", month_start, month_end.isoformat())
                    if monthly_token >= 0:
                        upsert_monthly_with_id(sb, "volcengine", month, monthly_token)
            return {"day": day, "bill_amount": ai_amount + non_ai_amount, "bill_gross_amount": ai_gross + non_ai_gross, "bill_currency": currency, "ai_amount": ai_amount, "non_ai_amount": non_ai_amount, "token_total": token_total, "raw": rows}
        
        elif vendor == "tianyancha_bill":
            day_date = datetime.fromisoformat(day).date()
            daily_info = (tian_daily or {}).get(day, None)
            if daily_info:
                amount = daily_info["amount"]
                gross = daily_info["gross"]
                currency = daily_info["currency"]
            else:
                amount, gross, currency = 0.0, 0.0, "CNY"
            print(f"[OK] tianyancha_bill day={day} amount={amount} gross={gross} currency={currency}")
            upsert_bill_daily_summary(sb, "tianyancha", day, amount, gross, currency, is_ai_cost=False)
            if write_summary:
                week_start, week_end = week_bounds(day_date)
                weekly = sum_bill_daily(sb, "tianyancha", False, week_start.isoformat(), week_end.isoformat())
                if weekly:
                    upsert_bill_weekly_summary(sb, "tianyancha", week_start.isoformat(), week_end.isoformat(), weekly["amount"], weekly["gross"], weekly["currency"], False)
                month = day[:7]
                month_start = f"{month}-01"
                month_end = (datetime.fromisoformat(month_start).replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
                monthly = sum_bill_daily(sb, "tianyancha", False, month_start, month_end.isoformat())
                if monthly:
                    upsert_bill_monthly_summary(sb, "tianyancha", month, monthly["amount"], monthly["gross"], monthly["currency"], False)
            return {"day": day, "bill_amount": amount, "bill_gross_amount": gross, "bill_currency": currency, "raw": {}}
        
        elif vendor == "moonshot_bill":
            day_date = datetime.fromisoformat(day).date()
            daily_info = (moonshot_daily or {}).get(day, None)
            if daily_info:
                total_amount = daily_info["amount"]
                total_gross = daily_info["gross"]
                currency = daily_info.get("currency", "CNY")
            else:
                total_amount, total_gross, currency = 0.0, 0.0, "CNY"
            print(f"[OK] moonshot_bill day={day} amount={total_amount} gross={total_gross} currency={currency}")
            upsert_bill_daily_summary(sb, "moonshot", day, total_amount, total_gross, currency, is_ai_cost=True)
            if write_summary:
                week_start, week_end = week_bounds(day_date)
                weekly = sum_bill_daily(sb, "moonshot", True, week_start.isoformat(), week_end.isoformat())
                if weekly:
                    upsert_bill_weekly_summary(sb, "moonshot", week_start.isoformat(), week_end.isoformat(), weekly["amount"], weekly["gross"], weekly["currency"], True)
                month = day[:7]
                month_start = f"{month}-01"
                month_end = (datetime.fromisoformat(month_start).replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
                monthly = sum_bill_daily(sb, "moonshot", True, month_start, month_end.isoformat())
                if monthly:
                    upsert_bill_monthly_summary(sb, "moonshot", month, monthly["amount"], monthly["gross"], monthly["currency"], True)
            return {"day": day, "bill_amount": total_amount, "bill_gross_amount": total_gross, "bill_currency": currency, "raw": {}}
        
        elif vendor == "textin_bill":
            day_date = datetime.fromisoformat(day).date()
            daily_info = (textin_daily or {}).get(day, None)
            if daily_info:
                total_amount = daily_info["amount"]
                currency = daily_info.get("currency", "CNY")
            else:
                total_amount, currency = 0.0, "CNY"
            print(f"[OK] textin_bill day={day} amount={total_amount} currency={currency}")
            upsert_bill_daily_summary(sb, "textin", day, total_amount, total_amount, currency, is_ai_cost=False)
            if write_summary:
                week_start, week_end = week_bounds(day_date)
                weekly = sum_bill_daily(sb, "textin", False, week_start.isoformat(), week_end.isoformat())
                if weekly:
                    upsert_bill_weekly_summary(sb, "textin", week_start.isoformat(), week_end.isoformat(), weekly["amount"], weekly["gross"], weekly["currency"], False)
                month = day[:7]
                month_start = f"{month}-01"
                month_end = (datetime.fromisoformat(month_start).replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
                monthly = sum_bill_daily(sb, "textin", False, month_start, month_end.isoformat())
                if monthly:
                    upsert_bill_monthly_summary(sb, "textin", month, monthly["amount"], monthly["gross"], monthly["currency"], False)
            return {"day": day, "bill_amount": total_amount, "bill_gross_amount": total_amount, "bill_currency": currency, "raw": {}}
        
        elif vendor == "deepseek_bill":
            daily_info = (deepseek_daily or {}).get(day, None)
            if daily_info:
                amount = daily_info["amount"]
                gross = daily_info["gross"]
                currency = daily_info.get("currency", "CNY")
            else:
                amount, gross, currency = 0.0, 0.0, "CNY"
            print(f"[OK] deepseek_bill day={day} amount={amount} gross={gross} currency={currency}")
            upsert_bill_daily_summary(sb, "deepseek", day, amount, gross, currency, is_ai_cost=True)
            if write_summary:
                day_date = datetime.fromisoformat(day).date()
                week_start, week_end = week_bounds(day_date)
                weekly = sum_bill_daily(sb, "deepseek", True, week_start.isoformat(), week_end.isoformat())
                upsert_bill_weekly_summary(sb, "deepseek", week_start.isoformat(), week_end.isoformat(), weekly["amount"], weekly["gross"], weekly["currency"], True)
                month = day[:7]
                month_start = f"{month}-01"
                month_end = (datetime.fromisoformat(month_start).replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
                monthly = sum_bill_daily(sb, "deepseek", True, month_start, month_end.isoformat())
                upsert_bill_monthly_summary(sb, "deepseek", month, monthly["amount"], monthly["gross"], monthly["currency"], True)
            return {"day": day, "bill_amount": amount, "bill_gross_amount": gross, "bill_currency": currency, "raw": {}}
        
        elif vendor == "deepseek_token":
            daily_info = (deepseek_token_daily or {}).get(day, None)
            if daily_info:
                total_tokens = daily_info["total_tokens"]
            else:
                total_tokens = 0
            print(f"[OK] deepseek_token day={day} total_tokens={total_tokens}")
            delete_existing_daily(sb, day, "deepseek", None)
            row = {
                "day": day,
                "vendor": "deepseek",
                "model_id": "total",
                "total_tokens": total_tokens,
                "remark": "deepseek token usage",
            }
            insert_daily_row(sb, row)
            if write_summary:
                day_date = datetime.fromisoformat(day).date()
                week_start, week_end = week_bounds(day_date)
                weekly = sum_token_daily(sb, "deepseek", week_start.isoformat(), week_end.isoformat())
                upsert_weekly_with_id(sb, "deepseek", week_start.isoformat(), week_end.isoformat(), weekly)
                month = day[:7]
                month_start = f"{month}-01"
                month_end = (datetime.fromisoformat(month_start).replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
                monthly = sum_token_daily(sb, "deepseek", month_start, month_end.isoformat())
                upsert_monthly_with_id(sb, "deepseek", month, monthly)
            return {"day": day, "total_tokens": total_tokens, "raw": {}}
        
        else:
            return {"day": day, "total_tokens": 0, "raw": {"error": f"unknown vendor: {vendor}"}}

    def do_POST(self):
        # 处理登录
        if self.path == "/login":
            raw = read_body(self)
            form = parse_qs(raw.decode("utf-8"))
            password = (form.get("password") or [""])[0].strip()
            if password == LOGIN_PASSWORD:
                token = _generate_login_token()
                self.send_response(302)
                self.send_header("Set-Cookie", f"ops_token={token}; Path=/; HttpOnly; Max-Age=86400")
                self.send_header("Location", "/")
                self.end_headers()
            else:
                self._send_login_page("密码错误，请重试")
            return

        # 其他 POST 接口需要登录
        if not self._is_logged_in():
            self._send_login_page()
            return

        if self.path != "/fetch":
            self.send_error(404)
            return
        raw = read_body(self)
        form = parse_qs(raw.decode("utf-8"))
        vendor = (form.get("vendor") or ["bailian"])[0].strip() or "bailian"
        cookie = (form.get("cookie") or [""])[0].strip()
        auth_secret = (form.get("auth_secret") or [""])[0].strip()
        tian_cookie = (form.get("tian_cookie") or [""])[0].strip()
        moonshot_token = (form.get("moonshot_token") or [""])[0].strip()
        moonshot_org_id = (form.get("moonshot_org_id") or [""])[0].strip()
        moonshot_cookie = (form.get("moonshot_cookie") or [""])[0].strip()
        textin_token = (form.get("textin_token") or [""])[0].strip()
        # 获取第一个非空的 deepseek_auth 和 deepseek_cookie（因为有两个同名字段）
        deepseek_auth = next((v.strip() for v in (form.get("deepseek_auth") or []) if v.strip()), "")
        deepseek_cookie = next((v.strip() for v in (form.get("deepseek_cookie") or []) if v.strip()), "")
        start_day = (form.get("start_day") or [""])[0].strip()
        end_day = (form.get("end_day") or [""])[0].strip()
        retry_day = (form.get("retry_day") or [""])[0].strip()
        aws_dump_raw = (form.get("aws_dump_raw") or [""])[0].strip() == "1"

        if vendor in ("bailian", "stepfun") and not cookie:
            self.send_error(400, "cookie is required")
            return
        if vendor == "tianyancha_bill":
            auth_secret = auth_secret or os.getenv("TIANYANCHA_AUTH_SECRET", "").strip()
            if not auth_secret:
                send_plain_error(self, 400, "missing authSecret (form or env: TIANYANCHA_AUTH_SECRET)")
                return
        if vendor == "moonshot_bill":
            moonshot_token = moonshot_token or os.getenv("MOONSHOT_BEARER_TOKEN", "").strip()
            moonshot_org_id = moonshot_org_id or os.getenv("MOONSHOT_ORG_ID", "").strip()
            moonshot_cookie = moonshot_cookie or os.getenv("MOONSHOT_COOKIE", "").strip()
            if not moonshot_token or not moonshot_org_id:
                send_plain_error(self, 400, "missing moonshot token or org_id")
                return
        if vendor == "textin_bill":
            textin_token = textin_token or os.getenv("TEXTIN_TOKEN", "").strip()
            if not textin_token:
                send_plain_error(self, 400, "missing TextIn token (form or env: TEXTIN_TOKEN)")
                return
        if vendor == "deepseek_bill":
            deepseek_auth = deepseek_auth or os.getenv("DEEPSEEK_AUTH_TOKEN", "").strip()
            if not deepseek_auth:
                send_plain_error(self, 400, "missing DeepSeek authorization token (form or env: DEEPSEEK_AUTH_TOKEN)")
                return
        if vendor == "deepseek_token":
            deepseek_auth = deepseek_auth or os.getenv("DEEPSEEK_AUTH_TOKEN", "").strip()
            if not deepseek_auth:
                send_plain_error(self, 400, "missing DeepSeek authorization token (form or env: DEEPSEEK_AUTH_TOKEN)")
                return
        if not start_day or not end_day:
            self.send_error(400, "start_day/end_day are required")
            return

        sb = supabase_client()
        results = []
        write_summary = os.getenv("BAILIAN_WRITE_SUMMARY", "1").strip() == "1"
        if retry_day == "1":
            day_list = [start_day]
        else:
            day_list = list(iter_days(start_day, end_day))

        bill_client = None
        bill_models = None
        aws_client = None
        volc_ready = False
        tian_orders = None
        tian_daily = None
        if vendor == "aliyun_bill":
            try:
                bill_client, bill_models = _aliyun_bss_client()
            except RuntimeError as exc:
                self.send_error(400, str(exc))
                return
        if vendor == "aws_bill":
            try:
                aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
                aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
                if not aws_access_key_id or not aws_secret_access_key:
                    self.send_error(400, "missing env: AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY")
                    return
                aws_client = _aws_ce_client()
            except RuntimeError as exc:
                self.send_error(400, str(exc))
                return
        if vendor == "volcengine_bill":
            try:
                _volcengine_headers()
                volc_ready = True
            except RuntimeError as exc:
                self.send_error(400, str(exc))
                return
        if vendor == "tianyancha_bill":
            tian_cookie = (tian_cookie or os.getenv("TIANYANCHA_COOKIE", "")).strip() or None
            try:
                tian_orders = _fetch_tianyancha_orders(
                    auth_secret,
                    start_day,
                    end_day,
                    cookie=tian_cookie,
                )
                tian_daily = _aggregate_tianyancha_daily(tian_orders)
            except Exception as exc:
                send_plain_error(self, 400, f"tianyancha fetch failed: {exc}")
                return
        moonshot_daily = None
        if vendor == "moonshot_bill":
            try:
                moonshot_bills = _fetch_moonshot_daily_bills(
                    moonshot_token,
                    moonshot_org_id,
                    start_day,
                    end_day,
                    cookie=moonshot_cookie or None,
                )
                # 按日期建立映射
                moonshot_daily = {}
                for item in moonshot_bills:
                    date_str = item.get("date", "")[:10]  # "2025-12-31T00:00:00+08:00" -> "2025-12-31"
                    recharge_fee = int(item.get("recharge_fee") or 0)
                    voucher_fee = int(item.get("voucher_fee") or 0)
                    # recharge_fee 单位是 1/100000 元，转元
                    amount = recharge_fee / 100000
                    moonshot_daily[date_str] = {
                        "amount": amount,
                        "gross": amount,
                        "voucher": voucher_fee / 100000,
                        "currency": "CNY",
                        "raw": item,
                    }
            except Exception as exc:
                send_plain_error(self, 400, f"moonshot fetch failed: {exc}")
                return

        textin_daily = None
        if vendor == "textin_bill":
            try:
                textin_items = _fetch_textin_consume(textin_token, start_day, end_day)
                textin_daily = _aggregate_textin_daily(textin_items)
            except Exception as exc:
                send_plain_error(self, 400, f"textin fetch failed: {exc}")
                return

        deepseek_daily = None
        if vendor == "deepseek_bill":
            try:
                start_date = datetime.fromisoformat(start_day).date()
                end_date = datetime.fromisoformat(end_day).date()
                all_daily = {}
                
                current = start_date.replace(day=1)
                while current <= end_date:
                    year = current.year
                    month = current.month
                    raw_data = _fetch_deepseek_cost(deepseek_auth, year, month, deepseek_cookie)
                    parsed = _parse_deepseek_cost(raw_data)
                    all_daily.update(parsed.get("daily", {}))
                    if month == 12:
                        current = current.replace(year=year + 1, month=1)
                    else:
                        current = current.replace(month=month + 1)
                
                deepseek_daily = all_daily
            except Exception as exc:
                send_plain_error(self, 400, f"deepseek fetch failed: {exc}")
                return

        deepseek_token_daily = None
        if vendor == "deepseek_token":
            try:
                start_date = datetime.fromisoformat(start_day).date()
                end_date = datetime.fromisoformat(end_day).date()
                all_daily = {}
                
                current = start_date.replace(day=1)
                while current <= end_date:
                    year = current.year
                    month = current.month
                    raw_data = _fetch_deepseek_amount(deepseek_auth, year, month, deepseek_cookie)
                    parsed = _parse_deepseek_amount(raw_data)
                    all_daily.update(parsed.get("daily", {}))
                    if month == 12:
                        current = current.replace(year=year + 1, month=1)
                    else:
                        current = current.replace(month=month + 1)
                
                deepseek_token_daily = all_daily
            except Exception as exc:
                send_plain_error(self, 400, f"deepseek token fetch failed: {exc}")
                return

        for day in day_list:
            day_date = datetime.fromisoformat(day).date()
            start_ms, end_ms = bj_day_range(day_date)

            if vendor == "stepfun":
                if "STEPFUN_OASIS_APPID" not in os.environ or "STEPFUN_OASIS_WEBID" not in os.environ:
                    self.send_error(400, "missing env: STEPFUN_OASIS_APPID/STEPFUN_OASIS_WEBID")
                    return
                data = fetch_stepfun_usage(cookie, start_ms, end_ms)
                records = data.get("records", []) or []
                metrics = _sum_stepfun_metrics(records)
                total_tokens = metrics["tokens"]
                cost_total = metrics["cost"]
                cost_currency = os.getenv("STEPFUN_COST_CURRENCY", "CNY")
                project_id = os.getenv("STEPFUN_PROJECT_ID") or None

                row = {
                    "day": day,
                    "vendor": "stepfun",
                    "model_id": "total",
                    "project_id": project_id,
                    "input_tokens": metrics["input"],
                    "output_tokens": metrics["output"],
                    "cache_tokens": metrics["cache"],
                    "total_tokens": total_tokens,
                    "request_count": 0,
                    "image_count": metrics["image"],
                    "websearch_count": metrics["websearch"],
                    "tts_word_count": metrics["tts"],
                    "asr_duration_seconds": metrics["asr"],
                    "extra_metrics": {
                        "records_count": len(records),
                        "fromTime": start_ms,
                        "toTime": end_ms,
                        "cost_amount": cost_total,
                        "cost_currency": cost_currency,
                        "cost_hits": metrics["cost_hits"],
                    },
                    "raw": records,
                    "remark": "stepfun aggregated by day (web cookie)",
                    "updated_at": datetime.now(tz=timezone.utc).isoformat(),
                }
                delete_existing_daily(sb, day, "stepfun", project_id)
                insert_daily_row(sb, row)
                print(f"[OK] stepfun day={day} total_tokens={total_tokens} cost={cost_total}")
                results.append(
                    {
                        "day": day,
                        "total_tokens": total_tokens,
                        "total_cost": cost_total,
                        "cost_currency": cost_currency,
                        "raw": records,
                    }
                )
                if cost_total or metrics["cost_hits"] > 0:
                    upsert_bill_daily_summary(
                        sb,
                        "stepfun",
                        day,
                        _normalize_amount(cost_total),
                        _normalize_amount(cost_total),
                        cost_currency,
                        is_ai_cost=True,
                    )
                if write_summary:
                    try:
                        week_start, week_end = week_bounds(day_date)
                        weekly_total = sum_token_daily(sb, "stepfun", week_start.isoformat(), week_end.isoformat())
                        if weekly_total >= 0:
                            upsert_weekly_with_id(sb, "stepfun", week_start.isoformat(), week_end.isoformat(), weekly_total)

                        month_str = day_date.strftime("%Y-%m")
                        month_start = day_date.replace(day=1).isoformat()
                        month_end = (day_date.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
                        monthly_total = sum_token_daily(sb, "stepfun", month_start, month_end.isoformat())
                        if monthly_total >= 0:
                            upsert_monthly_with_id(sb, "stepfun", month_str, monthly_total)
                    except APIError as exc:
                        print(f"[WARN] summary upsert failed: {exc}")
            elif vendor == "aliyun_bill":
                rows, summary = _fetch_aliyun_bill_rows(bill_client, bill_models, day_date)
                if rows:
                    delete_aliyun_bill_daily(sb, day)
                    try:
                        sb.schema("financial_hub_prod").table("aliyun_bill_daily").upsert(rows).execute()
                        print(f"[OK] aliyun bill day={day} rows={len(rows)} amount={summary['amount']}")
                    except APIError as exc:
                        if _is_missing_gross_error(exc):
                            rows_without_gross = _strip_pretax_gross(rows)
                            sb.schema("financial_hub_prod").table("aliyun_bill_daily").upsert(rows_without_gross).execute()
                            summary["gross"] = None
                            print(
                                f"[WARN] aliyun bill day={day} missing pretax_gross_amount column; gross skipped"
                            )
                        else:
                            raise
                else:
                    print(f"[WARN] aliyun bill day={day} rows=0")
                if rows:
                    agg = {}
                    for row in rows:
                        is_ai = bool(row.get("is_ai_cost"))
                        amount = float(row.get("pretax_amount") or 0)
                        gross = float(row.get("pretax_gross_amount") or row.get("pretax_amount") or 0)
                        currency = row.get("currency") or "CNY"
                        if is_ai not in agg:
                            agg[is_ai] = {"amount": 0.0, "gross": 0.0, "currencies": set()}
                        agg[is_ai]["amount"] += amount
                        agg[is_ai]["gross"] += gross
                        agg[is_ai]["currencies"].add(currency)
                    for is_ai_cost, info in agg.items():
                        currencies = info["currencies"]
                        if len(currencies) == 1:
                            currency = next(iter(currencies))
                        elif len(currencies) > 1:
                            currency = "MIXED"
                        else:
                            currency = "CNY"
                        upsert_bill_daily_summary(
                            sb,
                            "aliyun",
                            day,
                            _normalize_amount(info["amount"]),
                            _normalize_amount(info["gross"]),
                            currency,
                            is_ai_cost=is_ai_cost,
                        )
                results.append(
                    {
                        "day": day,
                        "bill_amount": summary["amount"],
                        "bill_gross_amount": summary["gross"],
                        "bill_currency": summary["currency"],
                        "raw": rows,
                    }
                )
            elif vendor == "aws_bill":
                summary = _fetch_aws_bill_daily(aws_client, day_date, include_raw=aws_dump_raw)
                print(
                    f"[OK] aws bill day={day} net={summary['amount']} gross={summary['gross']} currency={summary['currency']}"
                )
                if summary.get("record_type_totals"):
                    print(
                        f"[OK] aws record_type usage={summary.get('usage_amount')} tax={summary.get('tax_amount')} credit={summary.get('credit_amount')}"
                    )
                raw_for_page = summary.get("raw_response") if aws_dump_raw else summary
                upsert_bill_daily_summary(
                    sb,
                    "aws",
                    day,
                    summary["amount"],
                    summary["gross"],
                    summary["currency"],
                    is_ai_cost=False,
                )
                if write_summary:
                    week_start, week_end = week_bounds(day_date)
                    weekly = sum_bill_daily(sb, "aws", False, week_start.isoformat(), week_end.isoformat())
                    if weekly:
                        upsert_bill_weekly_summary(
                            sb,
                            "aws",
                            week_start.isoformat(),
                            week_end.isoformat(),
                            weekly["amount"],
                            weekly["gross"],
                            weekly["currency"],
                            is_ai_cost=False,
                        )
                    month_str = day_date.strftime("%Y-%m")
                    month_start = day_date.replace(day=1).isoformat()
                    month_end = (day_date.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
                    monthly = sum_bill_daily(sb, "aws", False, month_start, month_end.isoformat())
                    if monthly:
                        upsert_bill_monthly_summary(
                            sb,
                            "aws",
                            month_str,
                            monthly["amount"],
                            monthly["gross"],
                            monthly["currency"],
                            is_ai_cost=False,
                        )
                results.append(
                    {
                        "day": day,
                        "bill_amount": summary["amount"],
                        "bill_gross_amount": summary["gross"],
                        "bill_currency": summary["currency"],
                        "raw": raw_for_page,
                    }
                )
            elif vendor == "volcengine_bill":
                if not volc_ready:
                    self.send_error(400, "volcengine client not ready")
                    return
                rows, summary = _fetch_volcengine_bill_daily(
                    day_date, ignore_zero=0, verbose=(day == end_day)
                )
                ai_amount = summary.get("ai_amount", 0.0)
                ai_gross = summary.get("ai_gross", 0.0)
                non_ai_amount = summary.get("non_ai_amount", 0.0)
                non_ai_gross = summary.get("non_ai_gross", 0.0)
                print(
                    f"[OK] volcengine bill day={day} AI={ai_amount} 非AI={non_ai_amount} gross={summary['gross']} currency={summary['currency']} rows={summary['rows']}"
                )
                # 先删除旧数据
                delete_bill_daily_by_vendor(sb, "volcengine", day)
                # AI 部分入库
                if ai_amount > 0 or ai_gross > 0:
                    upsert_bill_daily_summary(sb, "volcengine", day, ai_amount, ai_gross, summary["currency"], is_ai_cost=True)
                # 非 AI 部分入库
                if non_ai_amount > 0 or non_ai_gross > 0:
                    upsert_bill_daily_summary(sb, "volcengine", day, non_ai_amount, non_ai_gross, summary["currency"], is_ai_cost=False)
                # 入库 Token 用量（如果有）
                token_total = summary.get("token_total", 0)
                if token_total > 0:
                    token_row = {
                        "day": day,
                        "vendor": "volcengine",
                        "model_id": "doubao",
                        "project_id": None,
                        "input_tokens": summary.get("token_input", 0),
                        "output_tokens": summary.get("token_output", 0),
                        "cache_tokens": 0,
                        "total_tokens": token_total,
                        "extra_metrics": {
                            "token_rows": summary.get("token_rows", []),
                        },
                        "raw": None,
                        "remark": "from volcengine bill api (doubao)",
                        "updated_at": datetime.now(tz=timezone.utc).isoformat(),
                    }
                    delete_existing_daily(sb, day, "volcengine", None)
                    insert_daily_row(sb, token_row)
                    print(f"[OK] volcengine token day={day} total={token_total} input={summary.get('token_input', 0)} output={summary.get('token_output', 0)}")
                if write_summary:
                    week_start, week_end = week_bounds(day_date)
                    # AI 账单周/月汇总
                    weekly_ai = sum_bill_daily(sb, "volcengine", True, week_start.isoformat(), week_end.isoformat())
                    if weekly_ai:
                        upsert_bill_weekly_summary(sb, "volcengine", week_start.isoformat(), week_end.isoformat(),
                                                   weekly_ai["amount"], weekly_ai["gross"], weekly_ai["currency"], True)
                    # 非AI 账单周/月汇总
                    weekly_non_ai = sum_bill_daily(sb, "volcengine", False, week_start.isoformat(), week_end.isoformat())
                    if weekly_non_ai:
                        upsert_bill_weekly_summary(sb, "volcengine", week_start.isoformat(), week_end.isoformat(),
                                                   weekly_non_ai["amount"], weekly_non_ai["gross"], weekly_non_ai["currency"], False)
                    month_str = day_date.strftime("%Y-%m")
                    month_start = day_date.replace(day=1).isoformat()
                    month_end = (day_date.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
                    monthly_ai = sum_bill_daily(sb, "volcengine", True, month_start, month_end.isoformat())
                    if monthly_ai:
                        upsert_bill_monthly_summary(sb, "volcengine", month_str,
                                                    monthly_ai["amount"], monthly_ai["gross"], monthly_ai["currency"], True)
                    monthly_non_ai = sum_bill_daily(sb, "volcengine", False, month_start, month_end.isoformat())
                    if monthly_non_ai:
                        upsert_bill_monthly_summary(sb, "volcengine", month_str,
                                                    monthly_non_ai["amount"], monthly_non_ai["gross"], monthly_non_ai["currency"], False)
                    # Token 周/月汇总
                    if token_total > 0:
                        weekly_token = sum_token_daily(sb, "volcengine", week_start.isoformat(), week_end.isoformat())
                        if weekly_token >= 0:
                            upsert_weekly_with_id(sb, "volcengine", week_start.isoformat(), week_end.isoformat(), weekly_token)
                        monthly_token = sum_token_daily(sb, "volcengine", month_start, month_end.isoformat())
                        if monthly_token >= 0:
                            upsert_monthly_with_id(sb, "volcengine", month_str, monthly_token)
                results.append(
                    {
                        "day": day,
                        "bill_amount": ai_amount + non_ai_amount,
                        "bill_gross_amount": ai_gross + non_ai_gross,
                        "bill_currency": summary["currency"],
                        "total_tokens": token_total,
                        "raw": rows if aws_dump_raw else summary,
                    }
                )
            elif vendor == "tianyancha_bill":
                daily_info = (tian_daily or {}).get(day, None)
                if daily_info:
                    amount = _normalize_amount(daily_info["amount"])
                    gross = _normalize_amount(daily_info["gross"])
                    currency = daily_info["currency"]
                    raw_rows = daily_info["rows"]
                else:
                    amount = 0.0
                    gross = 0.0
                    currency = "CNY"
                    raw_rows = []
                upsert_bill_daily_summary(
                    sb,
                    "tianyancha",
                    day,
                    amount,
                    gross,
                    currency,
                    is_ai_cost=False,
                )
                print(f"[OK] tianyancha bill day={day} amount={amount} gross={gross} currency={currency}")
                if write_summary:
                    week_start, week_end = week_bounds(day_date)
                    weekly = sum_bill_daily(sb, "tianyancha", False, week_start.isoformat(), week_end.isoformat())
                    if weekly:
                        upsert_bill_weekly_summary(
                            sb,
                            "tianyancha",
                            week_start.isoformat(),
                            week_end.isoformat(),
                            weekly["amount"],
                            weekly["gross"],
                            weekly["currency"],
                            is_ai_cost=False,
                        )
                    month_str = day_date.strftime("%Y-%m")
                    month_start = day_date.replace(day=1).isoformat()
                    month_end = (day_date.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
                    monthly = sum_bill_daily(sb, "tianyancha", False, month_start, month_end.isoformat())
                    if monthly:
                        upsert_bill_monthly_summary(
                            sb,
                            "tianyancha",
                            month_str,
                            monthly["amount"],
                            monthly["gross"],
                            monthly["currency"],
                            is_ai_cost=False,
                        )
                results.append(
                    {
                        "day": day,
                        "bill_amount": amount,
                        "bill_gross_amount": gross,
                        "bill_currency": currency,
                        "raw": raw_rows,
                    }
                )
            elif vendor == "moonshot_bill":
                daily_info = (moonshot_daily or {}).get(day, None)
                if daily_info:
                    amount = _normalize_amount(daily_info["amount"])
                    gross = _normalize_amount(daily_info["gross"])
                    currency = daily_info["currency"]
                    raw_item = daily_info["raw"]
                else:
                    amount = 0.0
                    gross = 0.0
                    currency = "CNY"
                    raw_item = {}
                upsert_bill_daily_summary(
                    sb,
                    "moonshot",
                    day,
                    amount,
                    gross,
                    currency,
                    is_ai_cost=True,
                )
                print(f"[OK] moonshot bill day={day} amount={amount} gross={gross} currency={currency}")
                if write_summary:
                    week_start, week_end = week_bounds(day_date)
                    weekly = sum_bill_daily(sb, "moonshot", True, week_start.isoformat(), week_end.isoformat())
                    if weekly:
                        upsert_bill_weekly_summary(
                            sb,
                            "moonshot",
                            week_start.isoformat(),
                            week_end.isoformat(),
                            weekly["amount"],
                            weekly["gross"],
                            weekly["currency"],
                            is_ai_cost=True,
                        )
                    month_str = day_date.strftime("%Y-%m")
                    month_start = day_date.replace(day=1).isoformat()
                    month_end = (day_date.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
                    monthly = sum_bill_daily(sb, "moonshot", True, month_start, month_end.isoformat())
                    if monthly:
                        upsert_bill_monthly_summary(
                            sb,
                            "moonshot",
                            month_str,
                            monthly["amount"],
                            monthly["gross"],
                            monthly["currency"],
                            is_ai_cost=True,
                        )
                results.append(
                    {
                        "day": day,
                        "bill_amount": amount,
                        "bill_gross_amount": gross,
                        "bill_currency": currency,
                        "raw": raw_item,
                    }
                )
            elif vendor == "textin_bill":
                daily_info = (textin_daily or {}).get(day, None)
                if daily_info:
                    amount = _normalize_amount(daily_info["amount"])
                    gross = _normalize_amount(daily_info["gross"])
                    currency = daily_info["currency"]
                    raw_items = daily_info["raw"]
                else:
                    amount = 0.0
                    gross = 0.0
                    currency = "CNY"
                    raw_items = []
                upsert_bill_daily_summary(
                    sb,
                    "textin",
                    day,
                    amount,
                    gross,
                    currency,
                    is_ai_cost=False,
                )
                print(f"[OK] textin bill day={day} amount={amount} gross={gross} currency={currency}")
                if write_summary:
                    week_start, week_end = week_bounds(day_date)
                    weekly = sum_bill_daily(sb, "textin", False, week_start.isoformat(), week_end.isoformat())
                    if weekly:
                        upsert_bill_weekly_summary(
                            sb,
                            "textin",
                            week_start.isoformat(),
                            week_end.isoformat(),
                            weekly["amount"],
                            weekly["gross"],
                            weekly["currency"],
                            is_ai_cost=False,
                        )
                    month_str = day_date.strftime("%Y-%m")
                    month_start = day_date.replace(day=1).isoformat()
                    month_end = (day_date.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
                    monthly = sum_bill_daily(sb, "textin", False, month_start, month_end.isoformat())
                    if monthly:
                        upsert_bill_monthly_summary(
                            sb,
                            "textin",
                            month_str,
                            monthly["amount"],
                            monthly["gross"],
                            monthly["currency"],
                            is_ai_cost=False,
                        )
                results.append(
                    {
                        "day": day,
                        "bill_amount": amount,
                        "bill_gross_amount": gross,
                        "bill_currency": currency,
                        "raw": raw_items,
                    }
                )
            elif vendor == "deepseek_bill":
                daily_info = (deepseek_daily or {}).get(day, None)
                if daily_info:
                    amount = _normalize_amount(daily_info["amount"])
                    gross = _normalize_amount(daily_info["gross"])
                    currency = daily_info["currency"]
                else:
                    amount = 0.0
                    gross = 0.0
                    currency = "CNY"
                upsert_bill_daily_summary(
                    sb,
                    "deepseek",
                    day,
                    amount,
                    gross,
                    currency,
                    is_ai_cost=True,
                )
                print(f"[OK] deepseek bill day={day} amount={amount} gross={gross} currency={currency}")
                if write_summary:
                    week_start, week_end = week_bounds(day_date)
                    weekly = sum_bill_daily(sb, "deepseek", True, week_start.isoformat(), week_end.isoformat())
                    if weekly:
                        upsert_bill_weekly_summary(
                            sb,
                            "deepseek",
                            week_start.isoformat(),
                            week_end.isoformat(),
                            weekly["amount"],
                            weekly["gross"],
                            weekly["currency"],
                            is_ai_cost=True,
                        )
                    month_str = day_date.strftime("%Y-%m")
                    month_start = day_date.replace(day=1).isoformat()
                    month_end = (day_date.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
                    monthly = sum_bill_daily(sb, "deepseek", True, month_start, month_end.isoformat())
                    if monthly:
                        upsert_bill_monthly_summary(
                            sb,
                            "deepseek",
                            month_str,
                            monthly["amount"],
                            monthly["gross"],
                            monthly["currency"],
                            is_ai_cost=True,
                        )
                results.append(
                    {
                        "day": day,
                        "bill_amount": amount,
                        "bill_gross_amount": gross,
                        "bill_currency": currency,
                        "raw": daily_info or {},
                    }
                )
            elif vendor == "deepseek_token":
                daily_info = (deepseek_token_daily or {}).get(day, None)
                if daily_info:
                    total_tokens = daily_info["total_tokens"]
                else:
                    total_tokens = 0
                delete_existing_daily(sb, day, "deepseek", None)
                row = {
                    "day": day,
                    "vendor": "deepseek",
                    "model_id": "total",
                    "total_tokens": total_tokens,
                    "remark": "deepseek token usage",
                }
                insert_daily_row(sb, row)
                print(f"[OK] deepseek_token day={day} total_tokens={total_tokens}")
                if write_summary:
                    week_start, week_end = week_bounds(day_date)
                    weekly = sum_token_daily(sb, "deepseek", week_start.isoformat(), week_end.isoformat())
                    upsert_weekly_with_id(sb, "deepseek", week_start.isoformat(), week_end.isoformat(), weekly)
                    month_str = day_date.strftime("%Y-%m")
                    month_start = day_date.replace(day=1).isoformat()
                    month_end = (day_date.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
                    monthly = sum_token_daily(sb, "deepseek", month_start, month_end.isoformat())
                    upsert_monthly_with_id(sb, "deepseek", month_str, monthly)
                results.append(
                    {
                        "day": day,
                        "total_tokens": total_tokens,
                        "raw": daily_info or {},
                    }
                )
            else:
                workspace_id = os.getenv("ALIYUN_BAILIAN_WORKSPACE_ID", "")
                region = os.getenv("ALIYUN_BAILIAN_REGION", "")
                url = os.getenv("ALIYUN_BAILIAN_USAGE_URL") or None
                sec_token = os.getenv("ALIYUN_BAILIAN_SEC_TOKEN") or None
                csrf_token = os.getenv("ALIYUN_BAILIAN_CSRF_TOKEN") or None
                if not workspace_id or not region or not url:
                    self.send_error(400, "missing env: WORKSPACE_ID/REGION/USAGE_URL")
                    return

                data = fetch_usage(
                    workspace_id,
                    start_ms,
                    end_ms,
                    url=url,
                    cookie=cookie,
                    sec_token=sec_token,
                    csrf_token=csrf_token,
                    region=region,
                )
                total_tokens = parse_total_tokens(data)
                print(f"[OK] bailian day={day} workspace={workspace_id} total_tokens={total_tokens}")

                row = {
                    "day": day,
                    "vendor": "aliyun",
                    "model_id": "total",
                    "project_id": workspace_id,
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "cache_tokens": 0,
                    "total_tokens": total_tokens,
                    "raw": data,
                    "remark": "from bailian web usage-statistic",
                    "updated_at": datetime.now(tz=timezone.utc).isoformat(),
                }
                delete_existing_daily(sb, day, "aliyun", workspace_id)
                insert_daily_row(sb, row)

                weekly_total = None
                monthly_total = None
                if write_summary:
                    try:
                        week_start, week_end = week_bounds(day_date)
                        weekly_total = sum_token_daily(sb, "aliyun", week_start.isoformat(), week_end.isoformat())
                        if weekly_total >= 0:
                            upsert_weekly_with_id(sb, "aliyun", week_start.isoformat(), week_end.isoformat(), weekly_total)

                        month_str = day_date.strftime("%Y-%m")
                        month_start = day_date.replace(day=1).isoformat()
                        month_end = (day_date.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
                        monthly_total = sum_token_daily(sb, "aliyun", month_start, month_end.isoformat())
                        if monthly_total >= 0:
                            upsert_monthly_with_id(sb, "aliyun", month_str, monthly_total)
                    except APIError as exc:
                        print(f"[WARN] summary upsert failed: {exc}")

                if write_summary:
                    print(f"[OK] supabase day={day} weekly={weekly_total} monthly={monthly_total}")
                else:
                    print(f"[OK] supabase day={day} (summary skipped)")
                results.append({"day": day, "total_tokens": total_tokens, "raw": data})

        page = render_result(start_day, end_day, results, vendor)
        self.send_response(200)
        self.send_header("content-type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(page.encode("utf-8"))


def main():
    host = os.getenv("BAILIAN_WEB_HOST", "127.0.0.1")
    port = int(os.getenv("BAILIAN_WEB_PORT", "8000"))
    server = ThreadingHTTPServer((host, port), Handler)
    url = f"http://{host}:{port}"
    print(f"[OK] Bailian web running at {url}")
    try:
        webbrowser.open(url)
    except Exception:
        pass
    server.serve_forever()


def _shutdown_server(server: ThreadingHTTPServer):
    time.sleep(0.2)
    server.shutdown()


if __name__ == "__main__":
    main()
