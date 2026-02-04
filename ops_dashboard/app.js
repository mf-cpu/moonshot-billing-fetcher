// ========== 登录模块 ==========
const AUTH_KEY = "ops_dashboard_auth";
const AUTH_EXPIRY_HOURS = 24; // 登录有效期（小时）

// 默认密码: "ops2026"
// 可在 config.js 中通过 window.PASSWORD_HASH 覆盖
const DEFAULT_PASSWORD = "ops2026";

async function sha256(message) {
  const msgBuffer = new TextEncoder().encode(message);
  const hashBuffer = await crypto.subtle.digest("SHA-256", msgBuffer);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  return hashArray.map((b) => b.toString(16).padStart(2, "0")).join("");
}

// 控制台工具：生成密码哈希
window.generatePasswordHash = async function(password) {
  const hash = await sha256(password);
  console.log("密码:", password);
  console.log("哈希值:", hash);
  console.log("将以下代码添加到 config.js:");
  console.log(`window.PASSWORD_HASH = "${hash}";`);
  return hash;
};

function getPasswordHash() {
  return window.PASSWORD_HASH || null;
}

function isLoggedIn() {
  const auth = sessionStorage.getItem(AUTH_KEY);
  if (!auth) return false;
  try {
    const { timestamp } = JSON.parse(auth);
    const hoursPassed = (Date.now() - timestamp) / (1000 * 60 * 60);
    return hoursPassed < AUTH_EXPIRY_HOURS;
  } catch {
    return false;
  }
}

function setLoggedIn() {
  sessionStorage.setItem(AUTH_KEY, JSON.stringify({ timestamp: Date.now() }));
}

function logout() {
  sessionStorage.removeItem(AUTH_KEY);
  showLoginScreen();
}

function showLoginScreen() {
  document.getElementById("loginScreen").style.display = "flex";
  document.getElementById("mainApp").style.display = "none";
}

function showMainApp() {
  document.getElementById("loginScreen").style.display = "none";
  document.getElementById("mainApp").style.display = "block";
}

async function verifyPassword(password) {
  const configuredHash = getPasswordHash();
  
  if (configuredHash) {
    // 使用配置的哈希值验证
    const inputHash = await sha256(password);
    return inputHash === configuredHash;
  } else {
    // 使用默认密码验证
    return password === DEFAULT_PASSWORD;
  }
}

async function handleLogin(event) {
  event.preventDefault();
  const password = document.getElementById("loginPassword").value;
  const errorEl = document.getElementById("loginError");
  
  if (!password) {
    errorEl.textContent = "请输入密码";
    errorEl.classList.add("show");
    return;
  }

  const isValid = await verifyPassword(password);

  if (isValid) {
    setLoggedIn();
    errorEl.classList.remove("show");
    document.getElementById("loginPassword").value = "";
    showMainApp();
    initApp();
  } else {
    errorEl.textContent = "密码错误，请重试";
    errorEl.classList.add("show");
    document.getElementById("loginPassword").value = "";
    document.getElementById("loginPassword").focus();
  }
}

function initAuth() {
  // 绑定登录表单
  document.getElementById("loginForm").addEventListener("submit", handleLogin);
  
  // 绑定注销按钮
  document.getElementById("logoutButton").addEventListener("click", logout);

  // 检查登录状态
  if (isLoggedIn()) {
    showMainApp();
    initApp();
  } else {
    showLoginScreen();
    document.getElementById("loginPassword").focus();
  }
}

// ========== 主应用 ==========
const state = {
  startDate: null,
  endDate: null,
  // 各供应商数据缓存
  billData: null,
  volcData: null,
  awsData: null,
  stepfunData: null,
  moonshotData: null,
  tianData: null,
  textinData: null,
  authingData: null,
};

const els = {
  startDate: document.getElementById("startDate"),
  endDate: document.getElementById("endDate"),
  applyRange: document.getElementById("applyRange"),
  refreshButton: document.getElementById("refreshButton"),
  // DeepSeek
  deepseekAuth: document.getElementById("deepseekAuth"),
  deepseekYear: document.getElementById("deepseekYear"),
  deepseekMonth: document.getElementById("deepseekMonth"),
  fetchDeepseek: document.getElementById("fetchDeepseek"),
  deepseekStatus: document.getElementById("deepseekStatus"),
  deepseekResult: document.getElementById("deepseekResult"),
  deepseekTotalCost: document.getElementById("deepseekTotalCost"),
  deepseekCurrency: document.getElementById("deepseekCurrency"),
  deepseekModelCount: document.getElementById("deepseekModelCount"),
  deepseekModelTable: document.getElementById("deepseekModelTable"),
  deepseekDailyTable: document.getElementById("deepseekDailyTable"),
  // 汇总
  aiTotalCost: document.getElementById("aiTotalCost"),
  aiTotalToken: document.getElementById("aiTotalToken"),
  nonAiTotalCost: document.getElementById("nonAiTotalCost"),
  aiSummaryCard: document.getElementById("aiSummaryCard"),
  nonAiSummaryCard: document.getElementById("nonAiSummaryCard"),
  // 阿里云
  billStatus: document.getElementById("billStatus"),
  billTotal: document.getElementById("billTotal"),
  billGrossTotal: document.getElementById("billGrossTotal"),
  billDiscountTotal: document.getElementById("billDiscountTotal"),
  billAi: document.getElementById("billAi"),
  billNonAi: document.getElementById("billNonAi"),
  billRows: document.getElementById("billRows"),
  billProductTable: document.getElementById("billProductTable"),
  // Token
  tokenStatus: document.getElementById("tokenStatus"),
  tokenTotal: document.getElementById("tokenTotal"),
  tokenTotalCost: document.getElementById("tokenTotalCost"),
  tokenVendorCount: document.getElementById("tokenVendorCount"),
  tokenTable: document.getElementById("tokenTable"),
  // DMXAPI
  dmxapiStatus: document.getElementById("dmxapiStatus"),
  dmxapiTotal: document.getElementById("dmxapiTotal"),
  dmxapiAmount: document.getElementById("dmxapiAmount"),
  dmxapiWeeks: document.getElementById("dmxapiWeeks"),
  dmxapiTable: document.getElementById("dmxapiTable"),
  // 阿里云 Token
  aliyunTokenStatus: document.getElementById("aliyunTokenStatus"),
  aliyunTokenTotal: document.getElementById("aliyunTokenTotal"),
  aliyunTokenRows: document.getElementById("aliyunTokenRows"),
  aliyunTokenTable: document.getElementById("aliyunTokenTable"),
  // AWS
  awsBillStatus: document.getElementById("awsBillStatus"),
  awsBillTotal: document.getElementById("awsBillTotal"),
  awsBillRows: document.getElementById("awsBillRows"),
  awsBillTable: document.getElementById("awsBillTable"),
  awsOverviewStatus: document.getElementById("awsOverviewStatus"),
  awsOverviewTotal: document.getElementById("awsOverviewTotal"),
  awsOverviewRows: document.getElementById("awsOverviewRows"),
  // 天眼查
  tianOverviewStatus: document.getElementById("tianOverviewStatus"),
  tianOverviewTotal: document.getElementById("tianOverviewTotal"),
  tianOverviewRows: document.getElementById("tianOverviewRows"),
  tianBillStatus: document.getElementById("tianBillStatus"),
  tianBillTotal: document.getElementById("tianBillTotal"),
  tianBillRows: document.getElementById("tianBillRows"),
  tianBillTable: document.getElementById("tianBillTable"),
  // 月之暗面
  moonshotOverviewStatus: document.getElementById("moonshotOverviewStatus"),
  moonshotOverviewTotal: document.getElementById("moonshotOverviewTotal"),
  moonshotOverviewRows: document.getElementById("moonshotOverviewRows"),
  // 阶跃星辰
  stepfunOverviewStatus: document.getElementById("stepfunOverviewStatus"),
  stepfunOverviewTotal: document.getElementById("stepfunOverviewTotal"),
  stepfunOverviewRows: document.getElementById("stepfunOverviewRows"),
  // Authing
  authingOverviewStatus: document.getElementById("authingOverviewStatus"),
  authingOverviewTotal: document.getElementById("authingOverviewTotal"),
  authingOverviewRows: document.getElementById("authingOverviewRows"),
  // TextIn
  textinOverviewStatus: document.getElementById("textinOverviewStatus"),
  textinOverviewTotal: document.getElementById("textinOverviewTotal"),
  textinOverviewRows: document.getElementById("textinOverviewRows"),
  // 火山
  volcBillStatus: document.getElementById("volcBillStatus"),
  volcBillTotal: document.getElementById("volcBillTotal"),
  volcBillRows: document.getElementById("volcBillRows"),
  volcBillTable: document.getElementById("volcBillTable"),
  volcOverviewStatus: document.getElementById("volcOverviewStatus"),
  volcOverviewTotal: document.getElementById("volcOverviewTotal"),
  volcOverviewAi: document.getElementById("volcOverviewAi"),
  volcOverviewNonAi: document.getElementById("volcOverviewNonAi"),
  volcOverviewRows: document.getElementById("volcOverviewRows"),
  // 系统
  lastRefresh: document.getElementById("lastRefresh"),
  errorBox: document.getElementById("errorBox"),
  // Tab
  tabs: Array.from(document.querySelectorAll(".tab")),
  tabPanels: {
    overview: document.getElementById("tab-overview"),
    "ai-usage": document.getElementById("tab-ai-usage"),
    bills: document.getElementById("tab-bills"),
    "data-fetch": document.getElementById("tab-data-fetch"),
  },
  // Modal
  modalOverlay: document.getElementById("modalOverlay"),
  modalTitle: document.getElementById("modalTitle"),
  modalContent: document.getElementById("modalContent"),
  modalClose: document.getElementById("modalClose"),
};

// 1亿 = 100,000,000
const YI = 100000000;

function formatCurrency(amount, currency = "CNY") {
  if (Number.isNaN(amount) || amount === null || amount === undefined) return "-";
  return new Intl.NumberFormat("zh-CN", {
    style: "currency",
    currency,
    maximumFractionDigits: 2,
  }).format(amount);
}

function formatNumber(num) {
  if (Number.isNaN(num) || num === null || num === undefined) return "-";
  return new Intl.NumberFormat("zh-CN").format(num);
}

function formatTokenYi(tokens) {
  if (Number.isNaN(tokens) || tokens === null || tokens === undefined) return "-";
  const yi = tokens / YI;
  return yi < 0.01 ? yi.toFixed(4) : yi.toFixed(2);
}

function setStatus(el, text) {
  if (el) el.textContent = text;
}

function setError(text) {
  els.errorBox.textContent = text || "无";
  els.errorBox.classList.toggle("error", !!text);
}

// 日期工具
function formatDate(date) {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const d = String(date.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function formatDateShort(dateStr) {
  // "2026-01-15" -> "01-15"
  return dateStr ? dateStr.slice(5) : "-";
}

function getThisMonthRange() {
  const today = new Date();
  const start = new Date(today.getFullYear(), today.getMonth(), 1);
  return { start, end: today };
}

function getThisWeekRange() {
  // 本周一至今日
  const today = new Date();
  const dayOfWeek = today.getDay() || 7; // 周日=7
  const start = new Date(today);
  start.setDate(today.getDate() - dayOfWeek + 1); // 周一
  return { start, end: today };
}

function getLastWeekRange() {
  // 上周一至上周日
  const today = new Date();
  const dayOfWeek = today.getDay() || 7;
  const thisMonday = new Date(today);
  thisMonday.setDate(today.getDate() - dayOfWeek + 1);
  const lastSunday = new Date(thisMonday);
  lastSunday.setDate(thisMonday.getDate() - 1);
  const lastMonday = new Date(lastSunday);
  lastMonday.setDate(lastSunday.getDate() - 6);
  return { start: lastMonday, end: lastSunday };
}

function getLastMonthRange() {
  // 上月1日至上月最后一日
  const today = new Date();
  const start = new Date(today.getFullYear(), today.getMonth() - 1, 1);
  const end = new Date(today.getFullYear(), today.getMonth(), 0); // 上月最后一天
  return { start, end };
}

function initDateRange() {
  // 默认上周
  const { start, end } = getLastWeekRange();
  state.startDate = start;
  state.endDate = end;
  els.startDate.value = formatDate(start);
  els.endDate.value = formatDate(end);
  updateRangeHint();
  // 高亮上周按钮
  document.querySelector('[data-range="lastWeek"]')?.classList.add("active");
}

function updateRangeHint() {
  // 日期已在输入框显示，无需额外提示
}

function setDateRange(range) {
  let start, end;
  switch (range) {
    case "thisMonth":
      ({ start, end } = getThisMonthRange());
      break;
    case "thisWeek":
      ({ start, end } = getThisWeekRange());
      break;
    case "lastWeek":
      ({ start, end } = getLastWeekRange());
      break;
    case "lastMonth":
      ({ start, end } = getLastMonthRange());
      break;
  }
  if (start && end) {
    state.startDate = start;
    state.endDate = end;
    els.startDate.value = formatDate(start);
    els.endDate.value = formatDate(end);
    updateRangeHint();
    // 更新按钮高亮
    document.querySelectorAll(".shortcut-btn").forEach((btn) => {
      btn.classList.toggle("active", btn.dataset.range === range);
    });
    refresh();
  }
}

function buildClient() {
  if (!window.SUPABASE_URL || !window.SUPABASE_ANON_KEY) {
    throw new Error("请先配置 config.js 中的 Supabase 信息");
  }
  return window.supabase.createClient(window.SUPABASE_URL, window.SUPABASE_ANON_KEY);
}

// 渲染表格
function renderBillTable(rows) {
  els.billProductTable.innerHTML = "";
  if (!rows.length) {
    els.billProductTable.innerHTML = '<tr><td colspan="6" class="muted">暂无数据</td></tr>';
    return;
  }
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${row.product_code || "-"}</td>
      <td>${row.product_name || "-"}</td>
      <td>${row.is_ai_cost ? '<span class="tag tag-ai">AI</span>' : '<span class="tag tag-nonai">非AI</span>'}</td>
      <td class="right">${formatCurrency(row.gross_amount)}</td>
      <td class="right">${formatCurrency(row.discount_amount)}</td>
      <td class="right">${formatCurrency(row.amount)}</td>
    `;
    els.billProductTable.appendChild(tr);
  });
}

function renderTokenTable(rows) {
  els.tokenTable.innerHTML = "";
  if (!rows.length) {
    els.tokenTable.innerHTML = '<tr><td colspan="3" class="muted">暂无数据</td></tr>';
    return;
  }
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    tr.className = "clickable-row";
    tr.dataset.vendor = row.vendorCode;
    tr.innerHTML = `
      <td>${row.vendor || "-"}</td>
      <td class="right">${formatTokenYi(row.total_tokens)}</td>
      <td class="right">${row.cost !== null ? formatCurrency(row.cost) : "-"}</td>
    `;
    tr.addEventListener("click", () => showVendorDetail(row.vendorCode));
    els.tokenTable.appendChild(tr);
  });
}

function renderAliyunTokenTable(rows) {
  els.aliyunTokenTable.innerHTML = "";
  if (!rows.length) {
    els.aliyunTokenTable.innerHTML = '<tr><td colspan="3" class="muted">暂无数据</td></tr>';
    return;
  }
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${row.day}</td>
      <td>${row.project_id || "-"}</td>
      <td class="right">${formatTokenYi(row.total_tokens)}</td>
    `;
    els.aliyunTokenTable.appendChild(tr);
  });
}

function renderDailyBillTable(rows, tableEl) {
  tableEl.innerHTML = "";
  if (!rows.length) {
    tableEl.innerHTML = '<tr><td colspan="4" class="muted">暂无数据</td></tr>';
    return;
  }
  rows.forEach((row) => {
    const currency = row.currency || "CNY";
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${row.billing_date}</td>
      <td>${row.is_ai_cost ? '<span class="tag tag-ai">AI</span>' : '<span class="tag tag-nonai">非AI</span>'}</td>
      <td class="right">${formatCurrency(row.amount, currency)}</td>
      <td>${currency}</td>
    `;
    tableEl.appendChild(tr);
  });
}

// 弹窗
function showModal(title, content) {
  els.modalTitle.textContent = title;
  els.modalContent.innerHTML = content;
  els.modalOverlay.classList.add("active");
}

function hideModal() {
  els.modalOverlay.classList.remove("active");
}

async function showVendorDetail(vendorCode) {
  const titles = {
    aliyun: "阿里云账单明细",
    aliyun_bailian: "阿里云百炼 Token 明细",
    aws: "亚马逊账单明细",
    volcengine: "火山引擎账单明细",
    stepfun: "阶跃星辰 Token 明细",
    deepseek: "DeepSeek Token 明细",
    moonshot: "月之暗面账单明细",
    tianyancha: "天眼查账单明细",
    textin: "TextIn 账单明细",
    dmxapi: "DMXAPI Token 明细",
  };

  const title = titles[vendorCode] || `${vendorCode} 明细`;
  let content = '<p class="muted">加载中...</p>';
  showModal(title, content);

  try {
    const client = buildClient();
    const start = els.startDate.value;
    const end = els.endDate.value;

    // Token 类供应商：显示 token 明细
    if (vendorCode === "aliyun_bailian" || vendorCode === "stepfun" || vendorCode === "deepseek") {
      // aliyun_bailian 需要同时查询 vendor="aliyun" 和 "aliyun_bailian"（历史数据兼容）
      const vendors = vendorCode === "aliyun_bailian" ? ["aliyun", "aliyun_bailian"] : [vendorCode];
      const { data } = await client
        .schema("financial_hub_prod")
        .from("llm_token_daily_usage")
        .select("day,project_id,total_tokens,input_tokens,output_tokens")
        .in("vendor", vendors)
        .gte("day", start)
        .lte("day", end)
        .order("day", { ascending: false });

      content = renderDetailTable(data || [], [
        { key: "day", label: "日期" },
        { key: "project_id", label: "项目ID" },
        { key: "total_tokens", label: "总 Token", render: (v) => formatNumber(v || 0), align: "right" },
        { key: "input_tokens", label: "输入", render: (v) => formatNumber(v || 0), align: "right" },
        { key: "output_tokens", label: "输出", render: (v) => formatNumber(v || 0), align: "right" },
      ]);
    } else if (vendorCode === "aliyun") {
      // 阿里云产品明细
      const { data } = await client
        .schema("financial_hub_prod")
        .from("aliyun_bill_daily")
        .select("billing_date,product_code,product_name,pretax_amount,pretax_gross_amount,is_ai_cost")
        .gte("billing_date", start)
        .lte("billing_date", end)
        .order("billing_date", { ascending: false });

      content = renderDetailTable(data || [], [
        { key: "billing_date", label: "日期" },
        { key: "product_code", label: "产品编码" },
        { key: "product_name", label: "产品名称" },
        { key: "is_ai_cost", label: "AI", render: (v) => v ? "是" : "否" },
        { key: "pretax_amount", label: "金额", render: formatCurrency, align: "right" },
      ]);
    } else if (vendorCode === "volcengine") {
      // 火山引擎明细
      const { data } = await client
        .schema("financial_hub_prod")
        .from("bill_daily_summary")
        .select("billing_date,is_ai_cost,amount,gross_amount,currency")
        .eq("vendor_code", "volcengine")
        .gte("billing_date", start)
        .lte("billing_date", end)
        .order("billing_date", { ascending: false });

      content = renderDetailTable(data || [], [
        { key: "billing_date", label: "日期" },
        { key: "is_ai_cost", label: "AI", render: (v) => v ? "是" : "否" },
        { key: "amount", label: "金额", render: formatCurrency, align: "right" },
        { key: "gross_amount", label: "原始金额", render: formatCurrency, align: "right" },
        { key: "currency", label: "币种" },
      ]);
    } else {
      // 其他供应商
      const { data } = await client
        .schema("financial_hub_prod")
        .from("bill_daily_summary")
        .select("billing_date,is_ai_cost,amount,gross_amount,currency")
        .eq("vendor_code", vendorCode)
        .gte("billing_date", start)
        .lte("billing_date", end)
        .order("billing_date", { ascending: false });

      content = renderDetailTable(data || [], [
        { key: "billing_date", label: "日期" },
        { key: "is_ai_cost", label: "AI", render: (v) => v ? "是" : "否" },
        { key: "amount", label: "金额", render: formatCurrency, align: "right" },
        { key: "currency", label: "币种" },
      ]);
    }
  } catch (err) {
    content = `<p class="error">加载失败: ${err.message}</p>`;
  }

  els.modalContent.innerHTML = content;
}

function renderDetailTable(rows, columns) {
  if (!rows.length) {
    return '<p class="muted">暂无数据</p>';
  }

  const ths = columns.map((c) => `<th class="${c.align === 'right' ? 'right' : ''}">${c.label}</th>`).join("");
  const trs = rows
    .map((row) => {
      const tds = columns
        .map((c) => {
          const val = row[c.key];
          const rendered = c.render ? c.render(val) : (val ?? "-");
          return `<td class="${c.align === 'right' ? 'right' : ''}">${rendered}</td>`;
        })
        .join("");
      return `<tr>${tds}</tr>`;
    })
    .join("");

  return `
    <div class="table-wrap" style="max-height: 500px;">
      <table>
        <thead><tr>${ths}</tr></thead>
        <tbody>${trs}</tbody>
      </table>
    </div>
    <p class="muted" style="margin-top: 12px;">共 ${rows.length} 条记录</p>
  `;
}

// 数据加载
const ALIYUN_AI_PRODUCT = "sfm";

async function loadBillData(client, startDate, endDate) {
  setStatus(els.billStatus, "加载中...");

  const { data, error } = await client
    .schema("financial_hub_prod")
    .from("aliyun_bill_daily")
    .select("billing_date,pretax_amount,pretax_gross_amount,is_ai_cost,product_code,product_name,currency")
    .gte("billing_date", startDate)
    .lte("billing_date", endDate);

  if (error) throw error;

  let total = 0, totalGross = 0, totalDiscount = 0, ai = 0, nonAi = 0;
  const productMap = new Map();

  data.forEach((row) => {
    const amount = Number(row.pretax_amount || 0);
    const gross = Number(row.pretax_gross_amount || 0);
    const discount = gross - amount;
    total += amount;
    totalGross += gross;
    totalDiscount += discount;
    if (row.is_ai_cost) ai += amount;
    else nonAi += amount;

    const key = `${row.product_code || ""}|${row.product_name || ""}|${row.is_ai_cost ? "1" : "0"}`;
    if (!productMap.has(key)) {
      productMap.set(key, {
        product_code: row.product_code,
        product_name: row.product_name,
        is_ai_cost: row.is_ai_cost,
        gross_amount: 0,
        discount_amount: 0,
        amount: 0,
      });
    }
    const t = productMap.get(key);
    t.gross_amount += gross;
    t.discount_amount += discount;
    t.amount += amount;
  });

  const productRows = Array.from(productMap.values()).sort((a, b) => b.amount - a.amount);

  els.billTotal.textContent = formatCurrency(total);
  els.billGrossTotal.textContent = formatCurrency(totalGross);
  els.billDiscountTotal.textContent = formatCurrency(totalDiscount);
  els.billAi.textContent = formatCurrency(ai);
  els.billNonAi.textContent = formatCurrency(nonAi);
  els.billRows.textContent = formatNumber(data.length);
  renderBillTable(productRows);
  setStatus(els.billStatus, `${data.length} 条`);

  // sfm 产品金额
  let sfmAmount = 0;
  data.forEach((row) => {
    if ((row.product_code || "").toLowerCase() === ALIYUN_AI_PRODUCT) {
      sfmAmount += Number(row.pretax_amount || 0);
    }
  });

  state.billData = { total, ai, nonAi, sfmAmount };
  return { aliyunSfmAmount: sfmAmount, aliyunNonAi: nonAi };
}

const VENDOR_NAME_MAP = {
  aliyun: "阿里云(账单)",
  aliyun_bailian: "阿里云百炼",
  aws: "AWS",
  volcengine: "火山引擎",
  stepfun: "阶跃星辰",
  deepseek: "DeepSeek",
  moonshot: "月之暗面",
  dmxapi: "DMXAPI",
};

function toVendorName(code) {
  return VENDOR_NAME_MAP[code?.toLowerCase()] || code || "-";
}

async function loadTokenData(client, startDate, endDate, vendorCosts) {
  setStatus(els.tokenStatus, "加载中...");

  // 查询日表数据
  const { data, error } = await client
    .schema("financial_hub_prod")
    .from("llm_token_daily_usage")
    .select("day,vendor,total_tokens")
    .gte("day", startDate)
    .lte("day", endDate);

  if (error) throw error;

  // 查询 DMXAPI 周表数据（DMXAPI 是手工录入周数据，不在日表中）
  const { data: dmxWeekly, error: dmxError } = await client
    .schema("financial_hub_prod")
    .from("llm_token_weekly_usage")
    .select("week_start,week_end,token_total")
    .eq("vendor_code", "dmxapi")
    .gte("week_start", startDate)
    .lte("week_end", endDate);

  if (dmxError) throw dmxError;

  const vendorMap = new Map();
  let totalTokens = 0;

  data.forEach((row) => {
    const tokens = Number(row.total_tokens || 0);
    totalTokens += tokens;
    // Token 表中的 "aliyun" 实际是百炼 token 数据，统一映射为 "aliyun_bailian"
    let vendor = (row.vendor || "").toLowerCase();
    if (vendor === "aliyun") vendor = "aliyun_bailian";
    if (!vendorMap.has(vendor)) {
      vendorMap.set(vendor, { tokens: 0, cost: null });
    }
    vendorMap.get(vendor).tokens += tokens;
  });

  // 加入 DMXAPI 周表数据
  if (dmxWeekly && dmxWeekly.length > 0) {
    const dmxTokens = dmxWeekly.reduce((sum, row) => sum + Number(row.token_total || 0), 0);
    totalTokens += dmxTokens;
    vendorMap.set("dmxapi", { tokens: dmxTokens, cost: vendorCosts.dmxapi || null });
  }

  // 关联金额（aliyun_bailian 使用阿里云 sfm 的金额）
  if (vendorMap.has("aliyun_bailian")) vendorMap.get("aliyun_bailian").cost = vendorCosts.aliyun || 0;
  if (vendorMap.has("volcengine")) vendorMap.get("volcengine").cost = vendorCosts.volcengine || 0;
  if (vendorMap.has("stepfun")) vendorMap.get("stepfun").cost = vendorCosts.stepfun || 0;
  if (vendorMap.has("deepseek")) vendorMap.get("deepseek").cost = vendorCosts.deepseek || 0;
  if (vendorMap.has("moonshot")) vendorMap.get("moonshot").cost = vendorCosts.moonshot || 0;

  const vendorRows = Array.from(vendorMap.entries())
    .map(([vendor, d]) => ({
      vendor: toVendorName(vendor),
      vendorCode: vendor,
      total_tokens: d.tokens,
      cost: d.cost,
    }))
    .sort((a, b) => b.total_tokens - a.total_tokens);

  let totalCost = 0;
  vendorRows.forEach((row) => {
    if (row.cost !== null) totalCost += row.cost;
  });

  els.tokenTotal.textContent = formatTokenYi(totalTokens);
  els.tokenTotalCost.textContent = formatCurrency(totalCost);
  els.tokenVendorCount.textContent = formatNumber(vendorRows.length);
  renderTokenTable(vendorRows);
  setStatus(els.tokenStatus, `${data.length} 条`);

  // 返回各供应商的 token 数据
  const vendorTokens = {};
  vendorMap.forEach((v, k) => { vendorTokens[k] = v.tokens; });

  return { totalTokens, totalCost, vendorTokens };
}

async function loadAliyunTokenDaily(client, startDate, endDate) {
  setStatus(els.aliyunTokenStatus, "加载中...");

  const { data, error } = await client
    .schema("financial_hub_prod")
    .from("llm_token_daily_usage")
    .select("day,project_id,total_tokens")
    .eq("vendor", "aliyun")
    .gte("day", startDate)
    .lte("day", endDate)
    .order("day", { ascending: false });

  if (error) throw error;

  const total = data.reduce((sum, row) => sum + Number(row.total_tokens || 0), 0);

  els.aliyunTokenTotal.textContent = formatTokenYi(total);
  els.aliyunTokenRows.textContent = formatNumber(data.length);
  renderAliyunTokenTable(data);
  setStatus(els.aliyunTokenStatus, `${data.length} 条`);
}

async function loadDmxapiWeekly(client, startDate, endDate) {
  setStatus(els.dmxapiStatus, "加载中...");

  // 获取 Token 数据
  const { data: tokenData, error: tokenError } = await client
    .schema("financial_hub_prod")
    .from("llm_token_weekly_usage")
    .select("week_start,week_end,token_total")
    .eq("vendor_code", "dmxapi")
    .gte("week_start", startDate)
    .lte("week_end", endDate)
    .order("week_start", { ascending: false });

  if (tokenError) throw tokenError;

  // 获取金额数据
  const { data: billData, error: billError } = await client
    .schema("financial_hub_prod")
    .from("bill_weekly_summary")
    .select("week_start,week_end,amount,currency")
    .eq("vendor_code", "dmxapi")
    .gte("week_start", startDate)
    .lte("week_end", endDate)
    .order("week_start", { ascending: false });

  if (billError) throw billError;

  // 合并数据
  const billMap = new Map();
  (billData || []).forEach((row) => {
    const key = `${row.week_start}_${row.week_end}`;
    billMap.set(key, row);
  });

  const totalTokens = (tokenData || []).reduce((sum, row) => sum + Number(row.token_total || 0), 0);
  const totalAmount = (billData || []).reduce((sum, row) => sum + Number(row.amount || 0), 0);

  els.dmxapiTotal.textContent = formatTokenYi(totalTokens);
  els.dmxapiWeeks.textContent = formatNumber((tokenData || []).length);
  if (els.dmxapiAmount) {
    els.dmxapiAmount.textContent = formatCurrency(totalAmount);
  }
  
  // 渲染表格
  els.dmxapiTable.innerHTML = "";
  if (!tokenData || tokenData.length === 0) {
    els.dmxapiTable.innerHTML = '<tr><td colspan="4" class="muted">暂无数据</td></tr>';
  } else {
    tokenData.forEach((row) => {
      const key = `${row.week_start}_${row.week_end}`;
      const bill = billMap.get(key);
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${row.week_start || "-"}</td>
        <td>${row.week_end || "-"}</td>
        <td class="right">${formatTokenYi(row.token_total)}</td>
        <td class="right">${bill ? formatCurrency(bill.amount, bill.currency) : "-"}</td>
      `;
      els.dmxapiTable.appendChild(tr);
    });
  }
  setStatus(els.dmxapiStatus, `${(tokenData || []).length} 周`);
  
  return { dmxapiTotal: totalTokens, dmxapiAmount: totalAmount };
}

async function loadAwsBillDaily(client, startDate, endDate) {
  setStatus(els.awsBillStatus, "加载中...");
  setStatus(els.awsOverviewStatus, "加载中...");

  const { data, error } = await client
    .schema("financial_hub_prod")
    .from("bill_daily_summary")
    .select("billing_date,is_ai_cost,amount,gross_amount,currency")
    .eq("vendor_code", "aws")
    .gte("billing_date", startDate)
    .lte("billing_date", endDate)
    .order("billing_date", { ascending: false });

  if (error) throw error;

  let total = 0, currency = "USD";
  (data || []).forEach((row) => {
    total += Number(row.amount || 0);
    if (row.currency) currency = row.currency;
  });

  els.awsBillTotal.textContent = formatCurrency(total, currency);
  els.awsBillRows.textContent = formatNumber(data.length);
  renderDailyBillTable(data || [], els.awsBillTable);
  setStatus(els.awsBillStatus, `${data.length} 条`);

  els.awsOverviewTotal.textContent = formatCurrency(total, currency);
  els.awsOverviewRows.textContent = formatNumber(data.length);
  setStatus(els.awsOverviewStatus, `${data.length} 条`);

  state.awsData = { total, currency };
  return { awsTotal: total };
}

async function loadVolcBillDaily(client, startDate, endDate) {
  setStatus(els.volcBillStatus, "加载中...");
  setStatus(els.volcOverviewStatus, "加载中...");

  const { data, error } = await client
    .schema("financial_hub_prod")
    .from("bill_daily_summary")
    .select("billing_date,is_ai_cost,amount,gross_amount,currency")
    .eq("vendor_code", "volcengine")
    .gte("billing_date", startDate)
    .lte("billing_date", endDate)
    .order("billing_date", { ascending: false });

  if (error) throw error;

  let total = 0, aiTotal = 0, nonAiTotal = 0, currency = "CNY";
  (data || []).forEach((row) => {
    const amt = Number(row.amount || 0);
    total += amt;
    if (row.is_ai_cost) aiTotal += amt;
    else nonAiTotal += amt;
    if (row.currency) currency = row.currency;
  });

  els.volcBillTotal.textContent = formatCurrency(total, currency);
  els.volcBillRows.textContent = formatNumber(data.length);
  renderDailyBillTable(data || [], els.volcBillTable);
  setStatus(els.volcBillStatus, `${data.length} 条`);

  els.volcOverviewTotal.textContent = formatCurrency(total, currency);
  els.volcOverviewAi.textContent = formatCurrency(aiTotal, currency);
  els.volcOverviewNonAi.textContent = formatCurrency(nonAiTotal, currency);
  els.volcOverviewRows.textContent = formatNumber(data.length);
  setStatus(els.volcOverviewStatus, `${data.length} 条`);

  state.volcData = { total, aiTotal, nonAiTotal };
  return { volcAiAmount: aiTotal, volcNonAiAmount: nonAiTotal };
}

async function loadTianyanchaBillDaily(client, startDate, endDate) {
  setStatus(els.tianBillStatus, "加载中...");
  setStatus(els.tianOverviewStatus, "加载中...");

  const { data, error } = await client
    .schema("financial_hub_prod")
    .from("bill_daily_summary")
    .select("billing_date,is_ai_cost,amount,gross_amount,currency")
    .eq("vendor_code", "tianyancha")
    .gte("billing_date", startDate)
    .lte("billing_date", endDate)
    .order("billing_date", { ascending: false });

  if (error) throw error;

  let total = 0, currency = "CNY";
  const rows = (data || []).map((row) => ({ ...row, is_ai_cost: false }));
  rows.forEach((row) => {
    total += Number(row.amount || 0);
    if (row.currency) currency = row.currency;
  });

  els.tianBillTotal.textContent = formatCurrency(total, currency);
  els.tianBillRows.textContent = formatNumber(rows.length);
  renderDailyBillTable(rows, els.tianBillTable);
  setStatus(els.tianBillStatus, `${rows.length} 条`);

  els.tianOverviewTotal.textContent = formatCurrency(total, currency);
  els.tianOverviewRows.textContent = formatNumber(rows.length);
  setStatus(els.tianOverviewStatus, `${rows.length} 条`);

  state.tianData = { total };
  return { tianTotal: total };
}

async function loadMoonshotBillDaily(client, startDate, endDate) {
  setStatus(els.moonshotOverviewStatus, "加载中...");

  const { data, error } = await client
    .schema("financial_hub_prod")
    .from("bill_daily_summary")
    .select("billing_date,is_ai_cost,amount,gross_amount,currency")
    .eq("vendor_code", "moonshot")
    .gte("billing_date", startDate)
    .lte("billing_date", endDate);

  if (error) throw error;

  let total = 0, currency = "CNY";
  (data || []).forEach((row) => {
    total += Number(row.amount || 0);
    if (row.currency) currency = row.currency;
  });

  els.moonshotOverviewTotal.textContent = formatCurrency(total, currency);
  els.moonshotOverviewRows.textContent = formatNumber(data.length);
  setStatus(els.moonshotOverviewStatus, `${data.length} 条`);

  state.moonshotData = { total };
  return { moonshotTotal: total };
}

async function loadDeepseekBillDaily(client, startDate, endDate) {
  const { data, error } = await client
    .schema("financial_hub_prod")
    .from("bill_daily_summary")
    .select("billing_date,is_ai_cost,amount,gross_amount,currency")
    .eq("vendor_code", "deepseek")
    .gte("billing_date", startDate)
    .lte("billing_date", endDate);

  if (error) throw error;

  let total = 0, currency = "CNY";
  (data || []).forEach((row) => {
    total += Number(row.amount || 0);
    if (row.currency) currency = row.currency;
  });

  return { deepseekAmount: total };
}

async function loadStepfunBillDaily(client, startDate, endDate) {
  setStatus(els.stepfunOverviewStatus, "加载中...");

  const { data, error } = await client
    .schema("financial_hub_prod")
    .from("bill_daily_summary")
    .select("billing_date,is_ai_cost,amount,gross_amount,currency")
    .eq("vendor_code", "stepfun")
    .gte("billing_date", startDate)
    .lte("billing_date", endDate);

  if (error) throw error;

  let total = 0, currency = "CNY";
  (data || []).forEach((row) => {
    total += Number(row.amount || 0);
    if (row.currency) currency = row.currency;
  });

  els.stepfunOverviewTotal.textContent = formatCurrency(total, currency);
  els.stepfunOverviewRows.textContent = formatNumber(data.length);
  setStatus(els.stepfunOverviewStatus, `${data.length} 条`);

  state.stepfunData = { total };
  return { stepfunAmount: total };
}

async function loadAuthingBillMonthly(client, startDate, endDate) {
  setStatus(els.authingOverviewStatus, "加载中...");

  const startMonth = startDate.slice(0, 7);
  const endMonth = endDate.slice(0, 7);

  const { data, error } = await client
    .schema("financial_hub_prod")
    .from("bill_monthly_summary")
    .select("month,amount,currency")
    .eq("vendor_code", "authing")
    .gte("month", startMonth)
    .lte("month", endMonth);

  if (error) throw error;

  let total = 0, currency = "CNY";
  (data || []).forEach((row) => {
    total += Number(row.amount || 0);
    if (row.currency) currency = row.currency;
  });

  els.authingOverviewTotal.textContent = formatCurrency(total, currency);
  els.authingOverviewRows.textContent = formatNumber(data.length);
  setStatus(els.authingOverviewStatus, `${data.length} 条`);

  state.authingData = { total };
  return { authingTotal: total };
}

async function loadTextinBillDaily(client, startDate, endDate) {
  setStatus(els.textinOverviewStatus, "加载中...");

  const { data, error } = await client
    .schema("financial_hub_prod")
    .from("bill_daily_summary")
    .select("billing_date,is_ai_cost,amount,currency")
    .eq("vendor_code", "textin")
    .gte("billing_date", startDate)
    .lte("billing_date", endDate);

  if (error) throw error;

  let total = 0, currency = "CNY";
  (data || []).forEach((row) => {
    total += Number(row.amount || 0);
    if (row.currency) currency = row.currency;
  });

  els.textinOverviewTotal.textContent = formatCurrency(total, currency);
  els.textinOverviewRows.textContent = formatNumber(data.length);
  setStatus(els.textinOverviewStatus, `${data.length} 条`);

  state.textinData = { total };
  return { textinTotal: total };
}

// 更新汇总
function updateSummary(results) {
  const vt = results.vendorTokens || {};
  // AI 消耗明细（包含 token）
  state.aiBreakdown = [
    { name: "阿里云百炼", amount: results.aliyunSfmAmount || 0, tokens: vt.aliyun_bailian || 0 },
    { name: "火山引擎(AI)", amount: results.volcAiAmount || 0, tokens: vt.volcengine || 0 },
    { name: "阶跃星辰", amount: results.stepfunAmount || 0, tokens: vt.stepfun || 0 },
    { name: "月之暗面", amount: results.moonshotTotal || 0, tokens: vt.moonshot || 0 },
    { name: "DeepSeek", amount: results.deepseekAmount || 0, tokens: vt.deepseek || 0 },
    { name: "DMXAPI", amount: results.dmxapiAmount || 0, tokens: vt.dmxapi || 0 },
  ];

  // 非AI 消耗明细
  state.nonAiBreakdown = [
    { name: "阿里云(非AI)", amount: results.aliyunNonAi || 0 },
    { name: "亚马逊 AWS", amount: results.awsTotal || 0 },
    { name: "火山引擎(非AI)", amount: results.volcNonAiAmount || 0 },
    { name: "TextIn", amount: results.textinTotal || 0 },
    { name: "Authing", amount: results.authingTotal || 0 },
    { name: "天眼查", amount: results.tianTotal || 0 },
  ];

  const aiCost = state.aiBreakdown.reduce((sum, item) => sum + item.amount, 0);
  const nonAiCost = state.nonAiBreakdown.reduce((sum, item) => sum + item.amount, 0);

  els.aiTotalCost.textContent = formatNumber(aiCost.toFixed(2));
  els.aiTotalToken.textContent = formatTokenYi(results.totalTokens || 0);
  els.nonAiTotalCost.textContent = formatNumber(nonAiCost.toFixed(2));
}

// 显示汇总明细
function showSummaryDetail(type) {
  const isAi = type === "ai";
  const title = isAi ? "AI 消耗明细" : "非 AI 消耗明细";
  const breakdown = isAi ? state.aiBreakdown : state.nonAiBreakdown;

  if (!breakdown || !breakdown.length) {
    showModal(title, '<p class="muted">暂无数据</p>');
    return;
  }

  const total = breakdown.reduce((sum, item) => sum + item.amount, 0);
  const totalTokens = isAi ? breakdown.reduce((sum, item) => sum + (item.tokens || 0), 0) : 0;
  
  const rows = breakdown
    .filter((item) => item.amount > 0 || (isAi && item.tokens > 0))
    .sort((a, b) => b.amount - a.amount)
    .map((item) => {
      const pct = total > 0 ? ((item.amount / total) * 100).toFixed(1) : 0;
      if (isAi) {
        return `
          <tr>
            <td>${item.name}</td>
            <td class="right">${formatTokenYi(item.tokens || 0)}</td>
            <td class="right">${formatCurrency(item.amount)}</td>
            <td class="right">${pct}%</td>
          </tr>
        `;
      } else {
        return `
          <tr>
            <td>${item.name}</td>
            <td class="right">${formatCurrency(item.amount)}</td>
            <td class="right">${pct}%</td>
          </tr>
        `;
      }
    })
    .join("");

  const headerRow = isAi
    ? '<tr><th>供应商</th><th class="right">Token(亿)</th><th class="right">金额</th><th class="right">占比</th></tr>'
    : '<tr><th>供应商</th><th class="right">金额</th><th class="right">占比</th></tr>';

  const summaryText = isAi
    ? `Token: ${formatTokenYi(totalTokens)} | 合计: ${formatCurrency(total)}`
    : `合计: ${formatCurrency(total)}`;

  const content = `
    <div class="detail-summary">
      <span class="detail-total">${summaryText}</span>
    </div>
    <table class="detail-table">
      <thead>${headerRow}</thead>
      <tbody>${rows}</tbody>
    </table>
  `;

  showModal(title, content);
}

async function refresh() {
  try {
    setError("");
    const client = buildClient();
    const start = els.startDate.value;
    const end = els.endDate.value;

    // 先加载账单数据
    const [billResult, volcResult, stepfunResult, awsResult, tianResult, moonshotResult, textinResult, authingResult, deepseekResult] =
      await Promise.all([
        loadBillData(client, start, end),
        loadVolcBillDaily(client, start, end),
        loadStepfunBillDaily(client, start, end),
        loadAwsBillDaily(client, start, end),
        loadTianyanchaBillDaily(client, start, end),
        loadMoonshotBillDaily(client, start, end),
        loadTextinBillDaily(client, start, end),
        loadAuthingBillMonthly(client, start, end),
        loadDeepseekBillDaily(client, start, end),
      ]);

    // 先加载 DMXAPI（周数据），以便在 loadTokenData 中合并显示
    const dmxapiResult = await loadDmxapiWeekly(client, start, end);

    const vendorCosts = {
      aliyun: billResult.aliyunSfmAmount,
      volcengine: volcResult.volcAiAmount,
      stepfun: stepfunResult.stepfunAmount,
      deepseek: deepseekResult.deepseekAmount,
      moonshot: moonshotResult.moonshotTotal,
      dmxapi: dmxapiResult.dmxapiAmount || 0,
    };

    const tokenResult = await loadTokenData(client, start, end, vendorCosts);
    await loadAliyunTokenDaily(client, start, end);

    // 更新汇总
    updateSummary({
      ...billResult,
      ...volcResult,
      ...stepfunResult,
      ...awsResult,
      ...tianResult,
      ...moonshotResult,
      ...textinResult,
      ...authingResult,
      ...deepseekResult,
      ...dmxapiResult,
      ...tokenResult,
    });

    els.lastRefresh.textContent = new Date().toLocaleString("zh-CN");
  } catch (err) {
    setError(err.message || "数据加载失败");
  }
}

// 事件绑定
els.applyRange.addEventListener("click", () => {
  state.startDate = new Date(els.startDate.value);
  state.endDate = new Date(els.endDate.value);
  updateRangeHint();
  document.querySelectorAll(".shortcut-btn").forEach((btn) => btn.classList.remove("active"));
  refresh();
});

els.refreshButton.addEventListener("click", refresh);

document.querySelectorAll(".shortcut-btn").forEach((btn) => {
  btn.addEventListener("click", () => setDateRange(btn.dataset.range));
});

els.tabs.forEach((tab) => {
  tab.addEventListener("click", () => {
    els.tabs.forEach((t) => t.classList.remove("active"));
    tab.classList.add("active");
    Object.entries(els.tabPanels).forEach(([key, panel]) => {
      if (panel) panel.classList.toggle("active", key === tab.dataset.tab);
    });
  });
});

// 点击供应商标题显示详情
document.querySelectorAll(".section-header h2.clickable").forEach((h2) => {
  h2.addEventListener("click", () => {
    const vendor = h2.dataset.vendor;
    if (vendor) showVendorDetail(vendor);
  });
});

// 弹窗关闭
els.modalClose.addEventListener("click", hideModal);
els.modalOverlay.addEventListener("click", (e) => {
  if (e.target === els.modalOverlay) hideModal();
});

// 汇总卡片点击
els.aiSummaryCard.addEventListener("click", () => showSummaryDetail("ai"));
els.nonAiSummaryCard.addEventListener("click", () => showSummaryDetail("non-ai"));

// DeepSeek 相关函数
function initDeepseekForm() {
  const now = new Date();
  els.deepseekYear.value = now.getFullYear();
  els.deepseekMonth.value = now.getMonth() + 1;
  
  // 从 localStorage 恢复 token
  const savedToken = localStorage.getItem("deepseek_auth_token");
  if (savedToken) {
    els.deepseekAuth.value = savedToken;
  }
}

function parseDeepseekCost(data) {
  const bizDataList = data?.data?.biz_data || [];
  if (!bizDataList.length) {
    return { totalCost: 0, currency: "CNY", models: {}, daily: [] };
  }

  const bizData = bizDataList[0];
  const currency = bizData.currency || "CNY";
  const totalList = bizData.total || [];
  const models = {};
  let totalCost = 0;

  for (const modelData of totalList) {
    const modelName = modelData.model || "unknown";
    const usageList = modelData.usage || [];

    const modelCost = {
      prompt_cache_hit: 0,
      prompt_cache_miss: 0,
      response_token: 0,
      total: 0,
    };

    for (const usage of usageList) {
      const usageType = usage.type || "";
      const amount = parseFloat(usage.amount) || 0;

      if (usageType === "PROMPT_CACHE_HIT_TOKEN") {
        modelCost.prompt_cache_hit = amount;
      } else if (usageType === "PROMPT_CACHE_MISS_TOKEN") {
        modelCost.prompt_cache_miss = amount;
      } else if (usageType === "RESPONSE_TOKEN") {
        modelCost.response_token = amount;
      }
    }

    modelCost.total = modelCost.prompt_cache_hit + modelCost.prompt_cache_miss + modelCost.response_token;
    models[modelName] = modelCost;
    totalCost += modelCost.total;
  }

  // 解析每日数据
  const daysList = bizData.days || [];
  const daily = [];

  for (const dayData of daysList) {
    const dateStr = dayData.date || "";
    const dayModels = dayData.data || [];

    let dayCost = 0;
    for (const modelData of dayModels) {
      const usageList = modelData.usage || [];
      for (const usage of usageList) {
        const usageType = usage.type || "";
        const amount = parseFloat(usage.amount) || 0;
        if (["PROMPT_CACHE_HIT_TOKEN", "PROMPT_CACHE_MISS_TOKEN", "RESPONSE_TOKEN"].includes(usageType)) {
          dayCost += amount;
        }
      }
    }

    daily.push({ date: dateStr, total: dayCost });
  }

  return { totalCost, currency, models, daily };
}

function renderDeepseekResult(result) {
  els.deepseekTotalCost.textContent = formatCurrency(result.totalCost);
  els.deepseekCurrency.textContent = result.currency;
  
  const modelNames = Object.keys(result.models);
  els.deepseekModelCount.textContent = modelNames.length;

  // 渲染模型表格
  els.deepseekModelTable.innerHTML = "";
  if (modelNames.length === 0) {
    els.deepseekModelTable.innerHTML = '<tr><td colspan="5" class="muted">暂无数据</td></tr>';
  } else {
    for (const [name, cost] of Object.entries(result.models)) {
      if (cost.total > 0) {
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td>${name}</td>
          <td class="right">${formatCurrency(cost.prompt_cache_hit)}</td>
          <td class="right">${formatCurrency(cost.prompt_cache_miss)}</td>
          <td class="right">${formatCurrency(cost.response_token)}</td>
          <td class="right"><strong>${formatCurrency(cost.total)}</strong></td>
        `;
        els.deepseekModelTable.appendChild(tr);
      }
    }
  }

  // 渲染每日表格
  els.deepseekDailyTable.innerHTML = "";
  const nonZeroDays = result.daily.filter((d) => d.total > 0);
  if (nonZeroDays.length === 0) {
    els.deepseekDailyTable.innerHTML = '<tr><td colspan="2" class="muted">暂无数据</td></tr>';
  } else {
    // 按日期倒序
    nonZeroDays.sort((a, b) => b.date.localeCompare(a.date));
    for (const day of nonZeroDays) {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${day.date}</td>
        <td class="right">${formatCurrency(day.total)}</td>
      `;
      els.deepseekDailyTable.appendChild(tr);
    }
  }

  els.deepseekResult.style.display = "block";
}

async function fetchDeepseekCost() {
  const authToken = els.deepseekAuth.value.trim();
  const year = parseInt(els.deepseekYear.value, 10);
  const month = parseInt(els.deepseekMonth.value, 10);

  if (!authToken) {
    setStatus(els.deepseekStatus, "请输入 Authorization Token");
    return;
  }

  // 保存 token 到 localStorage
  localStorage.setItem("deepseek_auth_token", authToken);

  setStatus(els.deepseekStatus, "加载中...");
  els.deepseekResult.style.display = "none";

  try {
    const url = `https://platform.deepseek.com/api/v0/usage/cost?year=${year}&month=${month}`;
    
    const response = await fetch(url, {
      method: "GET",
      headers: {
        "accept": "*/*",
        "authorization": `Bearer ${authToken}`,
        "x-app-version": "20240425.0",
      },
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    const data = await response.json();

    if (data.code !== 0) {
      throw new Error(`API 错误: ${data.msg || "未知错误"}`);
    }

    const result = parseDeepseekCost(data);
    renderDeepseekResult(result);
    setStatus(els.deepseekStatus, `已加载 ${year}年${month}月数据`);
  } catch (err) {
    setStatus(els.deepseekStatus, `错误: ${err.message}`);
    console.error("DeepSeek API error:", err);
  }
}

// DeepSeek 事件绑定
els.fetchDeepseek.addEventListener("click", fetchDeepseekCost);

// 初始化主应用（登录后调用）
function initApp() {
  initDateRange();
  initDeepseekForm();
  refresh();
}

// 页面加载时初始化认证
initAuth();
