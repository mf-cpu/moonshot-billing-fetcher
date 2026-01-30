const state = {
  startDate: null,
  endDate: null,
};

const els = {
  startDate: document.getElementById("startDate"),
  endDate: document.getElementById("endDate"),
  applyRange: document.getElementById("applyRange"),
  refreshButton: document.getElementById("refreshButton"),
  rangeHint: document.getElementById("rangeHint"),
  billStatus: document.getElementById("billStatus"),
  billTotal: document.getElementById("billTotal"),
  billGrossTotal: document.getElementById("billGrossTotal"),
  billDiscountTotal: document.getElementById("billDiscountTotal"),
  billAi: document.getElementById("billAi"),
  billNonAi: document.getElementById("billNonAi"),
  billRows: document.getElementById("billRows"),
  billProductTable: document.getElementById("billProductTable"),
  tokenStatus: document.getElementById("tokenStatus"),
  tokenTotal: document.getElementById("tokenTotal"),
  tokenVendorCount: document.getElementById("tokenVendorCount"),
  tokenTable: document.getElementById("tokenTable"),
  aliyunTokenStatus: document.getElementById("aliyunTokenStatus"),
  aliyunTokenTotal: document.getElementById("aliyunTokenTotal"),
  aliyunTokenRows: document.getElementById("aliyunTokenRows"),
  aliyunTokenTable: document.getElementById("aliyunTokenTable"),
  awsBillStatus: document.getElementById("awsBillStatus"),
  awsBillTotal: document.getElementById("awsBillTotal"),
  awsBillGrossTotal: document.getElementById("awsBillGrossTotal"),
  awsBillRows: document.getElementById("awsBillRows"),
  awsBillTable: document.getElementById("awsBillTable"),
  awsOverviewStatus: document.getElementById("awsOverviewStatus"),
  awsOverviewTotal: document.getElementById("awsOverviewTotal"),
  awsOverviewGrossTotal: document.getElementById("awsOverviewGrossTotal"),
  awsOverviewRows: document.getElementById("awsOverviewRows"),
  tianOverviewStatus: document.getElementById("tianOverviewStatus"),
  tianOverviewTotal: document.getElementById("tianOverviewTotal"),
  tianOverviewGrossTotal: document.getElementById("tianOverviewGrossTotal"),
  tianOverviewRows: document.getElementById("tianOverviewRows"),
  moonshotOverviewStatus: document.getElementById("moonshotOverviewStatus"),
  moonshotOverviewTotal: document.getElementById("moonshotOverviewTotal"),
  moonshotOverviewGrossTotal: document.getElementById("moonshotOverviewGrossTotal"),
  moonshotOverviewRows: document.getElementById("moonshotOverviewRows"),
  authingOverviewStatus: document.getElementById("authingOverviewStatus"),
  authingOverviewTotal: document.getElementById("authingOverviewTotal"),
  authingOverviewRows: document.getElementById("authingOverviewRows"),
  textinOverviewStatus: document.getElementById("textinOverviewStatus"),
  textinOverviewTotal: document.getElementById("textinOverviewTotal"),
  textinOverviewGrossTotal: document.getElementById("textinOverviewGrossTotal"),
  textinOverviewRows: document.getElementById("textinOverviewRows"),
  volcBillStatus: document.getElementById("volcBillStatus"),
  volcBillTotal: document.getElementById("volcBillTotal"),
  volcBillGrossTotal: document.getElementById("volcBillGrossTotal"),
  volcBillRows: document.getElementById("volcBillRows"),
  volcBillTable: document.getElementById("volcBillTable"),
  volcOverviewStatus: document.getElementById("volcOverviewStatus"),
  volcOverviewTotal: document.getElementById("volcOverviewTotal"),
  volcOverviewGrossTotal: document.getElementById("volcOverviewGrossTotal"),
  volcOverviewRows: document.getElementById("volcOverviewRows"),
  tianBillStatus: document.getElementById("tianBillStatus"),
  tianBillTotal: document.getElementById("tianBillTotal"),
  tianBillGrossTotal: document.getElementById("tianBillGrossTotal"),
  tianBillRows: document.getElementById("tianBillRows"),
  tianBillTable: document.getElementById("tianBillTable"),
  lastRefresh: document.getElementById("lastRefresh"),
  errorBox: document.getElementById("errorBox"),
  tabs: Array.from(document.querySelectorAll(".tab")),
  tabPanels: {
    overview: document.getElementById("tab-overview"),
    fetch: document.getElementById("tab-fetch"),
  },
};

function formatCurrency(amount, currency = "CNY") {
  if (Number.isNaN(amount)) return "-";
  return new Intl.NumberFormat("zh-CN", {
    style: "currency",
    currency,
    maximumFractionDigits: 2,
  }).format(amount);
}

function formatCurrencyMaybe(amount, currency = "CNY") {
  if (amount === null || amount === undefined) return "-";
  return formatCurrency(amount, currency);
}

function formatNumber(num) {
  if (Number.isNaN(num)) return "-";
  return new Intl.NumberFormat("zh-CN").format(num);
}

function setStatus(el, text) {
  el.textContent = text;
}

function setError(text) {
  els.errorBox.textContent = text || "无";
  if (text) {
    els.errorBox.classList.add("error");
  } else {
    els.errorBox.classList.remove("error");
  }
}

function initDateRange() {
  const today = new Date();
  const end = new Date(today);
  end.setDate(end.getDate() - 1);
  const start = new Date(end);
  start.setDate(start.getDate() - 29);

  state.startDate = start;
  state.endDate = end;

  els.startDate.value = start.toISOString().slice(0, 10);
  els.endDate.value = end.toISOString().slice(0, 10);
  els.rangeHint.textContent = "默认近 30 天（不含今天）";
}

function buildClient() {
  if (!window.SUPABASE_URL || !window.SUPABASE_ANON_KEY) {
    throw new Error("请先配置 ops_dashboard/config.js 中的 Supabase 信息");
  }
  return window.supabase.createClient(
    window.SUPABASE_URL,
    window.SUPABASE_ANON_KEY
  );
}

function renderBillTable(rows) {
  els.billProductTable.innerHTML = "";
  if (!rows.length) {
    els.billProductTable.innerHTML =
      '<tr><td colspan="6" class="muted">暂无数据</td></tr>';
    return;
  }

  rows.forEach((row) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${row.product_code || "-"}</td>
      <td>${row.product_name || "-"}</td>
      <td>${row.is_ai_cost ? "是" : "否"}</td>
      <td class="right">${formatCurrencyMaybe(row.gross_amount)}</td>
      <td class="right">${formatCurrencyMaybe(row.discount_amount)}</td>
      <td class="right">${formatCurrency(row.amount)}</td>
    `;
    els.billProductTable.appendChild(tr);
  });
}

function renderTokenTable(rows) {
  els.tokenTable.innerHTML = "";
  if (!rows.length) {
    els.tokenTable.innerHTML =
      '<tr><td colspan="2" class="muted">暂无数据</td></tr>';
    return;
  }
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${row.vendor || "-"}</td>
      <td class="right">${formatNumber(row.total_tokens)}</td>
    `;
    els.tokenTable.appendChild(tr);
  });
}

function renderAliyunTokenTable(rows) {
  els.aliyunTokenTable.innerHTML = "";
  if (!rows.length) {
    els.aliyunTokenTable.innerHTML =
      '<tr><td colspan="3" class="muted">暂无数据</td></tr>';
    return;
  }
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${row.day}</td>
      <td>${row.project_id || "-"}</td>
      <td class="right">${formatNumber(row.total_tokens)}</td>
    `;
    els.aliyunTokenTable.appendChild(tr);
  });
}

function renderDailyBillTable(rows, tableEl) {
  tableEl.innerHTML = "";
  if (!rows.length) {
    tableEl.innerHTML =
      '<tr><td colspan="5" class="muted">暂无数据</td></tr>';
    return;
  }
  rows.forEach((row) => {
    const currency = row.currency || "USD";
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${row.billing_date}</td>
      <td>${row.is_ai_cost ? "是" : "否"}</td>
      <td class="right">${formatCurrency(row.amount, currency)}</td>
      <td class="right">${formatCurrency(row.gross_amount, currency)}</td>
      <td>${currency}</td>
    `;
    tableEl.appendChild(tr);
  });
}

async function fetchBillRows(client, startDate, endDate) {
  const selectWithGross =
    "billing_date,pretax_amount,pretax_gross_amount,is_ai_cost,product_code,product_name,currency";
  const selectWithoutGross =
    "billing_date,pretax_amount,is_ai_cost,product_code,product_name,currency";

  const { data, error } = await client
    .schema("financial_hub_prod")
    .from("aliyun_bill_daily")
    .select(selectWithGross)
    .gte("billing_date", startDate)
    .lte("billing_date", endDate);

  if (!error) {
    return { data, hasGross: true };
  }

  if ((error.message || "").includes("pretax_gross_amount")) {
    const fallback = await client
      .schema("financial_hub_prod")
      .from("aliyun_bill_daily")
      .select(selectWithoutGross)
      .gte("billing_date", startDate)
      .lte("billing_date", endDate);
    if (fallback.error) {
      throw fallback.error;
    }
    return { data: fallback.data, hasGross: false };
  }

  throw error;
}

async function loadBillData(client, startDate, endDate) {
  setStatus(els.billStatus, "加载中...");

  const { data, hasGross } = await fetchBillRows(client, startDate, endDate);

  let total = 0;
  let totalGross = 0;
  let totalDiscount = 0;
  let ai = 0;
  let nonAi = 0;

  const productMap = new Map();
  data.forEach((row) => {
    const amount = Number(row.pretax_amount || 0);
    const gross = hasGross ? Number(row.pretax_gross_amount || 0) : null;
    const discount = hasGross ? gross - amount : null;
    total += amount;
    if (hasGross) {
      totalGross += gross;
      totalDiscount += discount;
    }
    if (row.is_ai_cost) {
      ai += amount;
    } else {
      nonAi += amount;
    }

    const key = [
      row.product_code || "",
      row.product_name || "",
      row.is_ai_cost ? "1" : "0",
    ].join("|");

    if (!productMap.has(key)) {
      productMap.set(key, {
        product_code: row.product_code,
        product_name: row.product_name,
        is_ai_cost: row.is_ai_cost,
        gross_amount: hasGross ? 0 : null,
        discount_amount: hasGross ? 0 : null,
        amount: 0,
      });
    }
    const target = productMap.get(key);
    if (hasGross) {
      target.gross_amount += gross;
      target.discount_amount += discount;
    }
    target.amount += amount;
  });

  const productRows = Array.from(productMap.values()).sort(
    (a, b) => b.amount - a.amount
  );

  els.billTotal.textContent = formatCurrency(total);
  els.billGrossTotal.textContent = hasGross
    ? formatCurrency(totalGross)
    : "-";
  els.billDiscountTotal.textContent = hasGross
    ? formatCurrency(totalDiscount)
    : "-";
  els.billAi.textContent = formatCurrency(ai);
  els.billNonAi.textContent = formatCurrency(nonAi);
  els.billRows.textContent = formatNumber(data.length);
  renderBillTable(productRows);
  setStatus(els.billStatus, `已加载 ${data.length} 条记录`);
}

const VENDOR_NAME_MAP = {
  aliyun: "阿里云",
  aws: "AWS",
  volcengine: "火山引擎",
  stepfun: "阶跃星辰",
};

function toVendorName(vendorCode) {
  const key = (vendorCode || "").toLowerCase();
  return VENDOR_NAME_MAP[key] || vendorCode || "-";
}

async function loadTokenData(client, startDate, endDate) {
  setStatus(els.tokenStatus, "加载中...");

  const { data, error } = await client
    .schema("financial_hub_prod")
    .from("llm_token_daily_usage")
    .select("day,vendor,total_tokens")
    .gte("day", startDate)
    .lte("day", endDate);

  if (error) {
    setStatus(els.tokenStatus, "加载失败");
    throw error;
  }

  const vendorMap = new Map();
  let totalTokens = 0;
  data.forEach((row) => {
    const tokens = Number(row.total_tokens || 0);
    totalTokens += tokens;
    if (!vendorMap.has(row.vendor)) {
      vendorMap.set(row.vendor, 0);
    }
    vendorMap.set(row.vendor, vendorMap.get(row.vendor) + tokens);
  });

  const vendorRows = Array.from(vendorMap.entries())
    .map(([vendor, total_tokens]) => ({
      vendor: toVendorName(vendor),
      total_tokens,
    }))
    .sort((a, b) => b.total_tokens - a.total_tokens);

  els.tokenTotal.textContent = formatNumber(totalTokens);
  els.tokenVendorCount.textContent = formatNumber(vendorRows.length);
  renderTokenTable(vendorRows);
  setStatus(els.tokenStatus, `已加载 ${data.length} 条记录`);
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

  if (error) {
    setStatus(els.aliyunTokenStatus, "加载失败");
    throw error;
  }

  const total = data.reduce(
    (sum, row) => sum + Number(row.total_tokens || 0),
    0
  );

  els.aliyunTokenTotal.textContent = formatNumber(total);
  els.aliyunTokenRows.textContent = formatNumber(data.length);
  renderAliyunTokenTable(data);
  setStatus(els.aliyunTokenStatus, `已加载 ${data.length} 条记录`);
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

  if (error) {
    setStatus(els.awsBillStatus, "加载失败");
    setStatus(els.awsOverviewStatus, "加载失败");
    throw error;
  }

  let total = 0;
  let totalGross = 0;
  let currency = null;
  const rows = data || [];
  rows.forEach((row) => {
    total += Number(row.amount || 0);
    totalGross += Number(row.gross_amount || 0);
    if (!currency && row.currency) {
      currency = row.currency;
    }
  });

  els.awsBillTotal.textContent = formatCurrency(total, currency || "USD");
  els.awsBillGrossTotal.textContent = formatCurrency(
    totalGross,
    currency || "USD"
  );
  els.awsBillRows.textContent = formatNumber(rows.length);
  renderDailyBillTable(rows, els.awsBillTable);
  setStatus(els.awsBillStatus, `已加载 ${rows.length} 条记录`);

  els.awsOverviewTotal.textContent = formatCurrency(total, currency || "USD");
  els.awsOverviewGrossTotal.textContent = formatCurrency(
    totalGross,
    currency || "USD"
  );
  els.awsOverviewRows.textContent = formatNumber(rows.length);
  setStatus(els.awsOverviewStatus, `已加载 ${rows.length} 条记录`);
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

  if (error) {
    setStatus(els.volcBillStatus, "加载失败");
    setStatus(els.volcOverviewStatus, "加载失败");
    throw error;
  }

  let total = 0;
  let totalGross = 0;
  let currency = null;
  const rows = data || [];
  rows.forEach((row) => {
    total += Number(row.amount || 0);
    totalGross += Number(row.gross_amount || 0);
    if (!currency && row.currency) {
      currency = row.currency;
    }
  });

  els.volcBillTotal.textContent = formatCurrency(total, currency || "CNY");
  els.volcBillGrossTotal.textContent = formatCurrency(
    totalGross,
    currency || "CNY"
  );
  els.volcBillRows.textContent = formatNumber(rows.length);
  renderDailyBillTable(rows, els.volcBillTable);
  setStatus(els.volcBillStatus, `已加载 ${rows.length} 条记录`);

  els.volcOverviewTotal.textContent = formatCurrency(
    total,
    currency || "CNY"
  );
  els.volcOverviewGrossTotal.textContent = formatCurrency(
    totalGross,
    currency || "CNY"
  );
  els.volcOverviewRows.textContent = formatNumber(rows.length);
  setStatus(els.volcOverviewStatus, `已加载 ${rows.length} 条记录`);
}

async function loadTianyanchaBillDaily(client, startDate, endDate) {
  setStatus(els.tianBillStatus, "加载中...");
  setStatus(els.tianOverviewStatus, "加载中...");

  const { data, error } = await client
    .schema("financial_hub_prod")
    .from("bill_daily_summary")
    .select("billing_date,is_ai_cost,amount,gross_amount,currency")
    .eq("vendor_code", "tianyancha")
    .eq("is_ai_cost", false)
    .gte("billing_date", startDate)
    .lte("billing_date", endDate)
    .order("billing_date", { ascending: false });

  if (error) {
    setStatus(els.tianBillStatus, "加载失败");
    setStatus(els.tianOverviewStatus, "加载失败");
    throw error;
  }

  let total = 0;
  let totalGross = 0;
  let currency = null;
  const rows = (data || []).map((row) => ({
    ...row,
    is_ai_cost: false,
  }));

  rows.forEach((row) => {
    total += Number(row.amount || 0);
    totalGross += Number(row.gross_amount || 0);
    if (!currency && row.currency) {
      currency = row.currency;
    }
  });

  els.tianBillTotal.textContent = formatCurrency(total, currency || "CNY");
  els.tianBillGrossTotal.textContent = formatCurrency(
    totalGross,
    currency || "CNY"
  );
  els.tianBillRows.textContent = formatNumber(rows.length);
  renderDailyBillTable(rows, els.tianBillTable);
  setStatus(els.tianBillStatus, `已加载 ${rows.length} 条记录`);

  els.tianOverviewTotal.textContent = formatCurrency(
    total,
    currency || "CNY"
  );
  els.tianOverviewGrossTotal.textContent = formatCurrency(
    totalGross,
    currency || "CNY"
  );
  els.tianOverviewRows.textContent = formatNumber(rows.length);
  setStatus(els.tianOverviewStatus, `已加载 ${rows.length} 条记录`);
}

async function loadMoonshotBillDaily(client, startDate, endDate) {
  setStatus(els.moonshotOverviewStatus, "加载中...");

  const { data, error } = await client
    .schema("financial_hub_prod")
    .from("bill_daily_summary")
    .select("billing_date,is_ai_cost,amount,gross_amount,currency")
    .eq("vendor_code", "moonshot")
    .gte("billing_date", startDate)
    .lte("billing_date", endDate)
    .order("billing_date", { ascending: false });

  if (error) {
    setStatus(els.moonshotOverviewStatus, "加载失败");
    throw error;
  }

  let total = 0;
  let totalGross = 0;
  let currency = null;
  const rows = data || [];

  rows.forEach((row) => {
    total += Number(row.amount || 0);
    totalGross += Number(row.gross_amount || 0);
    if (!currency && row.currency) {
      currency = row.currency;
    }
  });

  els.moonshotOverviewTotal.textContent = formatCurrency(
    total,
    currency || "CNY"
  );
  els.moonshotOverviewGrossTotal.textContent = formatCurrency(
    totalGross,
    currency || "CNY"
  );
  els.moonshotOverviewRows.textContent = formatNumber(rows.length);
  setStatus(els.moonshotOverviewStatus, `已加载 ${rows.length} 条记录`);
}

async function loadAuthingBillMonthly(client, startDate, endDate) {
  setStatus(els.authingOverviewStatus, "加载中...");

  // 从日期提取月份范围
  const startMonth = startDate.slice(0, 7);
  const endMonth = endDate.slice(0, 7);

  const { data, error } = await client
    .schema("financial_hub_prod")
    .from("bill_monthly_summary")
    .select("month,amount,gross_amount,currency")
    .eq("vendor_code", "authing")
    .gte("month", startMonth)
    .lte("month", endMonth)
    .order("month", { ascending: false });

  if (error) {
    setStatus(els.authingOverviewStatus, "加载失败");
    throw error;
  }

  let total = 0;
  let currency = null;
  const rows = data || [];

  rows.forEach((row) => {
    total += Number(row.amount || 0);
    if (!currency && row.currency) {
      currency = row.currency;
    }
  });

  els.authingOverviewTotal.textContent = formatCurrency(total, currency || "CNY");
  els.authingOverviewRows.textContent = formatNumber(rows.length);
  setStatus(els.authingOverviewStatus, `已加载 ${rows.length} 条月记录`);
}

async function loadTextinBillDaily(client, startDate, endDate) {
  setStatus(els.textinOverviewStatus, "加载中...");

  const { data, error } = await client
    .schema("financial_hub_prod")
    .from("bill_daily_summary")
    .select("billing_date,is_ai_cost,amount,gross_amount,currency")
    .eq("vendor_code", "textin")
    .eq("is_ai_cost", false)
    .gte("billing_date", startDate)
    .lte("billing_date", endDate)
    .order("billing_date", { ascending: false });

  if (error) {
    setStatus(els.textinOverviewStatus, "加载失败");
    throw error;
  }

  let total = 0;
  let totalGross = 0;
  let currency = null;
  const rows = data || [];

  rows.forEach((row) => {
    total += Number(row.amount || 0);
    totalGross += Number(row.gross_amount || 0);
    if (!currency && row.currency) {
      currency = row.currency;
    }
  });

  els.textinOverviewTotal.textContent = formatCurrency(total, currency || "CNY");
  els.textinOverviewGrossTotal.textContent = formatCurrency(
    totalGross,
    currency || "CNY"
  );
  els.textinOverviewRows.textContent = formatNumber(rows.length);
  setStatus(els.textinOverviewStatus, `已加载 ${rows.length} 条记录`);
}

async function refresh() {
  try {
    setError("");
    const client = buildClient();
    const start = els.startDate.value;
    const end = els.endDate.value;

    await Promise.all([
      loadBillData(client, start, end),
      loadTokenData(client, start, end),
      loadAliyunTokenDaily(client, start, end),
      loadAwsBillDaily(client, start, end),
      loadVolcBillDaily(client, start, end),
      loadTianyanchaBillDaily(client, start, end),
      loadMoonshotBillDaily(client, start, end),
      loadAuthingBillMonthly(client, start, end),
      loadTextinBillDaily(client, start, end),
    ]);

    els.lastRefresh.textContent = new Date().toLocaleString("zh-CN");
  } catch (err) {
    setError(err.message || "数据加载失败");
  }
}

els.applyRange.addEventListener("click", () => {
  state.startDate = new Date(els.startDate.value);
  state.endDate = new Date(els.endDate.value);
  els.rangeHint.textContent = `当前范围：${els.startDate.value} ~ ${els.endDate.value}`;
  refresh();
});

els.refreshButton.addEventListener("click", refresh);

els.tabs.forEach((tab) => {
  tab.addEventListener("click", () => {
    els.tabs.forEach((btn) => btn.classList.remove("active"));
    tab.classList.add("active");
    const target = tab.dataset.tab;
    Object.entries(els.tabPanels).forEach(([key, panel]) => {
      panel.classList.toggle("active", key === target);
    });
  });
});

initDateRange();
refresh();
