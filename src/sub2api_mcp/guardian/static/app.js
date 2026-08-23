const $ = (selector, root = document) => root.querySelector(selector);
const $$ = (selector, root = document) => [...root.querySelectorAll(selector)];

let apiKey = "";
let currentPage = "overview";
let policyState = null;
let policyDefaults = null;
let eventsCursor = null;
let eventItems = [];
let groupsState = [];

const pageMeta = {
  overview: ["总览", "Guardian 全局运行状态与健康快照"],
  groups: ["分组调度", "分组健康、可用池与独立策略覆盖"],
  channels: ["渠道池", "渠道评分、状态和人工控制"],
  routing: ["实时路由", "当前状态与候选调度结果对比"],
  spend: ["探测费用", "主动探测 token 与成本估算"],
  guide: ["调度说明", "评分、熔断、保底、降级和回池规则"],
  events: ["事件日志", "运行、状态迁移和人工操作审计"],
  policy: ["策略配置", "全局规则、系统参数与守护范围"],
  connection: ["连接设置", "上游、API、存储和写回适配器状态"],
  info: ["信息与通知", "版本信息和 LangBot 全渠道通知说明"],
};

const policyFields = [
  ["#p-enabled", "enabled", "boolean"],
  ["#p-observe", "observe_only", "boolean"],
  ["#p-scan", "scan_interval_seconds", "number"],
  ["#p-strategy", "strategy", "string"],
  ["#p-probe-enabled", "probe.enabled", "boolean"],
  ["#p-probe-skip", "probe.skip_when_traffic_fresh", "boolean"],
  ["#p-probe-interval", "probe.interval_seconds", "number"],
  ["#p-probe-timeout", "probe.timeout_seconds", "number"],
  ["#p-probe-concurrency", "probe.concurrency", "number"],
  ["#p-probe-model", "probe.model", "string"],
  ["#p-short-window", "scoring.short_window", "number"],
  ["#p-long-window", "scoring.long_window", "number"],
  ["#p-slow-ttfb", "scoring.slow_ttfb_ms", "number"],
  ["#p-latest-weight", "scoring.latest_weight", "number"],
  ["#p-short-ratio", "scoring.short_ratio", "number"],
  ["#p-decay", "scoring.decay", "number"],
  ["#score-perfect", "scoring.event_scores.PERFECT", "number"],
  ["#score-slow", "scoring.event_scores.SLOW_TTFB", "number"],
  ["#score-unknown", "scoring.event_scores.UPSTREAM_UNKNOWN", "number"],
  ["#score-gateway", "scoring.event_scores.GATEWAY_ERROR", "number"],
  ["#score-quota", "scoring.event_scores.QUOTA_EXHAUSTED", "number"],
  ["#score-probe", "scoring.event_scores.PROBE_FAIL", "number"],
  ["#score-fatal", "scoring.event_scores.FATAL", "number"],
  ["#p-breaker-enabled", "breaker.enabled", "boolean"],
  ["#p-http-window", "breaker.http_window", "number"],
  ["#p-http-failures", "breaker.http_failures", "number"],
  ["#p-http-score", "breaker.http_score_below", "number"],
  ["#p-max-fuse", "breaker.max_switch_per_round", "number"],
  ["#p-fuse-cooldown", "breaker.fused_cooldown_seconds", "number"],
  ["#p-min-pool", "breaker.min_pool_size", "number"],
  ["#p-min-score", "breaker.min_pool_score", "number"],
  ["#p-latency-threshold", "breaker.latency_ttfb_ms", "number"],
  ["#p-degrade-score", "degrade.score_threshold", "number"],
  ["#p-degrade-ratio", "degrade.load_factor_ratio", "number"],
  ["#p-priority-step", "degrade.priority_step", "number"],
  ["#p-recovery-score", "recovery.target_score", "number"],
  ["#p-recovery-count", "recovery.success_count", "number"],
  ["#p-recovery-hold", "recovery.hold_seconds", "number"],
  ["#p-weight-budget", "weights.budget", "number"],
  ["#p-weight-gate", "weights.gate_floor", "number"],
  ["#p-balance-ratio", "weights.balanced_price_ratio", "number"],
  ["#p-change-threshold", "weights.change_threshold", "number"],
  ["#p-weight-cooldown", "weights.cooldown_seconds", "number"],
  ["#p-max-load", "weights.max_load_factor", "number"],
  ["#p-group-mode", "scope.managed_group_mode", "string"],
  ["#p-managed-groups", "scope.managed_group_ids", "set"],
  ["#p-excluded-groups", "scope.excluded_group_ids", "set"],
  ["#p-account-types", "scope.managed_account_types", "set"],
  ["#p-platforms", "scope.managed_platforms", "set"],
  ["#p-paused-channels", "scope.paused_channel_ids", "set"],
  ["#p-excluded-channels", "scope.excluded_channel_ids", "set"],
];

function make(tag, className = "", text = "") {
  const element = document.createElement(tag);
  if (className) element.className = className;
  if (text !== "") element.textContent = String(text);
  return element;
}

function cell(row, value, className = "") {
  const td = make("td", className);
  if (value instanceof Node) td.append(value);
  else td.textContent = value == null ? "—" : String(value);
  row.append(td);
  return td;
}

function statusBadge(value) {
  const labels = {
    HEALTHY: "健康",
    DEGRADED: "降级",
    RATE_LIMITED: "限流",
    FUSED: "熔断",
    FORCED_KEEP: "强制保底",
    MANUALLY_PAUSED: "人工暂停",
    EXCLUDED: "已排除",
    PENDING: "待评分",
    NONE: "无",
    PAUSED: "暂停",
  };
  const className = ["FUSED", "EXCLUDED"].includes(value)
    ? "danger"
    : ["DEGRADED", "RATE_LIMITED", "FORCED_KEEP", "MANUALLY_PAUSED", "PAUSED"].includes(value)
      ? "warning"
      : value === "HEALTHY"
        ? "success"
        : "neutral";
  return make("span", `badge ${className}`, labels[value] || value || "—");
}

function booleanBadge(value) {
  return make("span", `badge ${value ? "success" : "neutral"}`, value ? "是" : "否");
}

function formatDate(value) {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(date);
}

function formatNumber(value, digits = 1) {
  const number = Number(value);
  return Number.isFinite(number) ? number.toFixed(digits) : "—";
}

function getPath(object, path) {
  return path.split(".").reduce((current, key) => current?.[key], object);
}

function setPath(object, path, value) {
  const keys = path.split(".");
  let current = object;
  for (const key of keys.slice(0, -1)) {
    if (!current[key]) current[key] = {};
    current = current[key];
  }
  current[keys.at(-1)] = value;
}

function parseSet(value) {
  return [...new Set(value.split(/[，,\n]/).map((item) => item.trim()).filter(Boolean))];
}

function toast(message, error = false) {
  const item = make("div", `toast${error ? " error" : ""}`, message);
  $("#toast-region").append(item);
  window.setTimeout(() => item.remove(), 4200);
}

async function api(path, options = {}) {
  const headers = new Headers(options.headers || {});
  headers.set("X-API-Key", apiKey);
  if (options.body !== undefined) headers.set("Content-Type", "application/json");
  const response = await fetch(`/api/guardian/v1${path}`, {
    ...options,
    headers,
    body: options.body === undefined ? undefined : JSON.stringify(options.body),
  });
  let payload;
  try {
    payload = await response.json();
  } catch {
    throw new Error(`服务返回了无法解析的响应（HTTP ${response.status}）`);
  }
  if (!response.ok || !payload.ok) {
    const error = new Error(payload.error?.message || `请求失败（HTTP ${response.status}）`);
    error.code = payload.error?.code || "HTTP_ERROR";
    error.status = response.status;
    if (response.status === 401) showLogin("连接已失效，请重新输入 API Key");
    throw error;
  }
  return payload.data;
}

function showLogin(message = "") {
  apiKey = "";
  $("#app-shell").hidden = true;
  $("#login-screen").hidden = false;
  $("#login-message").textContent = message;
  $("#api-key").value = "";
  $("#api-key").focus();
}

function showApp() {
  $("#login-screen").hidden = true;
  $("#app-shell").hidden = false;
}

function openSidebar() {
  $("#sidebar").classList.add("open");
  $("#sidebar-scrim").hidden = false;
}

function closeSidebar() {
  $("#sidebar").classList.remove("open");
  $("#sidebar-scrim").hidden = true;
}

async function navigate(page) {
  if (!pageMeta[page]) return;
  currentPage = page;
  $$(".nav-item").forEach((item) => item.classList.toggle("active", item.dataset.page === page));
  $$(".page").forEach((item) => item.classList.toggle("active", item.id === `page-${page}`));
  $("#page-title").textContent = pageMeta[page][0];
  $("#page-subtitle").textContent = pageMeta[page][1];
  closeSidebar();
  $("#main-content").focus({ preventScroll: true });
  try {
    await refreshPage(page);
  } catch (error) {
    toast(error.message, true);
  }
}

async function refreshPage(page = currentPage) {
  const loaders = {
    overview: loadOverview,
    groups: loadGroups,
    channels: loadChannels,
    routing: loadRouting,
    spend: loadSpend,
    events: () => loadEvents(true),
    policy: loadPolicy,
    connection: loadConnection,
    info: loadStatus,
  };
  if (loaders[page]) await loaders[page]();
}

async function loadStatus() {
  const status = await api("/status");
  const enabledLabel = status.enabled ? "后台运行中" : "后台已停用";
  $("#metric-engine").textContent = enabledLabel;
  $("#sidebar-status").textContent = status.observe_only ? "观察模式" : "执行模式";
  $("#mode-label").textContent = status.observe_only ? "观察模式" : "执行模式";
  $("#info-mode").textContent = status.observe_only ? "观察模式" : "执行模式";
  $("#sidebar-dot").style.background = status.enabled ? "var(--green)" : "var(--amber)";
  return status;
}

async function loadOverview() {
  const [overview, status, events] = await Promise.all([
    api("/overview"),
    api("/status"),
    api("/events?limit=6"),
  ]);
  const counts = overview.health_counts || {};
  const healthy = counts.HEALTHY || 0;
  const risk = Object.entries(counts)
    .filter(([name]) => !["HEALTHY", "PENDING"].includes(name))
    .reduce((total, [, count]) => total + Number(count), 0);
  $("#metric-mode").textContent = overview.observe_only ? "观察" : "执行";
  $("#metric-channels").textContent = overview.channel_count;
  $("#metric-groups").textContent = `${overview.group_count} 个分组`;
  $("#metric-healthy").textContent = healthy;
  $("#metric-risk").textContent = risk;
  $("#metric-revision").textContent = overview.policy_revision;
  await loadStatus();
  renderDistribution(counts, overview.channel_count);
  renderLastRun(status.last_run);
  renderEvents($("#overview-events"), events.items || []);
}

function renderDistribution(counts, total) {
  const root = $("#health-distribution");
  root.replaceChildren();
  const order = ["HEALTHY", "DEGRADED", "FUSED", "FORCED_KEEP", "MANUALLY_PAUSED", "EXCLUDED", "PENDING"];
  const rows = order.filter((name) => counts[name]);
  if (!rows.length) {
    root.append(make("p", "empty", "同步后显示健康分布"));
    return;
  }
  for (const name of rows) {
    const row = make("div", "distribution-row");
    row.append(statusBadge(name));
    const track = make("div", "distribution-track");
    const riskClass = ["DEGRADED", "FORCED_KEEP", "MANUALLY_PAUSED"].includes(name)
      ? " risk"
      : ["FUSED", "EXCLUDED"].includes(name)
        ? " fused"
        : "";
    const fill = make("div", `distribution-fill${riskClass}`);
    fill.style.width = `${Math.max(3, (Number(counts[name]) / Math.max(1, total)) * 100)}%`;
    track.append(fill);
    row.append(track, make("strong", "", counts[name]));
    root.append(row);
  }
}

function renderLastRun(run) {
  const root = $("#last-run-details");
  root.replaceChildren();
  const values = run
    ? [
        ["状态", run.status],
        ["开始时间", formatDate(run.started_at)],
        ["评估渠道", run.result?.channels_evaluated ?? "—"],
        ["状态转换", run.result?.state_transitions ?? "—"],
        ["预期差异", run.result?.expected_changes ?? "—"],
        ["实际写入", run.result?.writes_applied ?? 0],
      ]
    : [["状态", "尚未运行"]];
  for (const [label, value] of values) {
    const wrapper = make("div");
    wrapper.append(make("dt", "", label), make("dd", "", value));
    root.append(wrapper);
  }
}

function renderEvents(root, items, append = false) {
  if (!append) root.replaceChildren();
  if (!items.length && !append) {
    root.append(make("p", "empty", "暂无事件"));
    return;
  }
  for (const event of items) {
    const item = make("div", "event-item");
    item.append(
      make("span", `event-marker ${String(event.severity || "").toLowerCase()}`),
      make("span", "event-type", event.event_type),
      make("span", "event-message", event.message),
      make("time", "event-time", formatDate(event.created_at)),
    );
    root.append(item);
  }
}

async function loadGroups() {
  const data = await api("/groups");
  groupsState = data.items || [];
  const body = $("#groups-table");
  body.replaceChildren();
  $("#groups-empty").hidden = groupsState.length > 0;
  for (const group of groupsState) {
    const row = make("tr");
    const title = make("div");
    title.append(make("span", "cell-title", group.name), make("span", "cell-subtitle", `ID ${group.group_id}`));
    cell(row, title);
    cell(row, group.channel_count);
    cell(row, group.available_count);
    cell(row, make("span", "score-value", formatNumber(group.score)));
    cell(row, group.latency_ms == null ? "—" : `${formatNumber(group.latency_ms, 0)} ms`);
    cell(row, make("span", `badge ${group.override ? "neutral" : "success"}`, group.override ? "独立覆盖" : "继承全局"));
    const actions = make("div", "table-actions");
    const settings = make("button", "table-action", "设置");
    settings.type = "button";
    settings.addEventListener("click", () => openGroupDialog(group));
    actions.append(settings);
    cell(row, actions);
    body.append(row);
  }
  updateGroupFilter();
}

function updateGroupFilter() {
  const select = $("#channel-group");
  const current = select.value;
  select.replaceChildren(new Option("全部分组", ""));
  for (const group of groupsState) select.append(new Option(group.name, group.group_id));
  select.value = current;
}

function openGroupDialog(group) {
  const policy = group.override?.policy || {};
  $("#group-id").value = group.group_id;
  $("#group-dialog-title").textContent = `${group.name} · 策略覆盖`;
  $("#group-strategy").value = policy.strategy || "";
  $("#group-min-pool").value = policy.min_pool_size ?? "";
  $("#group-budget").value = policy.weight_budget ?? "";
  $("#group-probe-interval").value = policy.probe_interval_seconds ?? "";
  $("#group-clear").hidden = !group.override;
  $("#group-dialog").showModal();
}

async function saveGroupPolicy() {
  const groupId = $("#group-id").value;
  const patch = {};
  const strategy = $("#group-strategy").value;
  const minPool = $("#group-min-pool").value;
  const budget = $("#group-budget").value;
  const interval = $("#group-probe-interval").value;
  if (strategy) patch.strategy = strategy;
  if (minPool !== "") patch.min_pool_size = Number(minPool);
  if (budget !== "") patch.weight_budget = Number(budget);
  if (interval !== "") patch.probe_interval_seconds = Number(interval);
  await api(`/groups/${encodeURIComponent(groupId)}/policy`, {
    method: "PATCH",
    headers: { "Idempotency-Key": `ui:group:${groupId}:${Date.now()}` },
    body: patch,
  });
  $("#group-dialog").close();
  toast("分组策略已保存");
  await loadGroups();
}

async function clearGroupPolicy() {
  const groupId = $("#group-id").value;
  await api(`/groups/${encodeURIComponent(groupId)}/policy`, {
    method: "DELETE",
    headers: { "Idempotency-Key": `ui:group-clear:${groupId}:${Date.now()}` },
  });
  $("#group-dialog").close();
  toast("分组已恢复继承全局策略");
  await loadGroups();
}

async function loadChannels() {
  if (!groupsState.length) {
    const groups = await api("/groups");
    groupsState = groups.items || [];
    updateGroupFilter();
  }
  const params = new URLSearchParams({ limit: "200" });
  const query = $("#channel-query").value.trim();
  const group = $("#channel-group").value;
  const health = $("#channel-health").value;
  if (query) params.set("query", query);
  if (group) params.set("group_id", group);
  if (health) params.set("health", health);
  const data = await api(`/channels?${params}`);
  const items = data.items || [];
  const body = $("#channels-table");
  body.replaceChildren();
  $("#channels-empty").hidden = items.length > 0;
  for (const channel of items) body.append(channelRow(channel));
}

function channelRow(channel) {
  const row = make("tr");
  const title = make("button", "text-button");
  title.type = "button";
  title.textContent = channel.name;
  title.addEventListener("click", () => showChannel(channel.channel_id));
  const titleWrap = make("div");
  titleWrap.append(title, make("span", "cell-subtitle", `ID ${channel.channel_id}`));
  cell(row, titleWrap);
  cell(row, channel.group_id || "未分组");
  cell(row, make("span", "score-value", formatNumber(channel.score)));
  cell(row, channel.latency_ms == null ? "—" : `${channel.latency_ms} ms`);
  cell(row, booleanBadge(channel.upstream_schedulable));
  cell(row, statusBadge(channel.health));
  cell(row, make("span", `badge ${channel.details?.expected_action === "NO_CHANGE" ? "success" : "warning"}`, channel.details?.expected_action || "—"));
  cell(row, statusBadge(channel.manual_control));
  const actions = make("div", "table-actions");
  actions.append(actionButton("探测", channel, "probe"));
  const pauseAction = channel.manual_control === "PAUSED" ? "resume" : "pause";
  actions.append(actionButton(pauseAction === "pause" ? "暂停" : "恢复", channel, pauseAction, pauseAction === "pause"));
  const excludeAction = channel.manual_control === "EXCLUDED" ? "include" : "exclude";
  actions.append(actionButton(excludeAction === "exclude" ? "排除" : "纳入", channel, excludeAction, excludeAction === "exclude"));
  cell(row, actions);
  return row;
}

function actionButton(label, channel, action, dangerous = false) {
  const button = make("button", `table-action${dangerous ? " danger" : ""}`, label);
  button.type = "button";
  button.addEventListener("click", async () => {
    if (dangerous && !window.confirm(`确定要对渠道“${channel.name}”执行“${label}”吗？`)) return;
    try {
      await channelAction(channel.channel_id, action);
    } catch (error) {
      toast(error.message, true);
    }
  });
  return button;
}

async function channelAction(channelId, action) {
  await api(`/channels/${encodeURIComponent(channelId)}/actions`, {
    method: "POST",
    headers: { "Idempotency-Key": `ui:${action}:${channelId}:${Date.now()}` },
    body: { action },
  });
  toast(`渠道操作已提交：${action}`);
  await refreshPage(currentPage);
}

async function showChannel(channelId) {
  const channel = await api(`/channels/${encodeURIComponent(channelId)}`);
  $("#channel-dialog-title").textContent = channel.name;
  const root = $("#channel-detail");
  root.replaceChildren();
  const summary = make("div", "channel-summary");
  for (const [label, value] of [
    ["渠道 ID", channel.channel_id],
    ["分组", channel.group_id || "未分组"],
    ["健康分", formatNumber(channel.score)],
    ["状态", channel.health],
  ]) {
    const item = make("div");
    item.append(make("span", "", label), make("strong", "", value));
    summary.append(item);
  }
  root.append(summary, make("h3", "", "最近评分样本"));
  const samples = make("div", "sample-list");
  if (!channel.samples?.length) samples.append(make("p", "empty", "暂无评分样本"));
  for (const sample of channel.samples || []) {
    const row = make("div", "sample-row");
    row.append(
      make("span", "", sample.event_type),
      make("span", "", sample.source),
      make("strong", "", sample.score),
      make("time", "", formatDate(sample.occurred_at)),
    );
    samples.append(row);
  }
  root.append(samples);
  const override = channel.override || {};
  $("#channel-settings-id").value = channel.channel_id;
  $("#channel-priority").value = override.priority ?? "";
  $("#channel-load-factor").value = override.load_factor ?? "";
  $("#channel-concurrency").value = override.concurrency ?? "";
  $("#channel-multiplier").value = override.schedule_multiplier ?? "";
  $("#channel-probe-model").value = override.probe_model ?? "";
  $("#channel-unboost").hidden = !override.boost_until;
  const dialog = $("#channel-dialog");
  if (!dialog.open) dialog.showModal();
}

async function saveChannelSettings(event) {
  event.preventDefault();
  const channelId = $("#channel-settings-id").value;
  const nullableNumber = (selector) => {
    const value = $(selector).value;
    return value === "" ? null : Number(value);
  };
  await api(`/channels/${encodeURIComponent(channelId)}`, {
    method: "PATCH",
    headers: { "Idempotency-Key": `ui:channel:${channelId}:${Date.now()}` },
    body: {
      priority: nullableNumber("#channel-priority"),
      load_factor: nullableNumber("#channel-load-factor"),
      concurrency: nullableNumber("#channel-concurrency"),
      schedule_multiplier: nullableNumber("#channel-multiplier"),
      probe_model: $("#channel-probe-model").value.trim() || null,
    },
  });
  toast("渠道覆盖参数已保存");
  await showChannel(channelId);
  if (currentPage === "channels") await loadChannels();
}

async function boostChannel(action) {
  const channelId = $("#channel-settings-id").value;
  const minutes = Number($("#channel-boost-minutes").value);
  await api(`/channels/${encodeURIComponent(channelId)}/actions`, {
    method: "POST",
    headers: { "Idempotency-Key": `ui:${action}:${channelId}:${Date.now()}` },
    body: action === "boost" ? { action, minutes } : { action },
  });
  toast(action === "boost" ? `火箭已启动 ${minutes} 分钟` : "火箭已取消");
  await showChannel(channelId);
  if (currentPage === "channels") await loadChannels();
}

async function loadRouting() {
  const data = await api("/live-routing");
  const items = data.items || [];
  const body = $("#routing-table");
  body.replaceChildren();
  $("#routing-empty").hidden = items.length > 0;
  for (const item of items) {
    const row = make("tr");
    cell(row, item.name);
    cell(row, item.group_id || "未分组");
    cell(row, make("span", "score-value", formatNumber(item.score)));
    cell(row, booleanBadge(item.upstream_schedulable));
    cell(row, booleanBadge(item.desired_schedulable));
    cell(row, item.candidate_weight == null ? "—" : formatNumber(item.candidate_weight, 2));
    cell(row, make("span", `badge ${item.expected_action === "NO_CHANGE" ? "success" : "warning"}`, item.expected_action || "—"));
    body.append(row);
  }
}

async function loadSpend() {
  const data = await api("/probe-spend");
  $("#spend-count").textContent = data.probe_count;
  $("#spend-cost").textContent = `$${Number(data.estimated_cost || 0).toFixed(4)}`;
  $("#spend-unpriced").textContent = data.unpriced_count;
  $("#spend-currency").textContent = data.currency;
}

async function loadEvents(reset = false) {
  if (reset) {
    eventsCursor = null;
    eventItems = [];
  }
  const params = new URLSearchParams({ limit: "50" });
  const severity = $("#event-severity").value;
  const type = $("#event-type").value.trim();
  if (severity) params.set("severity", severity);
  if (type) params.set("event_type", type);
  if (eventsCursor) params.set("cursor", eventsCursor);
  const data = await api(`/events?${params}`);
  eventItems.push(...(data.items || []));
  eventsCursor = data.next_cursor;
  renderEvents($("#events-list"), eventItems);
  $("#events-more").hidden = !eventsCursor;
}

function fillPolicy(policy) {
  for (const [selector, path, type] of policyFields) {
    const input = $(selector);
    const value = getPath(policy, path);
    if (type === "boolean") input.checked = Boolean(value);
    else if (type === "set") input.value = Array.isArray(value) ? value.join(", ") : "";
    else input.value = value ?? "";
  }
  $("#policy-revision").textContent = policy.revision;
  $("#policy-message").textContent = "尚无未保存修改";
}

function collectPolicy() {
  const patch = {};
  for (const [selector, path, type] of policyFields) {
    if (path === "observe_only") continue;
    const input = $(selector);
    let value;
    if (type === "boolean") value = input.checked;
    else if (type === "number") value = Number(input.value);
    else if (type === "set") value = parseSet(input.value);
    else value = input.value;
    setPath(patch, path, value);
  }
  return patch;
}

async function loadPolicy() {
  const data = await api("/policy");
  policyState = data.policy;
  policyDefaults = data.defaults;
  fillPolicy(policyState);
}

async function savePolicy(event) {
  event.preventDefault();
  if (!policyState) return;
  const button = $("#policy-form button[type='submit']");
  button.disabled = true;
  try {
    const data = await api("/policy", {
      method: "PATCH",
      headers: {
        "If-Match": String(policyState.revision),
        "Idempotency-Key": `ui:policy:${policyState.revision}:${Date.now()}`,
      },
      body: collectPolicy(),
    });
    policyState = data.policy;
    fillPolicy(policyState);
    toast(`策略 revision ${policyState.revision} 已保存`);
    await loadStatus();
  } catch (error) {
    if (error.code === "POLICY_REVISION_CONFLICT") {
      $("#policy-message").textContent = "策略已被其他会话修改，请刷新后重试";
    }
    toast(error.message, true);
  } finally {
    button.disabled = false;
  }
}

async function loadConnection() {
  const status = await loadStatus();
  $("#connection-writer").textContent = status.writeback_adapter === "disabled" ? "未启用" : "已启用";
}

async function runCycle(source) {
  const button = source === "sync" ? $("#sync-button") : $("#run-button");
  button.disabled = true;
  const original = button.textContent;
  button.textContent = source === "sync" ? "同步中…" : "评估中…";
  try {
    const endpoint = source === "sync" ? "/syncs" : "/runs";
    const data = await api(endpoint, {
      method: "POST",
      headers: { "Idempotency-Key": `ui:${source}:${Date.now()}` },
      body: source === "sync" ? {} : { dry_run: true },
    });
    const count = data.result?.channels_evaluated ?? 0;
    toast(`${source === "sync" ? "同步" : "评估"}完成，共处理 ${count} 个渠道`);
    await refreshPage(currentPage);
  } catch (error) {
    toast(error.message, true);
  } finally {
    button.disabled = false;
    button.textContent = original;
  }
}

$("#login-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  apiKey = $("#api-key").value;
  $("#login-message").textContent = "正在验证连接…";
  try {
    await api("/overview");
    $("#api-key").value = "";
    $("#login-message").textContent = "";
    showApp();
    await navigate("overview");
  } catch (error) {
    $("#login-message").textContent = error.message;
    apiKey = "";
  }
});

$("#toggle-key").addEventListener("click", () => {
  const input = $("#api-key");
  const show = input.type === "password";
  input.type = show ? "text" : "password";
  $("#toggle-key").textContent = show ? "隐藏" : "显示";
});

$$(".nav-item").forEach((item) => item.addEventListener("click", () => navigate(item.dataset.page)));
$$('[data-page-link]').forEach((item) => item.addEventListener("click", () => navigate(item.dataset.pageLink)));
$("#open-sidebar").addEventListener("click", openSidebar);
$("#close-sidebar").addEventListener("click", closeSidebar);
$("#sidebar-scrim").addEventListener("click", closeSidebar);
$("#logout-button").addEventListener("click", () => showLogin("已断开当前连接"));
$("#sync-button").addEventListener("click", () => runCycle("sync"));
$("#run-button").addEventListener("click", () => runCycle("run"));
$("#channel-filter").addEventListener("click", () => loadChannels().catch((error) => toast(error.message, true)));
$("#channel-query").addEventListener("keydown", (event) => {
  if (event.key === "Enter") loadChannels().catch((error) => toast(error.message, true));
});
$("#event-filter").addEventListener("click", () => loadEvents(true).catch((error) => toast(error.message, true)));
$("#events-more").addEventListener("click", () => loadEvents(false).catch((error) => toast(error.message, true)));
$("#group-save").addEventListener("click", () => saveGroupPolicy().catch((error) => toast(error.message, true)));
$("#group-clear").addEventListener("click", () => clearGroupPolicy().catch((error) => toast(error.message, true)));
$("#close-channel-dialog").addEventListener("click", () => $("#channel-dialog").close());
$("#channel-settings").addEventListener("submit", (event) => {
  saveChannelSettings(event).catch((error) => toast(error.message, true));
});
$("#channel-boost").addEventListener("click", () => {
  boostChannel("boost").catch((error) => toast(error.message, true));
});
$("#channel-unboost").addEventListener("click", () => {
  boostChannel("unboost").catch((error) => toast(error.message, true));
});
$("#policy-form").addEventListener("submit", savePolicy);
$("#policy-form").addEventListener("input", () => {
  $("#policy-message").textContent = "有尚未保存的修改";
});
$("#policy-defaults").addEventListener("click", () => {
  if (policyDefaults) {
    fillPolicy({ ...policyDefaults, revision: policyState?.revision || 1 });
    $("#policy-message").textContent = "已载入默认值，点击保存后生效";
  }
});

$$('[data-policy-tab]').forEach((tab) => {
  tab.addEventListener("click", () => {
    const selected = tab.dataset.policyTab;
    $$('[data-policy-tab]').forEach((item) => {
      const active = item === tab;
      item.classList.toggle("active", active);
      item.setAttribute("aria-selected", String(active));
    });
    $$(".policy-pane").forEach((pane) => {
      const active = pane.id === `policy-${selected}`;
      pane.classList.toggle("active", active);
      pane.hidden = !active;
    });
  });
});
