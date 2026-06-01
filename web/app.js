const state = {
  symbols: [],
  models: {},
  pollMs: 2000,
};

const els = {
  statusText: document.getElementById("statusText"),
  abortBtn: document.getElementById("abortBtn"),
  generateSelectedBtn: document.getElementById("generateSelectedBtn"),
  generateAllBtn: document.getElementById("generateAllBtn"),
  singleTicker: document.getElementById("singleTicker"),
  analysisDate: document.getElementById("analysisDate"),
  modelPicker: document.getElementById("modelPicker"),
  symbolList: document.getElementById("symbolList"),
  symbolListBox: document.getElementById("symbolListBox"),
  symbolFilter: document.getElementById("symbolFilter"),
  selectVisibleBtn: document.getElementById("selectVisibleBtn"),
  clearSelectionBtn: document.getElementById("clearSelectionBtn"),
  doneCount: document.getElementById("doneCount"),
  leftCount: document.getElementById("leftCount"),
  jobMessage: document.getElementById("jobMessage"),
  jobPercent: document.getElementById("jobPercent"),
  progressFill: document.getElementById("progressFill"),
  currentWork: document.getElementById("currentWork"),
  schedulesList: document.getElementById("schedulesList"),
  reportsBody: document.getElementById("reportsBody"),
  reportCount: document.getElementById("reportCount"),
  sortReports: document.getElementById("sortReports"),
};

async function init() {
  const cfg = await fetchJSON("/api/config");
  state.symbols = cfg.symbols || [];
  state.models = cfg.models || {};
  state.pollMs = Math.max(1, cfg.ui?.poll_seconds || 2) * 1000;
  renderModels();
  renderSymbols();
  wireEvents();
  await refreshAll();
  setInterval(refreshAll, state.pollMs);
}

function wireEvents() {
  els.symbolFilter.addEventListener("input", filterSymbols);
  els.selectVisibleBtn.addEventListener("click", selectVisibleSymbols);
  els.clearSelectionBtn.addEventListener("click", clearSymbols);
  els.generateSelectedBtn.addEventListener("click", () => generate(false));
  els.generateAllBtn.addEventListener("click", () => generate(true));
  els.abortBtn.addEventListener("click", abortJob);
  els.sortReports.addEventListener("change", refreshReports);
}

function renderModels() {
  els.modelPicker.innerHTML = "";
  Object.entries(state.models).forEach(([name, model], index) => {
    const label = document.createElement("label");
    label.className = "model-toggle";
    const input = document.createElement("input");
    input.type = "checkbox";
    input.value = name;
    input.checked = index === 0;
    label.append(input, model.label || name);
    els.modelPicker.append(label);
  });
}

function renderSymbols() {
  els.symbolList.innerHTML = "";
  els.symbolListBox.innerHTML = "";
  state.symbols.forEach((symbol) => {
    const option = document.createElement("option");
    option.value = symbol;
    els.symbolList.append(option);

    const label = document.createElement("label");
    label.className = "symbol-item";
    label.dataset.symbol = symbol;
    const input = document.createElement("input");
    input.type = "checkbox";
    input.value = symbol;
    label.append(input, symbol);
    els.symbolListBox.append(label);
  });
}

function filterSymbols() {
  const q = els.symbolFilter.value.trim().toUpperCase();
  document.querySelectorAll(".symbol-item").forEach((item) => {
    item.classList.toggle("hidden", q && !item.dataset.symbol.includes(q));
  });
}

function selectVisibleSymbols() {
  document.querySelectorAll(".symbol-item:not(.hidden) input").forEach((input) => {
    input.checked = true;
  });
}

function clearSymbols() {
  document.querySelectorAll(".symbol-item input").forEach((input) => {
    input.checked = false;
  });
}

function selectedModels() {
  return [...document.querySelectorAll(".model-toggle input:checked")].map((input) => input.value);
}

function selectedSymbols() {
  const single = els.singleTicker.value.trim().toUpperCase();
  if (single) return [single];
  return [...document.querySelectorAll(".symbol-item input:checked")].map((input) => input.value);
}

async function generate(all) {
  const body = {
    symbols: all ? ["all"] : selectedSymbols(),
    models: selectedModels(),
    date: els.analysisDate.value,
  };
  try {
    const res = await fetch("/api/generate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const payload = await res.json();
    if (!res.ok) throw new Error(payload.error || res.statusText);
    renderStatus(payload);
  } catch (err) {
    els.statusText.textContent = err.message;
  }
}

async function abortJob() {
  try {
    const res = await fetch("/api/abort", { method: "POST" });
    const payload = await res.json();
    if (!res.ok) throw new Error(payload.error || res.statusText);
    renderStatus(payload);
  } catch (err) {
    els.statusText.textContent = err.message;
  }
}

async function refreshAll() {
  await Promise.all([refreshStatus(), refreshReports(), refreshSchedules()]);
}

async function refreshStatus() {
  const status = await fetchJSON("/api/status");
  renderStatus(status);
}

async function refreshReports() {
  const sort = encodeURIComponent(els.sortReports.value || "ticker");
  const payload = await fetchJSON(`/api/reports?sort=${sort}`);
  renderReports(payload.reports || []);
}

async function refreshSchedules() {
  const payload = await fetchJSON("/api/schedules");
  renderSchedules(payload.schedules || []);
}

function renderStatus(job) {
  const total = job.total || 0;
  const done = (job.completed || 0) + (job.failed || 0);
  const pct = total ? Math.round((done / total) * 100) : 0;
  els.statusText.textContent = job.state === "running" ? "Generating reports" : "Ready";
  els.doneCount.textContent = `${job.completed || 0}${job.failed ? ` / ${job.failed} failed` : ""}`;
  els.leftCount.textContent = job.remaining || 0;
  els.jobMessage.textContent = job.message || "Ready";
  els.jobPercent.textContent = `${pct}%`;
  els.progressFill.style.width = `${pct}%`;
  els.generateSelectedBtn.disabled = job.state === "running";
  els.generateAllBtn.disabled = job.state === "running";
  els.abortBtn.disabled = job.state !== "running";
  if (job.current) {
    els.currentWork.textContent = `Working on ${job.current.symbol} with ${job.current.model}. ${job.completed || 0} complete, ${job.remaining || 0} left.`;
  } else if (total) {
    els.currentWork.textContent = `${job.completed || 0} complete, ${job.failed || 0} failed, ${job.remaining || 0} left.`;
  } else {
    els.currentWork.textContent = "Idle";
  }
}

function renderSchedules(schedules) {
  els.schedulesList.innerHTML = "";
  if (!schedules.length) {
    els.schedulesList.textContent = "No schedules configured.";
    return;
  }
  schedules.forEach((schedule) => {
    const row = document.createElement("div");
    row.className = "schedule-item";
    const last = schedule.last_run_at ? new Date(schedule.last_run_at).toLocaleString() : "never";
    row.innerHTML = `
      <div>
        <strong>${escapeHTML(schedule.name)}</strong>
        <span>${escapeHTML(schedule.time)} ${escapeHTML((schedule.days || []).join(","))} · ${escapeHTML((schedule.symbols || []).join(","))} · ${escapeHTML((schedule.models || []).join(","))}</span>
      </div>
      <div>
        <span class="badge ${schedule.enabled ? "latest" : ""}">${schedule.enabled ? "enabled" : "disabled"}</span>
        <span>${escapeHTML(schedule.last_status || "never")} ${escapeHTML(last)}</span>
      </div>
    `;
    els.schedulesList.append(row);
  });
}

function renderReports(reports) {
  els.reportCount.textContent = `${reports.length} ${reports.length === 1 ? "report" : "reports"}`;
  els.reportsBody.innerHTML = "";
  reports.forEach((report) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td><strong>${escapeHTML(report.ticker)}</strong></td>
      <td>${escapeHTML(report.model_label || report.model)} ${report.latest ? '<span class="badge latest">latest</span>' : ""}</td>
      <td>${escapeHTML(report.generated_display || "")}</td>
      <td>${escapeHTML(report.analysis_date || "")}</td>
      <td><span class="badge ${report.status === "failed" ? "failed" : ""}">${escapeHTML(report.status || "ready")}</span></td>
      <td class="links">
        <a href="${report.report_url}" target="_blank" rel="noopener">Open</a>
        <a href="${report.final_url}" target="_blank" rel="noopener">Final</a>
        <a href="${report.index_url}" target="_blank" rel="noopener">Index</a>
      </td>
    `;
    els.reportsBody.append(tr);
  });
}

async function fetchJSON(url) {
  const res = await fetch(url);
  const payload = await res.json();
  if (!res.ok) throw new Error(payload.error || res.statusText);
  return payload;
}

function escapeHTML(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

init().catch((err) => {
  els.statusText.textContent = err.message;
});
