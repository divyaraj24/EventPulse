const form = document.getElementById("test-form");
const startBtn = document.getElementById("start-btn");
const chaosCheckbox = document.getElementById("chaos-enabled");
const chaosFields = document.getElementById("chaos-fields");

const statusPanel = document.getElementById("status-panel");
const statusDot = document.getElementById("status-dot");
const statusBadge = document.getElementById("status-badge");
const statusDetail = document.getElementById("status-detail");
const progressFill = document.getElementById("progress-fill");
const errorBox = document.getElementById("error-box");

const resultPanel = document.getElementById("result-panel");
const chartImg = document.getElementById("chart-img");

const durationInput = document.getElementById("duration");
const chaosTimelineInputs = ["steady", "fault", "recovery"].map((id) => document.getElementById(id));
const chaosTotalHint = document.getElementById("chaos-total-hint");

const cancelBtn = document.getElementById("cancel-btn");
const historyTable = document.getElementById("history-table");
const historyBody = document.getElementById("history-body");
const historyEmpty = document.getElementById("history-empty");

const TERMINAL_STATUSES = new Set(["done", "failed", "cancelled"]);
const ACTIVE_STATUSES = new Set(["starting", "running", "draining", "extracting"]);

let pollHandle = null;

chaosCheckbox.addEventListener("change", () => {
  chaosFields.hidden = !chaosCheckbox.checked;
  updateChaosTotalHint();
});

function updateChaosTotalHint() {
  if (!chaosCheckbox.checked) return;
  const total = chaosTimelineInputs.reduce((sum, el) => sum + (parseFloat(el.value) || 0), 0);
  const duration = parseFloat(durationInput.value) || 0;
  if (duration < total) {
    chaosTotalHint.textContent =
      `Timeline totals ${total}s, but duration is only ${duration}s -- the run waits for ` +
      `both load generation and the full fault timeline, so it'll keep going ${(total - duration).toFixed(0)}s ` +
      `past 100% events with no new data, and recovery-phase behavior won't be captured.`;
    chaosTotalHint.classList.add("warn-note");
  } else {
    chaosTotalHint.textContent = `Timeline totals ${total}s (duration ${duration}s covers it).`;
    chaosTotalHint.classList.remove("warn-note");
  }
}

durationInput.addEventListener("input", updateChaosTotalHint);
chaosTimelineInputs.forEach((el) => el.addEventListener("input", updateChaosTotalHint));

function num(id) {
  return parseFloat(document.getElementById(id).value);
}

function setStatus(status, detail) {
  statusBadge.textContent = status;
  statusBadge.className = "badge badge-" + status;
  statusDetail.textContent = detail || "";
  statusDot.className = "status-dot dot-" + status + (ACTIVE_STATUSES.has(status) ? " dot-pulse" : "");
}

function stopPolling() {
  if (pollHandle) {
    clearInterval(pollHandle);
    pollHandle = null;
  }
}

async function fetchHistory() {
  const resp = await fetch("/test/history");
  if (!resp.ok) return;
  const rows = await resp.json();

  historyBody.innerHTML = "";
  historyEmpty.hidden = rows.length > 0;
  historyTable.hidden = rows.length === 0;

  for (const r of rows) {
    const tr = document.createElement("tr");
    const started = new Date(r.started_at).toLocaleString();
    tr.innerHTML = `
      <td>${r.label}</td>
      <td>${r.policy}</td>
      <td><span class="badge badge-${r.status}">${r.status}</span></td>
      <td>${started}</td>
    `;
    historyBody.appendChild(tr);
  }
}

async function pollStatus(runId) {
  const resp = await fetch(`/test/status/${runId}`);
  if (!resp.ok) {
    setStatus("error", `couldn't fetch status (HTTP ${resp.status})`);
    stopPolling();
    startBtn.disabled = false;
    cancelBtn.hidden = true;
    return;
  }
  const data = await resp.json();

  const progress = data.progress || {};
  const sent = progress.events_sent || 0;
  const total = progress.events_total || 0;
  const pct = total > 0 ? Math.round((sent / total) * 100) : 0;
  progressFill.style.width = pct + "%";

  let detail = `${sent}/${total} events`;
  if (data.chaos_phase) {
    detail += ` — fault phase: ${data.chaos_phase}`;
  }
  setStatus(data.status, detail);

  if (TERMINAL_STATUSES.has(data.status)) {
    stopPolling();
    startBtn.disabled = false;
    cancelBtn.hidden = true;
    fetchHistory();

    if (data.status === "done") {
      chartImg.src = `/test/result/${runId}/chart.png?t=${Date.now()}`;
      resultPanel.hidden = false;
    } else {
      errorBox.hidden = false;
      errorBox.textContent = data.error ||
        (data.status === "cancelled" ? "Run was cancelled." : "Run failed with no error message.");
    }
  }
}

cancelBtn.addEventListener("click", async () => {
  cancelBtn.disabled = true;
  try {
    await fetch("/test/cancel", { method: "POST" });
  } finally {
    cancelBtn.disabled = false;
  }
});

fetchHistory();

form.addEventListener("submit", async (e) => {
  e.preventDefault();
  startBtn.disabled = true;
  errorBox.hidden = true;
  resultPanel.hidden = true;
  statusPanel.hidden = false;
  cancelBtn.hidden = false;
  progressFill.style.width = "0%";
  setStatus("starting", "");

  const payload = {
    label: document.getElementById("label").value,
    rate: num("rate"),
    duration: num("duration"),
    policy: document.getElementById("policy").value,
    worker_concurrency: parseInt(document.getElementById("worker-concurrency").value, 10),
    chaos: {
      enabled: chaosCheckbox.checked,
      steady: num("steady"),
      fault: num("fault"),
      recovery: num("recovery"),
      max_concurrency: parseInt(document.getElementById("max-concurrency").value, 10),
      reject_rate: num("reject-rate"),
      latency_ms: parseInt(document.getElementById("latency-ms").value, 10),
      recovered_max_concurrency: parseInt(document.getElementById("recovered-max-concurrency").value, 10),
      recovered_latency_ms: parseInt(document.getElementById("recovered-latency-ms").value, 10),
    },
  };

  let resp;
  try {
    resp = await fetch("/test/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
  } catch (err) {
    setStatus("error", "couldn't reach the harness API");
    startBtn.disabled = false;
    cancelBtn.hidden = true;
    return;
  }

  if (resp.status === 409) {
    const body = await resp.json();
    setStatus("busy", body.detail?.detail || "a test is already running");
    startBtn.disabled = false;
    cancelBtn.hidden = true;
    return;
  }
  if (!resp.ok) {
    setStatus("error", `HTTP ${resp.status}`);
    startBtn.disabled = false;
    cancelBtn.hidden = true;
    return;
  }

  const { run_id } = await resp.json();
  setStatus("starting", `run ${run_id}`);
  pollHandle = setInterval(() => pollStatus(run_id), 1500);
});
