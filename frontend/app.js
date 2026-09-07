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

const eventsPanel = document.getElementById("events-panel");
const eventsList = document.getElementById("events-list");

const durationInput = document.getElementById("duration");
const rateInput = document.getElementById("rate");
const workerConcurrencyInput = document.getElementById("worker-concurrency");
const chaosTimelineInputs = ["steady", "fault", "recovery"].map((id) => document.getElementById(id));
const chaosTotalHint = document.getElementById("chaos-total-hint");
const faultRhoHint = document.getElementById("fault-rho-hint");
const recoveryRhoHint = document.getElementById("recovery-rho-hint");

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
  updateRhoHints();
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

// rho = offered rate / sustainable service rate (mu = min(worker's own
// concurrency, receiver's max concurrency) / service time). Empirically
// derived this session: worker's own semaphore only produces delay/backlog
// (no real rejections, since httpx's timeout only covers the network call,
// not time spent waiting for a slot), while the receiver's own ceiling
// produces real 503s the instant demand exceeds it. Whichever is smaller
// determines both the number AND the failure mode.
function computeRho(rate, maxConcurrency, latencyMs, workerConcurrency) {
  const effectiveConcurrency = Math.min(maxConcurrency, workerConcurrency);
  if (latencyMs <= 0 || effectiveConcurrency <= 0) {
    return { rho: 0, mu: Infinity, bindingSide: null };
  }
  const mu = effectiveConcurrency / (latencyMs / 1000);
  return { rho: rate / mu, mu, bindingSide: workerConcurrency < maxConcurrency ? "worker" : "receiver" };
}

function describeRho(hintEl, phaseLabel, maxConcurrencyId, latencyMsId) {
  const rate = parseFloat(rateInput.value) || 0;
  const workerConcurrency = parseFloat(workerConcurrencyInput.value) || 0;
  const maxConcurrency = parseFloat(document.getElementById(maxConcurrencyId).value) || 0;
  const latencyMs = parseFloat(document.getElementById(latencyMsId).value) || 0;

  if (latencyMs <= 0) {
    hintEl.textContent = `${phaseLabel}: latency is 0ms, so the concurrency ceiling never binds ` +
      `(a slot frees instantly) -- this phase won't create real backpressure regardless of max concurrency.`;
    hintEl.classList.add("warn-note");
    return;
  }

  const { rho, mu, bindingSide } = computeRho(rate, maxConcurrency, latencyMs, workerConcurrency);
  const capacityNote = bindingSide === "worker"
    ? `worker's own concurrency (${workerConcurrency}) is the binding constraint, not the receiver's -- this produces delay/backlog, not real rejections`
    : `the receiver's max concurrency (${maxConcurrency}) is the binding constraint -- excess demand gets rejected (503), not queued`;

  hintEl.textContent = `${phaseLabel}: ρ = ${rho.toFixed(2)} (capacity ${mu.toFixed(1)} ev/s vs ${rate} ev/s offered) -- ` +
    `${rho >= 1 ? "genuine overload" : "comfortably under capacity"}; ${capacityNote}.`;
  hintEl.classList.toggle("warn-note", rho >= 1);
}

function updateRhoHints() {
  if (!chaosCheckbox.checked) return;
  describeRho(faultRhoHint, "Fault", "max-concurrency", "latency-ms");
  describeRho(recoveryRhoHint, "Recovery", "recovered-max-concurrency", "recovered-latency-ms");
}

[rateInput, workerConcurrencyInput, "max-concurrency", "latency-ms", "recovered-max-concurrency", "recovered-latency-ms"]
  .map((x) => (typeof x === "string" ? document.getElementById(x) : x))
  .forEach((el) => el.addEventListener("input", updateRhoHints));

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

async function fetchEvents() {
  try {
    const resp = await fetch("/test/events");
    if (!resp.ok) return;
    const events = await resp.json();
    eventsList.innerHTML = events.map((e) => `<div class="event-line">${e}</div>`).join("");
  } catch (err) {
    // cosmetic feature -- a failed fetch here shouldn't disrupt the status poll
  }
}

async function pollStatus(runId) {
  fetchEvents();
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
  eventsPanel.hidden = false;
  eventsList.innerHTML = "";
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
