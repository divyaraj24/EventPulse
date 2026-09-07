const form = document.getElementById("test-form");
const startBtn = document.getElementById("start-btn");
const chaosCheckbox = document.getElementById("chaos-enabled");
const chaosFields = document.getElementById("chaos-fields");

const statusPanel = document.getElementById("status-panel");
const statusBadge = document.getElementById("status-badge");
const statusDetail = document.getElementById("status-detail");
const progressFill = document.getElementById("progress-fill");
const errorBox = document.getElementById("error-box");

const resultPanel = document.getElementById("result-panel");
const chartImg = document.getElementById("chart-img");

let pollHandle = null;

chaosCheckbox.addEventListener("change", () => {
  chaosFields.hidden = !chaosCheckbox.checked;
});

function num(id) {
  return parseFloat(document.getElementById(id).value);
}

function setStatus(status, detail) {
  statusBadge.textContent = status;
  statusBadge.className = "badge badge-" + status;
  statusDetail.textContent = detail || "";
}

function stopPolling() {
  if (pollHandle) {
    clearInterval(pollHandle);
    pollHandle = null;
  }
}

async function pollStatus(runId) {
  const resp = await fetch(`/test/status/${runId}`);
  if (!resp.ok) {
    setStatus("error", `couldn't fetch status (HTTP ${resp.status})`);
    stopPolling();
    startBtn.disabled = false;
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

  if (data.status === "done") {
    stopPolling();
    startBtn.disabled = false;
    chartImg.src = `/test/result/${runId}/chart.png?t=${Date.now()}`;
    resultPanel.hidden = false;
  } else if (data.status === "failed") {
    stopPolling();
    startBtn.disabled = false;
    errorBox.hidden = false;
    errorBox.textContent = data.error || "Run failed with no error message.";
  }
}

form.addEventListener("submit", async (e) => {
  e.preventDefault();
  startBtn.disabled = true;
  errorBox.hidden = true;
  resultPanel.hidden = true;
  statusPanel.hidden = false;
  progressFill.style.width = "0%";
  setStatus("starting", "");

  const payload = {
    label: document.getElementById("label").value,
    rate: num("rate"),
    duration: num("duration"),
    policy: document.getElementById("policy").value,
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
    return;
  }

  if (resp.status === 409) {
    const body = await resp.json();
    setStatus("busy", body.detail?.detail || "a test is already running");
    startBtn.disabled = false;
    return;
  }
  if (!resp.ok) {
    setStatus("error", `HTTP ${resp.status}`);
    startBtn.disabled = false;
    return;
  }

  const { run_id } = await resp.json();
  setStatus("starting", `run ${run_id}`);
  pollHandle = setInterval(() => pollStatus(run_id), 1500);
});
