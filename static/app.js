// static/app.js
// UI использует WebSocket push (статус/логи), кнопки вызывают HTTP POST.
// Комментарии — на русском.

(function () {
  const WS_STATUS_PATH = `/ws/status`;
  const WS_LOGS_PATH = `/ws/logs`;
  const RECONNECT_INTERVAL = 5000;

  const btnStart = document.getElementById("btnStart");
  const btnPause = document.getElementById("btnPause");
  const btnResume = document.getElementById("btnResume");
  const btnStop = document.getElementById("btnStop");
  const btnCheckNow = document.getElementById("btnCheckNow");
  const btnStopCheck = document.getElementById("btnStopCheck");
  const btnUnsub = document.getElementById("btnUnsub");
  const btnStopUnsub = document.getElementById("btnStopUnsub");
  const btnDownloadGood = document.getElementById("btnDownloadGood");
  const btnClearGood = document.getElementById("btnClearGood");
  const btnExportLogs = document.getElementById("btnExportLogs");

  const delayMinInput = document.getElementById("delay_min");
  const delayMaxInput = document.getElementById("delay_max");
  const checkMinutesInput = document.getElementById("check_minutes");

  const runningEl = document.getElementById("running");
  const subscribeProgressEl = document.getElementById("subscribe_progress");
  const checkProgressEl = document.getElementById("check_progress");
  const totalTargetsEl = document.getElementById("total_targets");
  const attemptedEl = document.getElementById("attempted");
  const approvedEl = document.getElementById("approved");

  const accountsBody = document.getElementById("accountsBody");
  const progressHead = document.getElementById("progressHead");
  const progressBody = document.getElementById("progressBody");
  const logArea = document.getElementById("logArea");

  let wsStatus = null;
  let wsLogs = null;

  function connectStatusWS() {
    const protocol = (location.protocol === "https:") ? "wss" : "ws";
    const url = `${protocol}://${location.host}${WS_STATUS_PATH}`;
    try {
      wsStatus = new WebSocket(url);
    } catch (e) {
      console.error("WS status connect error:", e);
      scheduleReconnect(connectStatusWS);
      return;
    }
    wsStatus.onopen = () => console.info("WS status connected");
    wsStatus.onmessage = (ev) => {
      try {
        const data = JSON.parse(ev.data);
        handleStatusUpdate(data);
      } catch (e) {
        console.error("WS status parse error", e, ev.data);
      }
    };
    wsStatus.onclose = () => { console.info("WS status closed"); scheduleReconnect(connectStatusWS); };
    wsStatus.onerror = (e) => { console.error("WS status error", e); try { wsStatus.close(); } catch {} };
  }

  function connectLogsWS() {
    const protocol = (location.protocol === "https:") ? "wss" : "ws";
    const url = `${protocol}://${location.host}${WS_LOGS_PATH}`;
    try {
      wsLogs = new WebSocket(url);
    } catch (e) {
      console.error("WS logs connect error:", e);
      scheduleReconnect(connectLogsWS);
      return;
    }
    wsLogs.onopen = () => {
      console.info("WS logs connected");
      setInterval(() => {
        try { wsLogs && wsLogs.send("ping"); } catch (e) {}
      }, 25000);
    };
    wsLogs.onmessage = (ev) => {
      try {
        const payload = JSON.parse(ev.data);
        if (payload && payload.init && Array.isArray(payload.logs)) {
          setLogLines(payload.logs);
        } else {
          if (payload && payload.log) appendLog(payload.log);
          else appendLog(ev.data);
        }
      } catch (err) {
        appendLog(ev.data);
      }
    };
    wsLogs.onclose = () => { console.info("WS logs closed"); scheduleReconnect(connectLogsWS); };
    wsLogs.onerror = (e) => { console.error("WS logs error", e); try { wsLogs.close(); } catch {} };
  }

  function scheduleReconnect(fn) { setTimeout(() => { try { fn(); } catch (e) {} }, RECONNECT_INTERVAL); }

  function handleStatusUpdate(data) {
    try {
      if (data.stats) {
        const s = data.stats;
        subscribeProgressEl.textContent = (s.subscribe_progress || 0) + "%";
        checkProgressEl.textContent = (s.check_progress || 0) + "%";
        totalTargetsEl.textContent = s.total_targets || 0;
        attemptedEl.textContent = s.attempted || 0;
        approvedEl.textContent = s.approved || 0;
        runningEl.textContent = data.running ? "true" : "false";
      }
      if (data.accounts_meta || data.cooldowns) updateAccountsTable(data.accounts_meta || {}, data.cooldowns || {});
      if (data && data.stats) maybeUpdateProgressTable();
    } catch (e) { console.error("handleStatusUpdate error", e); }
  }

  function updateAccountsTable(accounts_meta, cooldowns) {
    accountsBody.innerHTML = "";
    const names = Object.keys(accounts_meta || {});
    if (names.length === 0) {
      fetch("/api/accounts_status").then(r => r.json()).then(j => renderAccountsFromApi(j.accounts || [])).catch(()=>{});
      return;
    }
    const now = Math.floor(Date.now()/1000);
    for (const name of names) {
      const meta = accounts_meta[name] || {};
      const cd_until = cooldowns && cooldowns[name] ? Math.max(0, Math.floor(cooldowns[name] - now)) : 0;
      const tr = document.createElement("tr");
      tr.innerHTML = `<td>${name}</td><td>${meta.authorized ? "true" : "?"}</td><td>${cd_until}</td><td>${meta.last_action || ""}</td>`;
      accountsBody.appendChild(tr);
    }
  }

  function renderAccountsFromApi(arr) {
    accountsBody.innerHTML = "";
    for (const a of arr) {
      const tr = document.createElement("tr");
      tr.innerHTML = `<td>${a.name}</td><td>${a.authorized ? "true" : "false"}</td><td>${a.cooldown_remaining}</td><td>${a.last_action || ""}</td>`;
      accountsBody.appendChild(tr);
    }
  }

  let lastProgressTotal = null;
  function maybeUpdateProgressTable() {
    if (lastProgressTotal !== null) return;
    fetch("/api/progress").then(r => r.json()).then(j => {
      buildProgressTable(j.accounts || [], j.targets || [], j.results || {});
      lastProgressTotal = (j.targets || []).length;
    }).catch(e => console.error("progress fetch error", e));
  }

  function buildProgressTable(accounts, targets, results) {
    progressHead.innerHTML = "";
    progressBody.innerHTML = "";
    const headRow = document.createElement("tr");
    headRow.innerHTML = `<th>Target</th><th>Title</th>` + accounts.map(a => `<th>${a}</th>`).join("");
    progressHead.appendChild(headRow);
    for (const t of targets) {
      const tr = document.createElement("tr");
      const targetKey = t.key || "";
      const title = t.title || "";
      let linkHtml = targetKey;
      if (targetKey && !targetKey.match(/^\d+$/)) {
        const username = targetKey.replace(/^@/, "");
        linkHtml = `<a class="link" href="https://t.me/${username}" target="_blank">${username}</a>`;
      }
      const cells = [`<td>${linkHtml}</td>`, `<td>${title}</td>`];
      for (const a of accounts) {
        const val = (results[targetKey] && results[targetKey][a]) ? "✓" : (results[targetKey] && (a in results[targetKey]) ? "✗" : "—");
        cells.push(`<td>${val}</td>`);
      }
      tr.innerHTML = cells.join("");
      progressBody.appendChild(tr);
    }
  }

  function setLogLines(lines) { logArea.innerHTML = ""; for (const ln of lines) appendLog(ln); }
  function appendLog(line) {
    const atBottom = (logArea.scrollTop + logArea.clientHeight >= logArea.scrollHeight - 10);
    const div = document.createElement("div"); div.textContent = line; logArea.appendChild(div);
    while (logArea.children.length > 2000) logArea.removeChild(logArea.firstChild);
    if (atBottom) logArea.scrollTop = logArea.scrollHeight;
  }

  // Кнопки
  btnStart.addEventListener("click", () => {
    const min = parseFloat(delayMinInput.value || "5");
    const max = parseFloat(delayMaxInput.value || "60");
    const checkmin = parseFloat(checkMinutesInput.value || "60");
    const form = new FormData();
    form.append("per_account_delay_min", String(min));
    form.append("per_account_delay_max", String(max));
    form.append("membership_check_minutes", String(checkmin));
    fetch("/api/start", { method: "POST", body: form }).then(r => r.json()).then(j => {
      if (!j.ok) alert("Ошибка: " + (j.msg || JSON.stringify(j)));
      else lastProgressTotal = null;
    });
  });

  btnPause.addEventListener("click", () => fetch("/api/pause", {method:"POST"}).then(r=>r.json()).then(j => { if(!j.ok) alert("Ошибка: "+(j.msg||JSON.stringify(j))) }));
  btnResume.addEventListener("click", () => fetch("/api/resume", {method:"POST"}).then(r=>r.json()).then(j => { if(!j.ok) alert("Ошибка: "+(j.msg||JSON.stringify(j))) }));
  btnStop.addEventListener("click", () => fetch("/api/stop", {method:"POST"}).then(r=>r.json()).then(j => { if(!j.ok) alert("Ошибка: "+(j.msg||JSON.stringify(j))) }));

  btnCheckNow.addEventListener("click", () => fetch("/api/check_now", {method:"POST"}).then(r=>r.json()).then(j => { if(!j.ok) alert("Ошибка: "+(j.msg||JSON.stringify(j))); else lastProgressTotal = null; }));
  btnStopCheck.addEventListener("click", () => fetch("/api/stop_check", {method:"POST"}).then(r=>r.json()).then(j => { if(!j.ok) alert("Ошибка: "+(j.msg||JSON.stringify(j))) }));

  btnUnsub.addEventListener("click", () => { if (!confirm("Вы уверены, что хотите запустить массовую отписку?")) return; fetch("/api/unsubscribe", {method:"POST"}).then(r=>r.json()).then(j => { if(!j.ok) alert("Ошибка: "+(j.msg||JSON.stringify(j))) }); });
  btnStopUnsub.addEventListener("click", () => fetch("/api/stop_unsubscribe", {method:"POST"}).then(r=>r.json()).then(j => { if(!j.ok) alert("Ошибка: "+(j.msg||JSON.stringify(j))) }));

  btnDownloadGood.addEventListener("click", () => window.location.href = "/download/good_targets");
  btnClearGood.addEventListener("click", () => { if(!confirm("Перезаписать good_targets.csv (очистить)?")) return; const f=new FormData(); f.append("confirm","true"); fetch("/api/clear_good_targets",{method:"POST",body:f}).then(r=>r.json()).then(j=>{ if(!j.ok) alert("Ошибка: "+(j.msg||JSON.stringify(j))); else alert("good_targets.csv очищен.") }) });
  btnExportLogs.addEventListener("click", () => window.location.href = "/api/export_logs");

  function init() {
    connectStatusWS();
    connectLogsWS();
    fetch("/api/progress").then(r => r.json()).then(j => buildProgressTable(j.accounts || [], j.targets || [], j.results || {})).catch(()=>{});
    fetch("/api/accounts_status").then(r => r.json()).then(j => renderAccountsFromApi(j.accounts || [])).catch(()=>{});
    fetch("/api/status").then(r => r.json()).then(j => { if (j.stats) { subscribeProgressEl.textContent = (j.stats.subscribe_progress || 0) + "%"; checkProgressEl.textContent = (j.stats.check_progress || 0) + "%"; totalTargetsEl.textContent = j.stats.total_targets || 0; attemptedEl.textContent = j.stats.attempted || 0; approvedEl.textContent = j.stats.approved || 0; runningEl.textContent = j.running ? "true" : "false"; } }).catch(()=>{});
  }

  init();

})();
