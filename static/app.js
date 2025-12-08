// static/app.js
// Фронтенд для управления приложением.
// - WebSocket push для логов (/ws/logs) и статуса (/ws/status).
// - Кнопки управления вызывают соответствующие REST endpoints.
// - Загрузка .session через /api/upload_session (требует api_id/api_hash/name).
// - Список загруженных сессий: /api/list_uploaded_sessions
// - Удаление: /api/delete_uploaded_session

(function () {
  // Константы
  const WS_LOGS = `/ws/logs`;
  const WS_STATUS = `/ws/status`;
  const RECONN_MS = 5000;

  // Элементы UI
  const delayMinEl = document.getElementById("delay_min");
  const delayMaxEl = document.getElementById("delay_max");
  const checkMinutesEl = document.getElementById("check_minutes");

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

  const fileInput = document.getElementById("fileInput");
  const sessionNameInput = document.getElementById("session_name");
  const sessionApiIdInput = document.getElementById("session_api_id");
  const sessionApiHashInput = document.getElementById("session_api_hash");
  const btnUpload = document.getElementById("btnUpload");

  const uploadedSessionsBody = document.getElementById("uploadedSessionsBody");
  const accountsBody = document.getElementById("accountsBody");
  const progressHead = document.getElementById("progressHead");
  const progressBody = document.getElementById("progressBody");
  const logArea = document.getElementById("logArea");

  let wsLogs = null;
  let wsStatus = null;

  // WebSocket: логи
  function connectLogsWS() {
    try {
      const proto = (location.protocol === "https:") ? "wss" : "ws";
      wsLogs = new WebSocket(`${proto}://${location.host}${WS_LOGS}`);
    } catch (e) {
      scheduleReconnect(connectLogsWS);
      return;
    }
    wsLogs.onopen = () => {
      console.info("WS logs connected");
    };
    wsLogs.onmessage = (ev) => {
      // Сообщения приходят в двух вариантах:
      // 1) Первое сообщение — JSON-string -> {"init":true,"logs":[...]}
      // 2) Последующие — text line (plain text)
      try {
        const parsed = JSON.parse(ev.data);
        if (parsed && parsed.init && Array.isArray(parsed.logs)) {
          setLogs(parsed.logs);
          return;
        }
      } catch (e) {
        // not JSON — просто текст
      }
      appendLog(ev.data);
    };
    wsLogs.onclose = () => {
      console.info("WS logs closed");
      scheduleReconnect(connectLogsWS);
    };
    wsLogs.onerror = (e) => {
      console.warn("WS logs error", e);
      try { wsLogs.close(); } catch (_) {}
    };
  }

  // WebSocket: статус (пуш)
  function connectStatusWS() {
    try {
      const proto = (location.protocol === "https:") ? "wss" : "ws";
      wsStatus = new WebSocket(`${proto}://${location.host}${WS_STATUS}`);
    } catch (e) {
      scheduleReconnect(connectStatusWS);
      return;
    }
    wsStatus.onopen = () => {
      console.info("WS status connected");
    };
    wsStatus.onmessage = (ev) => {
      try {
        const data = JSON.parse(ev.data);
        if (data && data.stats) {
          updateStats(data.stats);
        }
        if (data && data.accounts_meta) {
          updateAccounts(data.accounts_meta, data.cooldowns || {});
        }
        // попытка обновить прогресс таблицы (если уже загружена)
        if (data && data.stats) {
          refreshProgressOnce();
        }
      } catch (e) {
        console.warn("WS status parse error", e);
      }
    };
    wsStatus.onclose = () => {
      console.info("WS status closed");
      scheduleReconnect(connectStatusWS);
    };
    wsStatus.onerror = (e) => {
      console.warn("WS status error", e);
      try { wsStatus.close(); } catch (_) {}
    };
  }

  function scheduleReconnect(fn) {
    setTimeout(fn, RECONN_MS);
  }

  // Логи
  function setLogs(lines) {
    logArea.innerHTML = "";
    for (const l of lines) {
      appendLog(l);
    }
  }
  function appendLog(line) {
    const atBottom = (logArea.scrollTop + logArea.clientHeight >= logArea.scrollHeight - 10);
    const div = document.createElement("div");
    div.textContent = line;
    logArea.appendChild(div);
    // лимит строк во view
    while (logArea.children.length > 2000) {
      logArea.removeChild(logArea.firstChild);
    }
    if (atBottom) logArea.scrollTop = logArea.scrollHeight;
  }

  // Accounts table update
  function updateAccounts(accounts_meta, cooldowns) {
    accountsBody.innerHTML = "";
    const names = Object.keys(accounts_meta || {});
    if (names.length === 0) {
      // fallback: запрос к API
      fetch("/api/accounts_status").then(r => r.json()).then(j => {
        renderAccountsFromApi(j.accounts || []);
      }).catch(() => {
        accountsBody.innerHTML = '<tr><td colspan="4" class="muted">Нет данных</td></tr>';
      });
      return;
    }
    const now = Math.floor(Date.now()/1000);
    for (const name of names) {
      const meta = accounts_meta[name] || {};
      const cd_until = (cooldowns && cooldowns[name]) ? Math.max(0, Math.floor(cooldowns[name] - now)) : 0;
      const cooldown_display = cd_until > 0 ? format_mmss(cd_until) : "—";
      const auth = meta && meta.last_action ? "?" : "—";
      const tr = document.createElement("tr");
      tr.innerHTML = `<td>${name}</td><td>${auth}</td><td>${cooldown_display}</td><td>${meta.last_action || ""}</td>`;
      accountsBody.appendChild(tr);
    }
  }

  function renderAccountsFromApi(arr) {
    accountsBody.innerHTML = "";
    if (!arr || arr.length === 0) {
      accountsBody.innerHTML = '<tr><td colspan="4" class="muted">Нет аккаунтов</td></tr>';
      return;
    }
    for (const a of arr) {
      const cd = a.cooldown_remaining || 0;
      const tr = document.createElement("tr");
      tr.innerHTML = `<td>${a.name}</td><td>${a.authorized ? "✓" : "✗"}</td><td>${cd>0?format_mmss(cd):"—"}</td><td>${a.last_action||""}</td>`;
      accountsBody.appendChild(tr);
    }
  }

  function format_mmss(sec) {
    sec = Math.max(0, parseInt(sec||0));
    const m = Math.floor(sec/60);
    const s = sec%60;
    return `${String(m).padStart(2,"0")}:${String(s).padStart(2,"0")}`;
  }

  // Stats update (simple)
  function updateStats(stats) {
    // можно вынести в UI - сейчас просто логим
    // console.log("stats", stats);
  }

  // Progress table (targets x accounts)
  let progressLoaded = false;
  function refreshProgressOnce() {
    if (progressLoaded) return;
    fetch("/api/progress").then(r => r.json()).then(j => {
      buildProgressTable(j.accounts || [], j.targets || [], j.results || {});
      progressLoaded = true;
    }).catch(e => console.warn("progress fetch error", e));
  }

  function buildProgressTable(accounts, targets, results) {
    progressHead.innerHTML = "";
    progressBody.innerHTML = "";
    const headRow = document.createElement("tr");
    const accCols = accounts.map(a => `<th>${escapeHtml(a)}</th>`).join("");
    headRow.innerHTML = `<th>Target</th><th>Title</th>${accCols}`;
    progressHead.appendChild(headRow);
    for (const t of targets) {
      const tr = document.createElement("tr");
      const key = t.key || "";
      const title = t.title || "";
      let linkCell = escapeHtml(key);
      if (key && !/^\d+$/.test(key)) {
        const username = key.replace(/^@/,"");
        linkCell = `<a class="link" href="https://t.me/${encodeURIComponent(username)}" target="_blank">${escapeHtml(username)}</a>`;
      }
      let row = `<td>${linkCell}</td><td>${escapeHtml(title)}</td>`;
      for (const a of accounts) {
        const val = (results[key] && (a in results[key])) ? (results[key][a] ? "✓" : "✗") : "—";
        row += `<td style="text-align:center">${val}</td>`;
      }
      tr.innerHTML = row;
      progressBody.appendChild(tr);
    }
  }

  // Escape
  function escapeHtml(s) {
    if (!s && s !== 0) return "";
    return String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
  }

  // Uploaded sessions handling
  function refreshUploadedSessions() {
    fetch("/api/list_uploaded_sessions").then(r => r.json()).then(j => {
      if (!j.ok) {
        uploadedSessionsBody.innerHTML = '<tr><td colspan="5" class="muted">Ошибка получения списка</td></tr>';
        return;
      }
      const arr = j.sessions || [];
      uploadedSessionsBody.innerHTML = "";
      if (arr.length === 0) {
        uploadedSessionsBody.innerHTML = '<tr><td colspan="5" class="muted">Нет загруженных файлов</td></tr>';
        return;
      }
      for (const s of arr) {
        const exists = s.exists ? "yes" : "no";
        const tr = document.createElement("tr");
        tr.innerHTML = `<td>${escapeHtml(s.filename)}</td><td>${escapeHtml(s.name||"")}</td><td>${escapeHtml(s.api_id||"")}</td><td>${exists}</td>
          <td>
            <button class="deleteSessionBtn" data-file="${encodeURIComponent(s.filename)}">Удалить</button>
          </td>`;
        uploadedSessionsBody.appendChild(tr);
      }
      // attach delete handlers
      Array.from(document.getElementsByClassName("deleteSessionBtn")).forEach(btn => {
        btn.onclick = () => {
          const filename = decodeURIComponent(btn.getAttribute("data-file"));
          if (!confirm("Удалить сессию " + filename + " ?")) return;
          const fd = new FormData();
          fd.append("filename", filename);
          fetch("/api/delete_uploaded_session", { method: "POST", body: fd }).then(r=>r.json()).then(j => {
            if (!j.ok) alert("Ошибка: " + (j.msg || JSON.stringify(j)));
            refreshUploadedSessions();
          }).catch(e => alert("Ошибка: " + e));
        };
      });
    }).catch(e => {
      uploadedSessionsBody.innerHTML = '<tr><td colspan="5" class="muted">Ошибка запроса</td></tr>';
    });
  }

  // Upload handler
  btnUpload.addEventListener("click", () => {
    const file = fileInput.files[0];
    if (!file) {
      alert("Выберите файл .session");
      return;
    }
    const name = sessionNameInput.value.trim();
    const api_id = sessionApiIdInput.value.trim();
    const api_hash = sessionApiHashInput.value.trim();
    if (!api_id || !api_hash || !name) {
      alert("Заполните поля: Имя аккаунта, API_ID и API_HASH (обязательны).");
      return;
    }
    const fd = new FormData();
    fd.append("file", file, file.name);
    fd.append("name", name);
    fd.append("api_id", api_id);
    fd.append("api_hash", api_hash);
    btnUpload.disabled = true;
    btnUpload.textContent = "Загрузка...";
    fetch("/api/upload_session", { method: "POST", body: fd }).then(r => r.json()).then(j => {
      btnUpload.disabled = false;
      btnUpload.textContent = "Загрузить .session";
      if (!j.ok) {
        alert("Ошибка: " + (j.msg || JSON.stringify(j)));
      } else {
        alert("Файл загружен: " + (j.file || ""));
        refreshUploadedSessions();
      }
    }).catch(e => {
      btnUpload.disabled = false;
      btnUpload.textContent = "Загрузить .session";
      alert("Ошибка загрузки: " + e);
    });
  });

  // Buttons: start/pause/resume/stop
  btnStart.addEventListener("click", () => {
    const min = parseFloat(delayMinEl.value || "5");
    const max = parseFloat(delayMaxEl.value || "60");
    const checkMin = parseInt(checkMinutesEl.value || "60");
    if (isNaN(min) || isNaN(max) || min < 0 || max < min) {
      alert("Некорректные задержки. Проверьте min/max.");
      return;
    }
    const fd = new FormData();
    fd.append("per_account_delay_min", String(min));
    fd.append("per_account_delay_max", String(max));
    fd.append("membership_check_minutes", String(checkMin));
    btnStart.disabled = true;
    btnStart.textContent = "Запускаю...";
    fetch("/api/start", { method: "POST", body: fd }).then(r => r.json()).then(j => {
      btnStart.disabled = false;
      btnStart.textContent = "Запустить Подписку";
      if (!j.ok) alert("Ошибка: " + (j.msg || JSON.stringify(j)));
      else {
        appendLog("UI: подписка запущена.");
        // очистим прогресс флаг, чтобы следующая загрузка прогресса произошла
        progressLoaded = false;
      }
    }).catch(e => {
      btnStart.disabled = false;
      btnStart.textContent = "Запустить Подписку";
      alert("Ошибка: " + e);
    });
  });

  btnPause.addEventListener("click", () => {
    fetch("/api/pause", { method: "POST" }).then(r => r.json()).then(j => {
      if (!j.ok) alert("Ошибка: " + (j.msg || JSON.stringify(j)));
    }).catch(e => alert("Ошибка: " + e));
  });
  btnResume.addEventListener("click", () => {
    fetch("/api/resume", { method: "POST" }).then(r => r.json()).then(j => {
      if (!j.ok) alert("Ошибка: " + (j.msg || JSON.stringify(j)));
    }).catch(e => alert("Ошибка: " + e));
  });
  btnStop.addEventListener("click", () => {
    if (!confirm("Остановить задачу подписки?")) return;
    fetch("/api/stop", { method: "POST" }).then(r => r.json()).then(j => {
      if (!j.ok) alert("Ошибка: " + (j.msg || JSON.stringify(j)));
    }).catch(e => alert("Ошибка: " + e));
  });

  btnCheckNow.addEventListener("click", () => {
    fetch("/api/check_now", { method: "POST" }).then(r => r.json()).then(j => {
      if (!j.ok) alert("Ошибка: " + (j.msg || JSON.stringify(j)));
      else {
        progressLoaded = false;
        appendLog("UI: запущена немедленная проверка.");
      }
    }).catch(e => alert("Ошибка: " + e));
  });

  btnStopCheck.addEventListener("click", () => {
    fetch("/api/stop_check", { method: "POST" }).then(r => r.json()).then(j => {
      if (!j.ok) alert("Ошибка: " + (j.msg || JSON.stringify(j)));
      else appendLog("UI: запрошена остановка проверки.");
    }).catch(e => alert("Ошибка: " + e));
  });

  btnUnsub.addEventListener("click", () => {
    if (!confirm("Подтвердите запуск массовой отписки (Unsubscribe).")) return;
    fetch("/api/unsubscribe", { method: "POST" }).then(r => r.json()).then(j => {
      if (!j.ok) alert("Ошибка: " + (j.msg || JSON.stringify(j)));
      else {
        appendLog("UI: задача отписки запущена.");
      }
    }).catch(e => alert("Ошибка: " + e));
  });

  btnStopUnsub.addEventListener("click", () => {
    fetch("/api/stop_unsubscribe", { method: "POST" }).then(r => r.json()).then(j => {
      if (!j.ok) alert("Ошибка: " + (j.msg || JSON.stringify(j)));
      else appendLog("UI: запрошена остановка отписки.");
    }).catch(e => alert("Ошибка: " + e));
  });

  btnDownloadGood.addEventListener("click", () => {
    window.location.href = "/download/good_targets";
  });

  btnClearGood.addEventListener("click", () => {
    if (!confirm("Перезаписать good_targets.csv (очистить)?")) return;
    const fd = new FormData();
    fd.append("confirm", "true");
    fetch("/api/clear_good_targets", { method: "POST", body: fd }).then(r => r.json()).then(j => {
      if (!j.ok) alert("Ошибка: " + (j.msg || JSON.stringify(j)));
      else {
        alert("good_targets.csv очищен.");
      }
    }).catch(e => alert("Ошибка: " + e));
  });

  btnExportLogs.addEventListener("click", () => {
    window.location.href = "/api/export_logs";
  });

  // Init
  function init() {
    connectLogsWS();
    connectStatusWS();
    refreshUploadedSessions();
    refreshProgressOnce();
    fetch("/api/accounts_status").then(r => r.json()).then(j => {
      renderAccountsFromApi(j.accounts || []);
    }).catch(() => {});
  }

  // render accounts fallback
  function renderAccountsFromApi(arr) {
    accountsBody.innerHTML = "";
    if (!arr || arr.length === 0) {
      accountsBody.innerHTML = '<tr><td colspan="4" class="muted">Нет аккаунтов</td></tr>';
      return;
    }
    for (const a of arr) {
      const cd = a.cooldown_remaining || 0;
      const tr = document.createElement("tr");
      tr.innerHTML = `<td>${escapeHtml(a.name)}</td><td>${a.authorized? "✓":"✗"}</td><td>${cd>0?format_mmss(cd):"—"}</td><td>${escapeHtml(a.last_action||"")}</td>`;
      accountsBody.appendChild(tr);
    }
  }

  // Utility
  function format_mmss(sec) {
    sec = Math.max(0, parseInt(sec||0));
    const m = Math.floor(sec/60);
    const s = sec%60;
    return `${String(m).padStart(2,"0")}:${String(s).padStart(2,"0")}`;
  }
  function escapeHtml(s) {
    if (!s && s !== 0) return "";
    return String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
  }

  // Запуск
  init();

})();
