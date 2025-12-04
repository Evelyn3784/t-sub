# main.py
"""
Основной модуль FastAPI + Telethon.

Ключевые улучшения в этой версии:
- Подписка/Проверка/Отписка выполняются как набор worker'ов — по одному воркеру на аккаунт.
  Каждый аккаунт хранит свою позицию в списке targets и возобновляет работу с того же места
  после cooldown или после паузы/возобновления.
- Отложенная проверка (membership_check_minutes) теперь запускается строго через указанное время
  после завершения фазы "Подписка". Проверка дополняет existing good_targets.csv (не перезаписывает).
- Общая задержка (min/max) задаётся в UI и применяется ко всем фазам.
- Синхронизация обновления shared state осуществляется через asyncio.Lock (state['_lock']).
- WebSocket push (статус/логи) сохранён — UI получает события в режиме push.
- Self-ping (автопинг) интегрирован в приложение: при включении через env переменные
  приложение само будет делать GET-запросы к указанному URL через указанный интервал,
  чтобы предотвратить "засыпание" на бесплатных тарифах.

Комментарии в коде — на русском языке.
"""
import asyncio
import os
import re
import time
import random
import json
from datetime import datetime
from typing import List, Dict, Any, Optional, Set, Callable

import requests
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, Form, HTTPException
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import pandas as pd
from dotenv import load_dotenv

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import RPCError, ChannelPrivateError, UserAlreadyParticipantError
from telethon.tl.functions.channels import JoinChannelRequest, LeaveChannelRequest

load_dotenv()

app = FastAPI()
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

DATA_DIR = os.getenv("DATA_DIR", ".")
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR, exist_ok=True)

TARGETS_FILE = os.path.join(DATA_DIR, "targets.csv")
GOOD_TARGETS_FILE = os.path.join(DATA_DIR, "good_targets.csv")
LOG_FILE = os.path.join(DATA_DIR, "logs.txt")
LOG_MAX_LINES = 1000

# -------------------------
# Self-ping конфигурация (переменные окружения)
# -------------------------
# SELF_PING_ENABLED - true/false
# SELF_PING_URL - URL, который будет пинговаться (например https://<app>.onrender.com/api/accounts_status)
# SELF_PING_INTERVAL - интервал в секундах (рекомендуется >= 20)
# Self-ping использует requests (блокирующий) внутри asyncio.to_thread.
# Логи self-ping идут в общий лог через append_log.
# -------------------------

# -------------------------
# Notifications config
# -------------------------
TELEGRAM_NOTIFICATIONS_ENABLED = os.getenv("TELEGRAM_NOTIFICATIONS_ENABLED", "false").lower() in ("1", "true", "yes")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

EMAIL_NOTIFICATIONS_ENABLED = os.getenv("EMAIL_NOTIFICATIONS_ENABLED", "false").lower() in ("1", "true", "yes")
SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587") or 587)
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
NOTIFY_EMAIL_TO = os.getenv("NOTIFY_EMAIL_TO", "")

NOTIFY_ON_SUBSCRIBE_COMPLETE = os.getenv("NOTIFY_ON_SUBSCRIBE_COMPLETE", "true").lower() in ("1", "true", "yes")
NOTIFY_ON_CHECK_COMPLETE = os.getenv("NOTIFY_ON_CHECK_COMPLETE", "true").lower() in ("1", "true", "yes")
NOTIFY_ON_UNSUBSCRIBE_COMPLETE = os.getenv("NOTIFY_ON_UNSUBSCRIBE_COMPLETE", "true").lower() in ("1", "true", "yes")

# -------------------------
# Global state
# -------------------------
state: Dict[str, Any] = {
    "running_task": None,
    "pause_event": asyncio.Event(),
    "stop_requested": False,
    "logs": [],
    "clients": [],  # [{'name','client','authorized'}]
    "stats": {"total_targets": 0, "attempted": 0, "approved": 0, "subscribe_progress": 0, "check_progress": 0},
    "results": {},  # results[target][account] = True/False
    "manager_log_ws": set(),
    "manager_status_ws": set(),
    "scheduled_check_task": None,
    "cooldowns": {},  # name -> unix_timestamp
    "check_task": None,
    "unsubscribe_task": None,
    "accounts_meta": {},  # name -> {"last_action": "..."}
    "unsubscribe_pending": {},
    # позиции для каждого аккаунта в каждой фазе
    "positions_subscribe": {},   # name -> idx
    "positions_check": {},       # name -> idx
    "positions_unsubscribe": {}, # name -> idx
    # общая задержка между действиями (мин/макс)
    "_action_delay_min": 5.0,
    "_action_delay_max": 60.0,
    # синхронизатор для доступа к results/good_targets
    "_lock": asyncio.Lock(),
    # self-ping task и стоп-событие
    "_self_ping_task": None,
    "_self_ping_stop_event": None,
}
# по умолчанию включаем pause_event
state["pause_event"].set()

_last_pause_state = {"is_paused": False}

# -------------------------
# Utility: logging / broadcast
# -------------------------
def append_log(msg: str, force_send: bool = False):
    """Добавляет строку в лог, сохраняет на диск и отправляет всем ws логам."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    if state["logs"] and state["logs"][-1] == line and not force_send:
        return
    state["logs"].append(line)
    if len(state["logs"]) > LOG_MAX_LINES:
        state["logs"] = state["logs"][-LOG_MAX_LINES:]
    try:
        with open(LOG_FILE, "w", encoding="utf-8") as f:
            f.write("\n".join(state["logs"]))
    except Exception:
        pass
    # отправляем новым подключённым ws
    for ws in list(state["manager_log_ws"]):
        try:
            asyncio.create_task(ws.send_text(line))
        except Exception:
            pass


def broadcast_status():
    """Отправляем status в /ws/status всем подключенным клиентам."""
    payload = {
        "running": bool(state["running_task"] and not state["running_task"].done()),
        "check_running": bool(state.get("check_task") and not state["check_task"].done()),
        "unsubscribe_running": bool(state.get("unsubscribe_task") and not state["unsubscribe_task"].done()),
        "stats": state.get("stats", {}),
        "accounts_meta": state.get("accounts_meta", {}),
        "cooldowns": state.get("cooldowns", {}),
    }
    for ws in list(state["manager_status_ws"]):
        try:
            asyncio.create_task(ws.send_json(payload))
        except Exception:
            pass


# -------------------------
# Helpers: files / env
# -------------------------
def load_targets(path: str = TARGETS_FILE) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} не найден")
    df = pd.read_csv(path, dtype=str).fillna("")
    return df


def load_sessions_from_env() -> List[Dict[str, str]]:
    """Читаем сессии из env. Поддерживаем SESSION_STRING_1/... и SESSIONS (многострочный)."""
    sessions = []
    i = 1
    while True:
        key = f"SESSION_STRING_{i}"
        if not os.getenv(key):
            break
        session_string = os.getenv(key).strip()
        api_id = os.getenv(f"API_ID_{i}")
        api_hash = os.getenv(f"API_HASH_{i}")
        name = os.getenv(f"SESSION_NAME_{i}") or f"acc{i}"
        if not api_id or not api_hash:
            raise ValueError(f"API_ID_{i}/API_HASH_{i} не заданы для {key}")
        sessions.append({"session_string": session_string, "api_id": api_id.strip(), "api_hash": api_hash.strip(), "name": name.strip()})
        i += 1

    sess_block = os.getenv("SESSIONS")
    if sess_block:
        for ln in sess_block.splitlines():
            ln = ln.strip()
            if not ln:
                continue
            parts = [p.strip() for p in ln.split(",")]
            if len(parts) < 3:
                append_log(f"Строка в SESSIONS неверного формата: {ln}")
                continue
            session_string, api_id, api_hash = parts[0], parts[1], parts[2]
            name = parts[3] if len(parts) >= 4 else f"env_acc_{len(sessions)+1}"
            sessions.append({"session_string": session_string, "api_id": api_id, "api_hash": api_hash, "name": name})

    if not sessions:
        ss = os.getenv("SESSION_STRING")
        aid = os.getenv("API_ID")
        ah = os.getenv("API_HASH")
        name = os.getenv("SESSION_NAME") or "account1"
        if ss and aid and ah:
            sessions.append({"session_string": ss.strip(), "api_id": aid.strip(), "api_hash": ah.strip(), "name": name.strip()})

    if not sessions:
        raise FileNotFoundError("Не найдены аккаунты в .env (SESSION_STRING_1/... или SESSIONS или SESSION_STRING)")
    return sessions


# -------------------------
# Telethon: создание клиентов
# -------------------------
async def create_clients(sessions: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    """Создаёт и подключает клиентов; возвращает список объектов {'name','client','authorized'}."""
    clients = []
    for s in sessions:
        name = s.get("name") or s.get("api_id")
        session_string = s["session_string"]
        api_id = int(s["api_id"]) if str(s["api_id"]).isdigit() else s["api_id"]
        api_hash = s["api_hash"]
        append_log(f"Создаём клиент: {name}")
        client = TelegramClient(StringSession(session_string), api_id, api_hash)
        try:
            await client.connect()
            if not await client.is_user_authorized():
                append_log(f"Аккаунт {name} не авторизован.")
                await client.disconnect()
                clients.append({"name": name, "client": client, "authorized": False})
                state["accounts_meta"].setdefault(name, {"last_action": "not authorized"})
                continue
            me = await client.get_me()
            append_log(f"Клиент {name} авторизован как {getattr(me,'username', '')}")
            clients.append({"name": name, "client": client, "authorized": True})
            state["accounts_meta"].setdefault(name, {"last_action": f"connected as {getattr(me,'username', '')}"})
        except Exception as e:
            append_log(f"Ошибка подключения {name}: {e}")
            try:
                await client.disconnect()
            except Exception:
                pass
            clients.append({"name": name, "client": client, "authorized": False})
            state["accounts_meta"].setdefault(name, {"last_action": f"connect error: {e}"})
    return clients


# -------------------------
# Telethon actions: join/leave/check
# -------------------------
async def join_target_with_account(client: TelegramClient, target_key: str) -> Dict[str, Any]:
    try:
        entity = await client.get_entity(target_key)
    except Exception as e:
        return {"ok": False, "info": f"entity error: {e}"}
    try:
        await client(JoinChannelRequest(entity))
        return {"ok": True, "info": "Join отправлен/выполнен"}
    except UserAlreadyParticipantError:
        return {"ok": True, "info": "Уже участник"}
    except ChannelPrivateError as e:
        return {"ok": False, "info": f"Приватный: {e}"}
    except RPCError as e:
        return {"ok": False, "info": f"RPCError: {e}"}
    except Exception as e:
        return {"ok": False, "info": f"Ошибка join: {e}"}


async def leave_target_with_account(client: TelegramClient, target_key: str) -> Dict[str, Any]:
    try:
        entity = await client.get_entity(target_key)
    except Exception as e:
        return {"ok": False, "info": f"entity error: {e}"}
    try:
        await client(LeaveChannelRequest(entity))
        return {"ok": True, "info": "Leave отправлен/выполнен"}
    except Exception as e:
        return {"ok": False, "info": f"Ошибка leave: {e}"}


async def check_membership(client: TelegramClient, target_key: str) -> bool:
    """Проверка членства: iter_participants (fallback)."""
    try:
        entity = await client.get_entity(target_key)
    except Exception as e:
        append_log(f"check_membership: не удалось получить entity для {target_key}: {e}")
        return False
    try:
        me = await client.get_me()
        my_id = getattr(me, "id", None)
    except Exception as e:
        append_log(f"check_membership: не удалось получить current user: {e}")
        return False
    append_log(f"check_membership: iter_participants для {target_key} (ищем id={my_id})")
    try:
        async for participant in client.iter_participants(entity):
            pid = getattr(participant, "id", None)
            if pid == my_id:
                append_log(f"check_membership: найден пользователь (id={my_id}) в {target_key}")
                return True
        append_log(f"check_membership: пользователь (id={my_id}) НЕ найден в {target_key}")
        return False
    except Exception as e:
        append_log(f"check_membership: iter_participants упал для {target_key}: {e}")
        return False


# -------------------------
# parse wait seconds
# -------------------------
def parse_wait_seconds(msg: str) -> int:
    if not msg:
        return 0
    m = re.search(r"A wait of (\d+)\s*seconds? is required", msg)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return 0
    m2 = re.search(r"wait of (\d+)", msg)
    if m2:
        try:
            return int(m2.group(1))
        except Exception:
            return 0
    return 0


# -------------------------
# Сохранение good_targets.csv (дополняет существующий файл)
# -------------------------
async def save_good_targets(path: str = GOOD_TARGETS_FILE):
    """Добавляет цели, где все аккаунты являются участниками, в good_targets.csv (без удаления старых)."""
    async with state["_lock"]:
        new_rows: Set[str] = set()
        account_names = [c["name"] for c in state.get("clients", []) if c.get("authorized")]
        if not account_names:
            try:
                sessions = load_sessions_from_env()
                account_names = [s.get("name") or s.get("api_id") for s in sessions]
            except Exception:
                account_names = []
        # собираем новые цели, где по всем аккаунтам state['results'][t][acc] == True
        for target, per_acc in state.get("results", {}).items():
            if not account_names:
                continue
            ok = True
            for a in account_names:
                if not per_acc.get(a, False):
                    ok = False
                    break
            if ok:
                new_rows.add(target)

        existing: Set[str] = set()
        if os.path.exists(path):
            try:
                df_old = pd.read_csv(path, dtype=str).fillna("")
                if "target" in df_old.columns:
                    existing = set(df_old["target"].astype(str).tolist())
            except Exception:
                existing = set()

        merged = sorted(existing.union(new_rows))
        pd.DataFrame([{"target": t} for t in merged]).to_csv(path, index=False, encoding="utf-8")
        append_log(f"good_targets.csv обновлён ({len(merged)} записей, добавлено {len(merged) - len(existing)} новых).")


# -------------------------
# Notifications (telegram/email)
# -------------------------
async def notify_async(subject: str, body: str, telegram_text: Optional[str] = None):
    if TELEGRAM_NOTIFICATIONS_ENABLED and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            data = {"chat_id": TELEGRAM_CHAT_ID, "text": telegram_text or f"{subject}\n\n{body}"}
            await asyncio.to_thread(requests.post, url, data, timeout=15)
            append_log("Уведомление Telegram отправлено (async).")
        except Exception as e:
            append_log(f"notify_async: ошибка отправки telegram: {e}")
    if EMAIL_NOTIFICATIONS_ENABLED and SMTP_HOST and NOTIFY_EMAIL_TO:
        try:
            await asyncio.to_thread(_send_email_sync, subject, body)
            append_log("Уведомление Email отправлено (async).")
        except Exception as e:
            append_log(f"notify_async: ошибка отправки email: {e}")


def _send_email_sync(subject: str, body: str):
    import smtplib, ssl
    from email.message import EmailMessage
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = NOTIFY_EMAIL_TO
    msg.set_content(body)
    context = ssl.create_default_context()
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
        server.starttls(context=context)
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(msg)


async def notify_task_complete(task_type: str):
    try:
        st = state.get("stats", {})
        subject = f"Задача {task_type} завершена"
        body = f"Task: {task_type}\nStats: {st}\nTime: {datetime.now().isoformat()}"
        await notify_async(subject, body)
    except Exception as e:
        append_log(f"notify_task_complete: {e}")


# -------------------------
# Worker implementations (subscribe / check / unsubscribe)
# Каждая фаза создаёт worker'ы — по одному воркеру на аккаунт.
# Каждый воркер хранит и обновляет свою позицию в state['positions_*'].
# В случае cooldown воркер ждёт окончания (не увеличивая позицию), затем повторяет.
# -------------------------
async def _wait_for_cooldown_or_controls(acc_name: str):
    """Ожидание окончания cooldown для конкретного аккаунта.
    Уважает глобальную pause/stop; возвращает True если завершилось нормально, False если stop_requested.
    """
    while True:
        if state["stop_requested"]:
            return False
        now = time.time()
        until = state.get("cooldowns", {}).get(acc_name, 0)
        if now >= until:
            return True
        # уважать паузу
        if not state["pause_event"].is_set():
            append_log("Задача приостановлена (ожидание снятия паузы)...")
            while not state["pause_event"].is_set():
                if state["stop_requested"]:
                    return False
                await asyncio.sleep(0.5)
            append_log("Пауза снята, продолжаем ожидание cooldown.")
        # sleep небольшой шаг, чтобы проверять stop/pause часто
        to_sleep = min(1.0, max(0.5, until - now))
        await asyncio.sleep(to_sleep)


async def worker_subscribe(account: Dict[str, Any], targets: List[str]):
    """Worker для подписки от имени одного аккаунта."""
    name = account["name"]
    client = account["client"]
    # если позиции не установлены — начать с 0
    pos = state["positions_subscribe"].get(name, 0)
    total = len(targets)
    append_log(f"worker_subscribe: {name} стартует с позиции {pos}/{total}")
    while pos < total:
        if state["stop_requested"]:
            append_log(f"worker_subscribe {name}: stop_requested, выходим.")
            break
        # пауза
        while not state["pause_event"].is_set():
            append_log(f"worker_subscribe {name}: в паузе...")
            await asyncio.sleep(0.5)
            if state["stop_requested"]:
                break
        if state["stop_requested"]:
            break
        # проверяем cooldown
        now = time.time()
        cd = state.get("cooldowns", {}).get(name, 0)
        if now < cd:
            append_log(f"{name} в cooldown ({int(cd - now)}s) — worker будет ждать и затем продолжит с позиции {pos}")
            ok = await _wait_for_cooldown_or_controls(name)
            if not ok:
                break
            continue  # после ожидания повторяем цикл и попробуем тот же pos

        target = targets[pos]
        append_log(f"{name} -> попытка подписки на {target} ({pos+1}/{total})")
        state["accounts_meta"].setdefault(name, {})["last_action"] = f"join->{target}"
        res = await join_target_with_account(client, target)
        state["stats"]["attempted"] = state.get("stats", {}).get("attempted", 0) + 1
        append_log(f"{name}: Результат подписки: {res['info']}")
        # если RPCError с wait — выставляем cooldown и НЕ увеличиваем pos (повторим тот же target позже)
        if isinstance(res.get("info"), str) and res["info"].startswith("RPCError"):
            secs = parse_wait_seconds(res["info"])
            if secs > 0:
                state["cooldowns"][name] = time.time() + secs
                append_log(f"{name} помещён в cooldown на {secs}s из-за RPCError при подписке на {target}")
                # проверяем всё равно членство — возможно уже участник
                try:
                    member = await check_membership(client, target)
                except Exception:
                    member = False
                async with state["_lock"]:
                    state["results"].setdefault(target, {})[name] = member
                if member:
                    append_log(f"{name} уже участник {target}")
                    # если успешно — можно увеличить позицию
                    pos += 1
                    state["positions_subscribe"][name] = pos
                    await save_good_targets()
                else:
                    # не увеличиваем pos — будем повторять после cooldown
                    state["positions_subscribe"][name] = pos
                # уважать паузу/stop while waiting in next loop via cd
            else:
                # неизвестная RPCError — помечаем как не участник и продолжаем
                async with state["_lock"]:
                    state["results"].setdefault(target, {})[name] = False
                state["positions_subscribe"][name] = pos + 1
                pos += 1
        else:
            # обычный путь: проверяем членство (может быть Immediate join или join sent)
            try:
                member = await check_membership(client, target)
            except Exception as e:
                member = False
                append_log(f"{name}: ошибка проверки после join: {e}")
            async with state["_lock"]:
                state["results"].setdefault(target, {})[name] = member
            if member:
                append_log(f"{name} — подтверждён как участник {target}")
            else:
                append_log(f"{name} — не участник (заявка отправлена/ожидание) для {target}")
            # сохраняем good_targets всегда после обновления
            await save_good_targets()
            # продвигаем позицию
            pos += 1
            state["positions_subscribe"][name] = pos

        # обновляем stats/WS
        # пересчитываем approved
        async with state["_lock"]:
            approved = 0
            for per in state["results"].values():
                for v in per.values():
                    if v:
                        approved += 1
            state["stats"]["approved"] = approved
            # subscribe_progress — % среднего пройденного по аккаунтам (оценка)
            # для простоты считаем max progress: средняя позиции / total
            try:
                avg_pos = 0
                accs = [n for n in state["positions_subscribe"].keys()]
                if accs:
                    avg_pos = sum([state["positions_subscribe"].get(a, 0) for a in accs]) / len(accs)
                    state["stats"]["subscribe_progress"] = int((avg_pos / max(1, total)) * 100)
            except Exception:
                pass
        broadcast_status()

        # задержка между действиями аккаунта
        delay = max(0.0, random.uniform(state.get("_action_delay_min", 5.0), state.get("_action_delay_max", 60.0)))
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            append_log(f"worker_subscribe {name}: прерван во время задержки.")
            break

    append_log(f"worker_subscribe {name}: завершился (позиция {pos}/{len(targets)})")
    state["positions_subscribe"][name] = pos


async def worker_check(account: Dict[str, Any], targets: List[str]):
    """Worker проверки членства для одного аккаунта."""
    name = account["name"]
    client = account["client"]
    pos = state["positions_check"].get(name, 0)
    total = len(targets)
    append_log(f"worker_check: {name} стартует с позиции {pos}/{total}")
    while pos < total:
        if state["stop_requested"]:
            append_log(f"worker_check {name}: stop_requested, выходим.")
            break
        while not state["pause_event"].is_set():
            append_log(f"worker_check {name}: в паузе...")
            await asyncio.sleep(0.5)
            if state["stop_requested"]:
                break
        if state["stop_requested"]:
            break
        now = time.time()
        cd = state.get("cooldowns", {}).get(name, 0)
        if now < cd:
            append_log(f"{name} в cooldown ({int(cd-now)}s) — worker_check будет ждать...")
            ok = await _wait_for_cooldown_or_controls(name)
            if not ok:
                break
            continue
        target = targets[pos]
        append_log(f"{name} -> проверка членства в {target} ({pos+1}/{total})")
        state["accounts_meta"].setdefault(name, {})["last_action"] = f"check->{target}"
        try:
            member = await check_membership(client, target)
        except Exception as e:
            append_log(f"{name}: ошибка при check_membership {e}")
            member = False
        async with state["_lock"]:
            state["results"].setdefault(target, {})[name] = member
        if member:
            append_log(f"{name} — участник {target}")
        else:
            append_log(f"{name} — НЕ участник {target}")
        # обновляем good_targets и прогресс
        await save_good_targets()
        async with state["_lock"]:
            # update approved
            approved = 0
            for per in state["results"].values():
                for v in per.values():
                    if v:
                        approved += 1
            state["stats"]["approved"] = approved
            try:
                avg_pos = 0
                accs = [n for n in state["positions_check"].keys()]
                if accs:
                    avg_pos = sum([state["positions_check"].get(a, 0) for a in accs]) / len(accs)
                    state["stats"]["check_progress"] = int((avg_pos / max(1, total)) * 100)
            except Exception:
                pass
        state["positions_check"][name] = pos + 1
        pos += 1
        broadcast_status()
        delay = max(0.0, random.uniform(state.get("_action_delay_min", 5.0), state.get("_action_delay_max", 60.0)))
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            append_log(f"worker_check {name}: прерван во время задержки.")
            break
    append_log(f"worker_check {name}: завершился (позиция {pos}/{len(targets)})")
    state["positions_check"][name] = pos


async def worker_unsubscribe(account: Dict[str, Any], targets: List[str]):
    """Worker для отписки одного аккаунта (resume на позиции после cooldown)."""
    name = account["name"]
    client = account["client"]
    pos = state["positions_unsubscribe"].get(name, 0)
    total = len(targets)
    append_log(f"worker_unsubscribe: {name} стартует с позиции {pos}/{total}")
    while pos < total:
        if state["stop_requested"]:
            append_log(f"worker_unsubscribe {name}: stop_requested, выходим.")
            break
        while not state["pause_event"].is_set():
            append_log(f"worker_unsubscribe {name}: в паузе...")
            await asyncio.sleep(0.5)
            if state["stop_requested"]:
                break
        if state["stop_requested"]:
            break
        now = time.time()
        cd = state.get("cooldowns", {}).get(name, 0)
        if now < cd:
            append_log(f"{name} в cooldown ({int(cd-now)}s) — worker_unsubscribe будет ждать...")
            ok = await _wait_for_cooldown_or_controls(name)
            if not ok:
                break
            continue
        target = targets[pos]
        append_log(f"{name} -> попытка отписки от {target} ({pos+1}/{total})")
        state["accounts_meta"].setdefault(name, {})["last_action"] = f"unsubscribe->{target}"
        res = await leave_target_with_account(client, target)
        append_log(f"{name}: Результат отписки: {res.get('info')}")
        if isinstance(res.get("info"), str) and res["info"].startswith("entity error"):
            secs = parse_wait_seconds(res["info"])
            if secs > 0:
                state["cooldowns"][name] = time.time() + secs
                append_log(f"{name} помещён в cooldown на {secs}s из-за entity error при отписке от {target}.")
                # не увеличиваем позицию — повторим после cooldown
                state["positions_unsubscribe"][name] = pos
                # добавить в pending
                state["unsubscribe_pending"].setdefault(name, []).append(target)
                # loop will wait next iteration
            else:
                append_log(f"{name} получил entity error без явного времени при отписке от {target}. Пропускаем.")
                state["positions_unsubscribe"][name] = pos + 1
                pos += 1
        else:
            # успешная отписка или другое
            state["positions_unsubscribe"][name] = pos + 1
            pos += 1
        # задержка
        delay = max(0.0, random.uniform(state.get("_action_delay_min", 5.0), state.get("_action_delay_max", 60.0)))
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            append_log(f"worker_unsubscribe {name}: прерван во время задержки.")
            break
    append_log(f"worker_unsubscribe {name}: завершился (позиция {pos}/{len(targets)})")
    state["positions_unsubscribe"][name] = pos


# -------------------------
# Фазы: process_subscriptions, run_full_membership_check, process_unsubscribe
# Они готовят список targets, создают клиентов, и запускают worker'ов (по одному на аккаунт),
# затем ждут завершения всех worker'ов.
# -------------------------
async def process_subscriptions(pause_event: asyncio.Event, sessions_list: List[Dict[str, str]],
                                targets_df: pd.DataFrame, subscribe_delay_min: float, subscribe_delay_max: float,
                                membership_check_minutes: int):
    """Организует фазу подписки — запускает worker'ов для каждого аккаунта."""
    append_log("=== Начало задания: Подписаться ===")
    state["stop_requested"] = False
    # targets list
    targets = []
    for _, row in targets_df.iterrows():
        key = row.get("username") or row.get("id") or row.get("title")
        if key:
            targets.append(key)
            state["results"].setdefault(key, {})

    total = len(targets)
    state["stats"].update({"total_targets": total, "attempted": 0, "approved": 0, "subscribe_progress": 0})
    broadcast_status()

    # создаём клиентов
    clients_info = await create_clients(sessions_list)
    clients = [c for c in clients_info if c.get("authorized")]
    state["clients"] = clients
    if not clients:
        append_log("Нет авторизованных аккаунтов. Прерываем фазу подписки.")
        return

    append_log(f"Аккаунты в работе: {', '.join([c['name'] for c in clients])}")

    # создаём пустой good_targets если нет
    if not os.path.exists(GOOD_TARGETS_FILE):
        pd.DataFrame([]).to_csv(GOOD_TARGETS_FILE, index=False, encoding="utf-8")
        append_log("good_targets.csv создан (пустой).")

    # обновляем общие задержки
    state["_action_delay_min"] = float(subscribe_delay_min)
    state["_action_delay_max"] = float(subscribe_delay_max)

    # инициализируем позиции для аккаунтов (если не установлены)
    for c in clients:
        state["positions_subscribe"].setdefault(c["name"], 0)

    # запускаем worker'ы
    tasks = []
    for c in clients:
        t = asyncio.create_task(worker_subscribe(c, targets))
        tasks.append(t)

    # ждём завершения всех worker'ов
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        append_log("process_subscriptions: отменено (CancelledError).")
    except Exception as e:
        append_log(f"process_subscriptions: ошибка gather: {e}")

    append_log("Фаза подписки завершена.")
    # планируем отложенную проверку согласно membership_check_minutes (UI)
    if state.get("scheduled_check_task"):
        try:
            prev = state["scheduled_check_task"]
            if prev and not prev.done():
                prev.cancel()
        except Exception:
            pass
    state["scheduled_check_task"] = asyncio.create_task(_schedule_check_after_minutes(membership_check_minutes))
    append_log(f"Отложенная проверка запланирована через {membership_check_minutes} минут.")
    broadcast_status()
    await notify_task_complete("subscribe")


async def _schedule_check_after_minutes(minutes: int):
    """Вспомогательная функция для отложенной проверки (корректно ресчёт минут)."""
    if minutes <= 0:
        append_log("Отложенная проверка: minutes<=0 — запускаем немедленно.")
        await run_full_membership_check()
        return
    append_log(f"Запланирована проверка членства через {minutes} минут.")
    try:
        await asyncio.sleep(minutes * 60)
    except asyncio.CancelledError:
        append_log("Отложенная проверка была отменена.")
        return
    append_log("Отложенная проверка: время пришло, запускаем run_full_membership_check.")
    await run_full_membership_check()


async def run_full_membership_check(ignore_pause: bool = False):
    """Запускает проверку членства: создаёт worker'ов по аккаунтам и запускает их."""
    append_log("Запущена полная проверка членства (run_full_membership_check).")
    state["stop_requested"] = False

    # создаём клиентов, если их нет в state
    if not state.get("clients"):
        append_log("Нет подключённых клиентов в памяти — создаём клиентов из .env для проверки.")
        try:
            sessions_list = load_sessions_from_env()
        except Exception as e:
            append_log(f"Не удалось загрузить аккаунты из .env для проверки: {e}")
            return
        clients_info = await create_clients(sessions_list)
        clients = [c for c in clients_info if c.get("authorized")]
        state["clients"] = clients
        if not clients:
            append_log("После создания клиентов нет авторизованных аккаунтов — проверка отменена.")
            return
    else:
        clients = state.get("clients")

    # targets
    try:
        df = load_targets()
        targets = []
        for _, row in df.iterrows():
            key = row.get("username") or row.get("id") or row.get("title")
            if key:
                targets.append(key)
                state["results"].setdefault(key, {})
    except Exception as e:
        append_log(f"Ошибка чтения targets.csv: {e}")
        return

    total = len(targets)
    state["stats"]["total_targets"] = total

    # обновляем задержки (оставляем существующие)
    # инициализируем позиции_check
    for c in clients:
        state["positions_check"].setdefault(c["name"], 0)

    # создаём worker'ы
    tasks = []
    for c in clients:
        tasks.append(asyncio.create_task(worker_check(c, targets)))

    # ждём завершения
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        append_log("run_full_membership_check: отменено (CancelledError).")
    except Exception as e:
        append_log(f"run_full_membership_check: ошибка gather: {e}")

    append_log("Полная проверка членства завершена.")
    # обновим good_targets один раз в конце (и уже обновления делались в worker'ах)
    await save_good_targets()
    await notify_task_complete("check")
    broadcast_status()


async def process_unsubscribe(pause_event: asyncio.Event, sessions_list: List[Dict[str, str]], targets_df: pd.DataFrame):
    """Организует массовую отписку: стартует worker'ы для каждого аккаунта."""
    append_log("=== Начало задания: Отписаться (Unsubscribe) ===")
    state["stop_requested"] = False

    targets = []
    for _, row in targets_df.iterrows():
        key = row.get("username") or row.get("id") or row.get("title")
        if key:
            targets.append(key)

    clients_info = await create_clients(sessions_list)
    clients = [c for c in clients_info if c.get("authorized")]
    state["clients"] = clients
    if not clients:
        append_log("Нет авторизованных аккаунтов. Прерываем отписку.")
        return

    append_log(f"Аккаунты в работе (отписка): {', '.join([c['name'] for c in clients])}")

    for c in clients:
        state["unsubscribe_pending"].setdefault(c["name"], [])
        state["positions_unsubscribe"].setdefault(c["name"], 0)

    tasks = []
    for c in clients:
        tasks.append(asyncio.create_task(worker_unsubscribe(c, targets)))

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        append_log("process_unsubscribe: отменено (CancelledError).")
    except Exception as e:
        append_log(f"process_unsubscribe: ошибка gather: {e}")

    # обработка pending (повторные попытки)
    append_log("Первичный проход по targets завершён. Обработка pending...")
    try:
        while True:
            if state["stop_requested"]:
                append_log("Обработка pending прервана (stop_requested).")
                break
            pending_accounts = [name for name, lst in state.get("unsubscribe_pending", {}).items() if lst]
            if not pending_accounts:
                append_log("Нет отложенных задач для обработки — завершаем отписку.")
                break
            made_progress = False
            for name in pending_accounts:
                if state["stop_requested"]:
                    break
                client_entry = next((c for c in state.get("clients", []) if c["name"] == name), None)
                if not client_entry or not client_entry.get("authorized"):
                    append_log(f"{name} — нет клиента (pending) — пропускаем.")
                    continue
                now = time.time()
                cd = state.get("cooldowns", {}).get(name, 0)
                if now < cd:
                    append_log(f"{name} все ещё в cooldown ({int(cd-now)}s) — пропускаем повторную попытку.")
                    continue
                pending_list = state["unsubscribe_pending"].get(name, [])
                if not pending_list:
                    continue
                target = pending_list.pop(0)
                append_log(f"{name} -> повторная попытка отписки от {target}")
                try:
                    res = await leave_target_with_account(client_entry["client"], target)
                    append_log(f"{name}: результат повторной отписки: {res.get('info')}")
                except Exception as e:
                    append_log(f"{name}: исключение при повторной отписке: {e}")
                made_progress = True
                # задержка
                delay = max(0.0, random.uniform(state.get("_action_delay_min", 5.0), state.get("_action_delay_max", 60.0)))
                await asyncio.sleep(delay)
            if not made_progress:
                await asyncio.sleep(2.0)
    except asyncio.CancelledError:
        append_log("process_unsubscribe: cancelled during pending processing.")
    append_log("Задача отписки завершена.")
    broadcast_status()
    await notify_task_complete("unsubscribe")


# -------------------------
# API / UI endpoints (unchanged интерфейсы)
# -------------------------
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    try:
        tcount = len(load_targets())
    except Exception:
        tcount = 0
    try:
        scount = len(load_sessions_from_env())
    except Exception:
        scount = 0
    return templates.TemplateResponse("index.html", {"request": request, "targets_count": tcount, "sessions_count": scount})


@app.post("/api/start")
async def api_start(per_account_delay_min: float = Form(5.0),
                    per_account_delay_max: float = Form(60.0),
                    membership_check_minutes: int = Form(60)):
    """
    Запуск подписки. Значения задержек применяются глобально для всех фаз.
    membership_check_minutes — отложенная проверка в минутах.
    """
    if per_account_delay_min < 0:
        per_account_delay_min = 0.0
    if per_account_delay_max < per_account_delay_min:
        per_account_delay_max = per_account_delay_min
    state["_action_delay_min"] = float(per_account_delay_min)
    state["_action_delay_max"] = float(per_account_delay_max)

    if state["running_task"] and not state["running_task"].done():
        return JSONResponse({"ok": False, "msg": "Задача уже запущена"}, status_code=400)
    try:
        targets_df = load_targets()
    except Exception as e:
        return JSONResponse({"ok": False, "msg": f"Ошибка загрузки targets.csv: {e}"}, status_code=400)
    try:
        sessions = load_sessions_from_env()
    except Exception as e:
        return JSONResponse({"ok": False, "msg": f"Ошибка загрузки аккаунтов: {e}"}, status_code=400)

    state["logs"] = []
    append_log("Новая задача: лог очищен.")
    state["pause_event"].set()
    state["stop_requested"] = False

    state["running_task"] = asyncio.create_task(process_subscriptions(
        pause_event=state["pause_event"],
        sessions_list=sessions,
        targets_df=targets_df,
        subscribe_delay_min=per_account_delay_min,
        subscribe_delay_max=per_account_delay_max,
        membership_check_minutes=int(membership_check_minutes),
    ))
    append_log("Фоновая задача подписки запущена.")
    broadcast_status()
    return {"ok": True, "msg": "Задача подписки запущена"}


@app.post("/api/pause")
async def api_pause():
    if not state["running_task"] or state["running_task"].done():
        return JSONResponse({"ok": False, "msg": "Нет запущенной задачи"}, status_code=400)
    state["pause_event"].clear()
    broadcast_status()
    return {"ok": True}


@app.post("/api/resume")
async def api_resume():
    if not state["running_task"] or state["running_task"].done():
        return JSONResponse({"ok": False, "msg": "Нет запущенной задачи"}, status_code=400)
    state["pause_event"].set()
    broadcast_status()
    return {"ok": True}


@app.post("/api/stop")
async def api_stop():
    if not state["running_task"] or state["running_task"].done():
        return JSONResponse({"ok": False, "msg": "Нет запущенной фоновой подписки"}, status_code=400)
    state["stop_requested"] = True
    state["pause_event"].set()
    append_log("Запрошено завершение фоновой подписки (stop).")
    broadcast_status()
    return {"ok": True}


@app.post("/api/check_now")
async def api_check_now():
    if state.get("check_task") and not state["check_task"].done():
        return {"ok": False, "msg": "Проверка уже запущена"}
    # пересоздаём позиции_check для всех аккаунтов только если их нет — иначе продолжаем с текущих позиций
    try:
        sessions = load_sessions_from_env()
    except Exception:
        sessions = []
    # если clients пусты, will be created in run_full_membership_check
    state["check_task"] = asyncio.create_task(run_full_membership_check(ignore_pause=True))
    append_log("Немедленная проверка запущена в фоне.")
    broadcast_status()
    return {"ok": True, "msg": "Немедленная проверка запущена"}


@app.post("/api/stop_check")
async def api_stop_check():
    t = state.get("check_task")
    if not t:
        return {"ok": False, "msg": "Нет активной проверки"}
    if t.done():
        return {"ok": False, "msg": "Задача проверки уже завершена"}
    state["stop_requested"] = True
    try:
        t.cancel()
        append_log("Запрошена отмена немедленной проверки (cancel).")
    except Exception:
        pass
    state["check_task"] = None
    broadcast_status()
    return {"ok": True}


@app.post("/api/unsubscribe")
async def api_unsubscribe():
    if state.get("unsubscribe_task") and not state["unsubscribe_task"].done():
        return {"ok": False, "msg": "Задача отписки уже запущена"}
    try:
        targets_df = load_targets()
    except Exception as e:
        return JSONResponse({"ok": False, "msg": f"Ошибка загрузки targets.csv: {e}"}, status_code=400)
    try:
        sessions = load_sessions_from_env()
    except Exception as e:
        return JSONResponse({"ok": False, "msg": f"Ошибка загрузки аккаунтов: {e}"}, status_code=400)

    state["stop_requested"] = False
    state["unsubscribe_task"] = asyncio.create_task(process_unsubscribe(
        pause_event=state["pause_event"],
        sessions_list=sessions,
        targets_df=targets_df,
    ))
    append_log("Задача отписки запущена в фоне.")
    broadcast_status()
    return {"ok": True}


@app.post("/api/stop_unsubscribe")
async def api_stop_unsubscribe():
    t = state.get("unsubscribe_task")
    if not t:
        return {"ok": False, "msg": "Нет активной задачи отписки"}
    if t.done():
        return {"ok": False, "msg": "Задача отписки уже завершена"}
    state["stop_requested"] = True
    try:
        t.cancel()
        append_log("Запрошена отмена задачи отписки (cancel).")
    except Exception:
        pass
    state["unsubscribe_task"] = None
    broadcast_status()
    return {"ok": True}


@app.get("/api/logs")
async def api_get_logs():
    return {"logs": state["logs"]}


@app.get("/api/status")
async def api_get_status():
    return {
        "running": bool(state["running_task"] and not state["running_task"].done()),
        "check_running": bool(state.get("check_task") and not state["check_task"].done()),
        "unsubscribe_running": bool(state.get("unsubscribe_task") and not state["unsubscribe_task"].done()),
        "stats": state.get("stats", {})
    }


@app.get("/api/accounts_status")
async def api_accounts_status():
    try:
        sessions = load_sessions_from_env()
        order = [s.get("name") or s.get("api_id") for s in sessions]
    except Exception:
        order = [c["name"] for c in state.get("clients", [])]
    accounts = []
    now = time.time()
    for name in order:
        client_entry = next((c for c in state.get("clients", []) if c["name"] == name), None)
        authorized = bool(client_entry and client_entry.get("authorized"))
        cd_until = state.get("cooldowns", {}).get(name, 0)
        cooldown_remaining = max(0, int(cd_until - now))
        last_action = state.get("accounts_meta", {}).get(name, {}).get("last_action", "")
        accounts.append({"name": name, "authorized": authorized, "cooldown_remaining": cooldown_remaining, "last_action": last_action})
    return {"accounts": accounts}


@app.get("/download/good_targets")
async def download_good_targets():
    if not os.path.exists(GOOD_TARGETS_FILE):
        pd.DataFrame([]).to_csv(GOOD_TARGETS_FILE, index=False, encoding="utf-8")
        append_log("good_targets.csv не найден — создан пустой файл для скачивания.")
    return FileResponse(GOOD_TARGETS_FILE, media_type="text/csv", filename="good_targets.csv")


@app.post("/api/clear_good_targets")
async def api_clear_good_targets(confirm: bool = Form(False)):
    try:
        pd.DataFrame([]).to_csv(GOOD_TARGETS_FILE, index=False, encoding="utf-8")
        append_log("good_targets.csv очищен по запросу пользователя.")
        return {"ok": True}
    except Exception as e:
        return JSONResponse({"ok": False, "msg": f"Ошибка очистки: {e}"}, status_code=500)


@app.get("/api/progress")
async def api_progress():
    try:
        df = load_targets()
        targets = []
        for _, row in df.iterrows():
            key = row.get("username") or row.get("id") or row.get("title")
            targets.append({"key": key, "title": row.get("title", "")})
    except Exception:
        targets = []
    try:
        sessions = load_sessions_from_env()
        account_names = [s.get("name") or s.get("api_id") for s in sessions]
    except Exception:
        account_names = [c["name"] for c in state.get("clients", [])] or []
    results = state.get("results", {})
    return {"accounts": account_names, "targets": targets, "results": results, "stats": state.get("stats", {})}


# -------------------------
# WebSocket endpoints (logs/status)
# -------------------------
@app.websocket("/ws/logs")
async def websocket_logs(ws: WebSocket):
    await ws.accept()
    state["manager_log_ws"].add(ws)
    try:
        try:
            await ws.send_text(json.dumps({"init": True, "logs": state["logs"]}))
        except Exception:
            pass
        while True:
            try:
                _ = await ws.receive_text()
                await asyncio.sleep(0.01)
            except WebSocketDisconnect:
                break
            except Exception:
                break
    finally:
        state["manager_log_ws"].discard(ws)


@app.websocket("/ws/status")
async def websocket_status(ws: WebSocket):
    await ws.accept()
    state["manager_status_ws"].add(ws)
    try:
        try:
            await ws.send_json({"init": True, "running": bool(state["running_task"] and not state["running_task"].done()), "stats": state.get("stats", {}), "accounts_meta": state.get("accounts_meta", {})})
        except Exception:
            pass
        while True:
            try:
                await asyncio.sleep(2.0)
                payload = {
                    "running": bool(state["running_task"] and not state["running_task"].done()),
                    "check_running": bool(state.get("check_task") and not state["check_task"].done()),
                    "unsubscribe_running": bool(state.get("unsubscribe_task") and not state["unsubscribe_task"].done()),
                    "stats": state.get("stats", {}),
                    "accounts_meta": state.get("accounts_meta", {}),
                    "cooldowns": state.get("cooldowns", {}),
                }
                await ws.send_json(payload)
            except WebSocketDisconnect:
                break
            except Exception:
                await asyncio.sleep(1.0)
                continue
    finally:
        state["manager_status_ws"].discard(ws)


# -------------------------
# Health & logs export
# -------------------------
@app.get("/health")
async def health_check():
    try:
        return JSONResponse({"ok": True, "running": bool(state["running_task"] and not state["running_task"].done())})
    except Exception:
        return JSONResponse({"ok": True})


@app.get("/api/export_logs")
async def api_export_logs():
    try:
        with open(LOG_FILE, "w", encoding="utf-8") as f:
            f.write("\n".join(state.get("logs", [])))
    except Exception:
        pass
    if os.path.exists(LOG_FILE):
        return FileResponse(LOG_FILE, media_type="text/plain", filename="logs.txt")
    else:
        return JSONResponse({"ok": False, "msg": "Лог файл не найден"}, status_code=404)


# -------------------------
# Self-ping: реализация и интеграция в lifecycle
# -------------------------
def read_self_ping_config():
    """Читает конфигурацию self-ping из переменных окружения."""
    enabled = os.getenv("SELF_PING_ENABLED", "false").lower() in ("1", "true", "yes")
    url = os.getenv("SELF_PING_URL", "") or None
    try:
        interval = int(os.getenv("SELF_PING_INTERVAL", "300"))
    except Exception:
        interval = 300
    return enabled, url, max(1, interval)


async def _self_ping_once(url: str, timeout: int = 10) -> Any:
    """Выполнить один GET-запрос к url в thread pool и вернуть (status, text_or_error)."""
    def _req():
        try:
            r = requests.get(url, timeout=timeout)
            return (r.status_code, r.text[:400] if r.text else "")
        except Exception as e:
            return ("error", str(e))
    return await asyncio.to_thread(_req)


async def self_ping_loop(url: str, interval: int, stop_event: asyncio.Event, logger: Optional[Callable[[str], None]] = None):
    """
    Асинхронный цикл pингования самого себя.
    stop_event — asyncio.Event, при set() цикл завершается.
    logger — функция для логов (append_log).
    """
    if logger is None:
        def logger(x): print(x)
    logger(f"Self-ping loop стартует: url={url}, interval={interval}s")
    # небольшой jitter перед стартом
    await asyncio.sleep(min(interval, 1.0))
    while not stop_event.is_set():
        start = time.time()
        try:
            status, body = await _self_ping_once(url, timeout=10)
            logger(f"HTTP Request: GET {url} -> {status}")
        except Exception as e:
            logger(f"Self-ping exception: {e}")
        elapsed = time.time() - start
        to_wait = max(0, interval - elapsed)
        # ждём мелкими шагами, чтобы можно было прервать быстрее
        waited = 0.0
        while waited < to_wait and not stop_event.is_set():
            step = min(1.0, to_wait - waited)
            await asyncio.sleep(step)
            waited += step
    logger("Self-ping loop остановлен по stop_event.")


@app.on_event("startup")
async def startup_event():
    """При старте приложения — запускаем self-ping при необходимости."""
    append_log("Startup: приложение запустилось, проверяем self-ping конфигурацию...")
    enabled, url, interval = read_self_ping_config()
    if enabled and url:
        stop_ev = asyncio.Event()
        state["_self_ping_stop_event"] = stop_ev
        state["_self_ping_task"] = asyncio.create_task(self_ping_loop(url, interval, stop_ev, append_log))
        append_log(f"Self-ping запущен: {url} каждые {interval}s")
    else:
        append_log("Self-ping отключён (SELF_PING_ENABLED=false или SELF_PING_URL не задан).")


# -------------------------
# Shutdown: корректно останавливаем self-ping и отключаем telethon-клиентов.
# -------------------------
@app.on_event("shutdown")
async def shutdown_event():
    append_log("Shutdown: останавливаем self-ping (если запущен) и отключаем клиентов...")
    # останов self-ping
    try:
        sp_stop = state.get("_self_ping_stop_event")
        sp_task = state.get("_self_ping_task")
        if sp_stop and isinstance(sp_stop, asyncio.Event):
            sp_stop.set()
            append_log("Self-ping stop event установлен.")
        if sp_task and not sp_task.done():
            try:
                sp_task.cancel()
            except Exception:
                pass
            append_log("Self-ping таск отменён.")
    except Exception as e:
        append_log(f"Ошибка при остановке self-ping: {e}")

    # отключаем telethon клиентов
    for c in state.get("clients", []):
        client = c.get("client")
        try:
            if client and client.is_connected():
                await client.disconnect()
        except Exception:
            pass
    append_log("Shutdown завершён.")
