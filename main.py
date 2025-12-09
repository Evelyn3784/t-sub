# main.py
"""
Основной модуль FastAPI + Telethon.
Обновлённая версия с:
- поддержкой загрузки .session файлов и использования их копий (чтобы избежать sqlite locked),
- автоматическим реконнектом аккаунта при ошибке "Cannot send requests while disconnected",
- опцией включения/отключения отложенной проверки (membership_check_enabled),
- self-ping (автопинг) для render,
- WebSocket push для логов и статуса,
- сохранением позиций для resume (subscribe/check/unsubscribe).
Комментарии на русском языке.
"""

import asyncio
import os
import re
import time
import random
import json
import shutil
import uuid
import sqlite3
from datetime import datetime
from typing import List, Dict, Any, Optional, Set, Callable

import requests
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, Form, UploadFile, File
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

# Папка где будут храниться загруженные .session файлы
SESSIONS_UPLOAD_DIR = os.path.join(DATA_DIR, "sessions")
if not os.path.exists(SESSIONS_UPLOAD_DIR):
    os.makedirs(SESSIONS_UPLOAD_DIR, exist_ok=True)

# Мета-файл для загруженных сессий: список объектов {"filename","api_id","api_hash","name"}
SESSIONS_META_FILE = os.path.join(DATA_DIR, "sessions_meta.json")

TARGETS_FILE = os.path.join(DATA_DIR, "targets.csv")
GOOD_TARGETS_FILE = os.path.join(DATA_DIR, "good_targets.csv")
LOG_FILE = os.path.join(DATA_DIR, "logs.txt")
LOG_MAX_LINES = 1000

# Self-ping (SELF_PING_ENABLED, SELF_PING_URL, SELF_PING_INTERVAL)
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
    "clients": [],  # [{'name','client','authorized','session_copy'}]
    "stats": {"total_targets": 0, "attempted": 0, "approved": 0, "subscribe_progress": 0, "check_progress": 0},
    "results": {},  # results[target][account] = True/False
    "manager_log_ws": set(),
    "manager_status_ws": set(),
    "scheduled_check_task": None,
    "cooldowns": {},  # name -> unix_timestamp
    "check_task": None,
    "unsubscribe_task": None,
    "accounts_meta": {},  # name -> {"last_action": "..." }
    "unsubscribe_pending": {},
    # позиции для каждого аккаунта в каждой фазе
    "positions_subscribe": {},   # name -> idx
    "positions_check": {},       # name -> idx
    "positions_unsubscribe": {}, # name -> idx
    # общая задержка между действиями (min/max)
    "_action_delay_min": 5.0,
    "_action_delay_max": 60.0,
    "_lock": asyncio.Lock(),
    # self-ping task and stop event
    "_self_ping_task": None,
    "_self_ping_stop_event": None,
    # reconnect states
    "needs_reconnect": {},   # name -> True
    "reconnect_tasks": {},   # name -> task
}
# по умолчанию включаем pause_event (не в паузе)
state["pause_event"].set()

# -------------------------
# Utility: logging / broadcast
# -------------------------
def append_log(msg: str, force_send: bool = False):
    """Добавляет строку в лог, сохраняет на диск и отправляет всем ws логам."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    # избегаем повторов подряд
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
    # отправляем всем ws логам
    for ws in list(state["manager_log_ws"]):
        try:
            asyncio.create_task(ws.send_text(line))
        except Exception:
            pass

def broadcast_status():
    """Отправляем status в /ws/status всем подключенным клиентам."""
    payload = {
        "running": bool(state["running_task"] and not state["running_task"].done()),
        "check_running": bool(state.get("check_task") and not state.get("check_task").done()),
        "unsubscribe_running": bool(state.get("unsubscribe_task") and not state.get("unsubscribe_task").done()),
        "stats": state.get("stats", {}),
        "accounts_meta": state.get("accounts_meta", {}),
        "cooldowns": state.get("cooldowns", {}),
        "needs_reconnect": state.get("needs_reconnect", {}),
    }
    for ws in list(state["manager_status_ws"]):
        try:
            asyncio.create_task(ws.send_json(payload))
        except Exception:
            pass

# -------------------------
# Reconnect helpers
# -------------------------
async def start_reconnect_for(name: str, client: TelegramClient):
    """Запускает фоновую задачу по переподключению клиента (если ещё не запущена)."""
    if not name:
        return
    if state.get("reconnect_tasks", {}).get(name):
        return

    async def _reconnect_loop():
        append_log(f"Реконнект: начинаем попытки переподключения для {name}")
        backoff = 1
        while True:
            if state.get("stop_requested"):
                append_log(f"Реконнект {name}: отменён (stop_requested)")
                break
            try:
                await client.connect()
                await asyncio.sleep(0.3)
                try:
                    ok = await client.is_user_authorized()
                except Exception as e_author:
                    append_log(f"Реконнект {name}: is_user_authorized упал: {e_author}")
                    ok = False
                if ok:
                    append_log(f"Реконнект {name}: успешно подключён и авторизован.")
                    # обновляем запись о клиенте в state
                    for c in state.get("clients", []):
                        if c.get("name") == name:
                            c["client"] = client
                            c["authorized"] = True
                            break
                    state.get("needs_reconnect", {}).pop(name, None)
                    break
                else:
                    append_log(f"Реконнект {name}: клиент подключился, но не авторизован. Ожидаем следующей попытки.")
            except Exception as e:
                append_log(f"Реконнект {name}: ошибка при connect: {e}")
            await asyncio.sleep(min(60, backoff))
            backoff = min(60, backoff * 2)

        try:
            state.get("reconnect_tasks", {}).pop(name, None)
        except Exception:
            pass

    task = asyncio.create_task(_reconnect_loop())
    state.setdefault("reconnect_tasks", {})[name] = task

async def _wait_for_reauth_or_controls(acc_name: str):
    """Ожидание восстановления авторизации для аккаунта. Возвращает True если восстановлено и можно продолжать."""
    while True:
        if state.get("stop_requested"):
            return False
        # уважать паузу
        if not state["pause_event"].is_set():
            while not state["pause_event"].is_set():
                if state.get("stop_requested"):
                    return False
                await asyncio.sleep(0.5)
        client_entry = next((c for c in state.get("clients", []) if c.get("name") == acc_name), None)
        if client_entry and client_entry.get("authorized") and client_entry.get("client"):
            try:
                cl = client_entry.get("client")
                try:
                    if await cl.is_user_authorized():
                        return True
                except Exception:
                    pass
            except Exception:
                pass
        await asyncio.sleep(2.0)

# -------------------------
# Helpers: files / env / sessions meta
# -------------------------
def load_targets(path: str = TARGETS_FILE) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} не найден")
    df = pd.read_csv(path, dtype=str).fillna("")
    return df

def _read_sessions_meta() -> List[Dict[str, str]]:
    if not os.path.exists(SESSIONS_META_FILE):
        return []
    try:
        with open(SESSIONS_META_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
            return []
    except Exception:
        return []

def _write_sessions_meta(lst: List[Dict[str, str]]):
    try:
        with open(SESSIONS_META_FILE, "w", encoding="utf-8") as f:
            json.dump(lst, f, ensure_ascii=False, indent=2)
    except Exception as e:
        append_log(f"Ошибка записи sessions_meta.json: {e}")

def load_sessions_from_env() -> List[Dict[str, str]]:
    """
    Читаем сессии из env и из загруженных .session:
    Возвращаем список элементов с полями:
      - либо {"session_string": "...", "api_id":"...", "api_hash":"...", "name":"..."}
      - либо {"session_file": "/full/path/to/file.session", "api_id":"...", "api_hash":"...", "name":"..."}
    """
    sessions = []
    # 1) SESSION_STRING_1, API_ID_1, API_HASH_1, SESSION_NAME_1 ...
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

    # 2) блок SESSIONS — многострочный, каждая строка: session_string,api_id,api_hash[,name]
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

    # 3) единичные переменные SESSION_STRING, API_ID, API_HASH
    if not sessions:
        ss = os.getenv("SESSION_STRING")
        aid = os.getenv("API_ID")
        ah = os.getenv("API_HASH")
        name = os.getenv("SESSION_NAME") or "account1"
        if ss and aid and ah:
            sessions.append({"session_string": ss.strip(), "api_id": aid.strip(), "api_hash": ah.strip(), "name": name.strip()})

    # 4) Загруженные .session файлы (sessions_meta.json)
    meta = _read_sessions_meta()
    for ent in meta:
        fname = ent.get("filename")
        api_id = ent.get("api_id")
        api_hash = ent.get("api_hash")
        name = ent.get("name") or (os.path.splitext(os.path.basename(fname))[0] if fname else f"uploaded_{len(sessions)+1}")
        if not fname or not api_id or not api_hash:
            append_log(f"Неполная мета-запись сессии: {ent} — пропускаем")
            continue
        if not os.path.isabs(fname):
            full = os.path.join(SESSIONS_UPLOAD_DIR, fname)
        else:
            full = fname
        if not os.path.exists(full):
            append_log(f"Файл сессии {full} не найден — пропускаем")
            continue
        sessions.append({"session_file": full, "api_id": str(api_id), "api_hash": str(api_hash), "name": name})

    if not sessions:
        raise FileNotFoundError("Не найдены аккаунты: ни SESSION_STRING в env, ни загруженных .session (sessions_meta.json пуст).")
    return sessions

# -------------------------
# Utility: create unique session copy (to avoid sqlite locked)
# -------------------------
def create_unique_session_copy(original_path: str) -> str:
    """
    Создаёт уникальную рабочую копию .session файла для текущего запуска.
    Возвращает путь к копии.
    """
    try:
        if not os.path.exists(original_path):
            raise FileNotFoundError(original_path)
        base = os.path.basename(original_path)
        name, ext = os.path.splitext(base)
        unique = f"{name}_{uuid.uuid4().hex}{ext or '.session'}"
        dest = os.path.join(SESSIONS_UPLOAD_DIR, unique)
        shutil.copy2(original_path, dest)
        if os.path.exists(dest):
            append_log(f"Создана копия сессии для запуска: {os.path.basename(dest)}")
            return dest
        else:
            raise IOError("Не удалось создать копию сессии")
    except Exception as e:
        append_log(f"create_unique_session_copy: не удалось скопировать {original_path}: {e}")
        raise

# -------------------------
# Telethon: создание клиентов
# -------------------------
async def create_clients(sessions: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    """
    Создаёт и подключает клиентов; возвращает список объектов {'name','client','authorized','session_copy'}.
    Поддерживает записи с 'session_string' и с 'session_file'.
    """
    clients = []
    for s in sessions:
        name = s.get("name") or s.get("api_id")
        session_string = s.get("session_string")
        session_file = s.get("session_file")
        api_id = s.get("api_id")
        api_hash = s.get("api_hash")
        if api_id is None or api_hash is None:
            append_log(f"create_clients: пропущен аккаунт {name} — нет api_id/api_hash")
            continue
        try:
            api_id_val = int(api_id) if str(api_id).isdigit() else api_id
        except Exception:
            api_id_val = api_id
        append_log(f"Создаём клиент: {name}")
        client = None
        session_copy_path = None
        try:
            if session_file:
                try:
                    session_copy_path = create_unique_session_copy(session_file)
                except Exception as e:
                    append_log(f"Не удалось скопировать сессию {session_file} для {name}: {e}")
                    session_copy_path = session_file  # fallback, риск conflict
                client = TelegramClient(session_copy_path, api_id_val, api_hash)
            else:
                client = TelegramClient(StringSession(session_string), api_id_val, api_hash)
        except Exception as e:
            append_log(f"Ошибка создания TelegramClient для {name}: {e}")
            clients.append({"name": name, "client": None, "authorized": False, "session_copy": session_copy_path})
            state["accounts_meta"].setdefault(name, {"last_action": f"create client error: {e}"})
            continue

        try:
            await client.connect()
            if not await client.is_user_authorized():
                append_log(f"Аккаунт {name} не авторизован.")
                try:
                    await client.disconnect()
                except Exception:
                    pass
                clients.append({"name": name, "client": client, "authorized": False, "session_copy": session_copy_path})
                state["accounts_meta"].setdefault(name, {"last_action": "not authorized"})
                continue
            me = await client.get_me()
            uname = getattr(me, 'username', None)
            append_log(f"Клиент {name} авторизован как {uname}")
            clients.append({"name": name, "client": client, "authorized": True, "session_copy": session_copy_path})
            state["accounts_meta"].setdefault(name, {"last_action": f"connected as {uname}"})
        except sqlite3.OperationalError as oe:
            append_log(f"Ошибка подключения {name}: sqlite OperationalError: {oe}")
            try:
                await client.disconnect()
            except Exception:
                pass
            clients.append({"name": name, "client": client, "authorized": False, "session_copy": session_copy_path})
            state["accounts_meta"].setdefault(name, {"last_action": f"sqlite error: {oe}"})
        except Exception as e:
            append_log(f"Ошибка подключения {name}: {e}")
            try:
                await client.disconnect()
            except Exception:
                pass
            clients.append({"name": name, "client": client, "authorized": False, "session_copy": session_copy_path})
            state["accounts_meta"].setdefault(name, {"last_action": f"connect error: {e}"})
    return clients

# -------------------------
# Telethon actions: join/leave/check with robust reconnect handling
# -------------------------
async def ensure_client_connected(client: TelegramClient, name: str, max_retries: int = 3, delay_base: float = 1.0) -> bool:
    """
    Убедиться, что client подключён и авторизован. При sqlite 'database is locked' выполнять retry.
    Возвращает True если клиент подключён и авторизован.
    """
    try:
        if getattr(client, "is_connected", None) and client.is_connected():
            try:
                if await client.is_user_authorized():
                    return True
            except Exception:
                pass
        for attempt in range(1, max_retries + 1):
            try:
                await client.connect()
                await asyncio.sleep(0.2)
                if await client.is_user_authorized():
                    return True
                else:
                    append_log(f"{name}: клиент подключился, но не авторизован.")
                    return False
            except sqlite3.OperationalError as e:
                append_log(f"{name}: sqlite OperationalError при connect: {e} (attempt {attempt})")
                await asyncio.sleep(delay_base * attempt)
                continue
            except Exception as e:
                append_log(f"{name}: попытка connect #{attempt} не удалась: {e}")
                await asyncio.sleep(delay_base * attempt)
                continue
        return False
    except Exception as e:
        append_log(f"{name}: ensure_client_connected исключение: {e}")
        return False

async def join_target_with_account(client: TelegramClient, target_key: str, acc_name: str = "<acc>") -> Dict[str, Any]:
    """Надёжный join с обработкой disconnect и sqlite locked."""
    # Убедимся, что клиент подключён
    ok = await ensure_client_connected(client, acc_name, max_retries=3)
    if not ok:
        return {"ok": False, "info": "entity error: Cannot send requests while disconnected"}
    try:
        entity = await client.get_entity(target_key)
    except Exception as e:
        msg = str(e)
        if "disconnected" in msg.lower() or "cannot send requests" in msg.lower() or isinstance(e, sqlite3.OperationalError):
            append_log(f"{acc_name}: get_entity упал ({e}) — попытаемся reconnect и повторить.")
            try:
                if await ensure_client_connected(client, acc_name, max_retries=2):
                    entity = await client.get_entity(target_key)
                else:
                    return {"ok": False, "info": f"entity error: {e}"}
            except Exception as ee:
                return {"ok": False, "info": f"entity error: {ee}"}
        else:
            return {"ok": False, "info": f"entity error: {e}"}
    try:
        await client(JoinChannelRequest(entity))
        return {"ok": True, "info": "Join отправлен/выполнен"}
    except UserAlreadyParticipantError:
        return {"ok": True, "info": "Уже участник"}
    except ChannelPrivateError as e:
        return {"ok": False, "info": f"Приватный: {e}"}
    except RPCError as e:
        msg = str(e)
        if "disconnected" in msg.lower() or "cannot send requests" in msg.lower():
            append_log(f"{acc_name}: JoinChannelRequest упал ({e}) — попробуем reconnect и retry.")
            if await ensure_client_connected(client, acc_name, max_retries=2):
                try:
                    await client(JoinChannelRequest(entity))
                    return {"ok": True, "info": "Join отправлен/выполнен (после reconnect)"}
                except Exception as e2:
                    return {"ok": False, "info": f"RPCError после повторной попытки: {e2}"}
        return {"ok": False, "info": f"RPCError: {e}"}
    except sqlite3.OperationalError as e:
        append_log(f"{acc_name}: sqlite OperationalError при join: {e}")
        secs = 60
        state["cooldowns"][acc_name] = time.time() + secs
        return {"ok": False, "info": f"entity error: A wait of {secs} seconds is required (caused by sqlite database is locked)"}
    except Exception as e:
        return {"ok": False, "info": f"Ошибка join: {e}"}

async def leave_target_with_account(client: TelegramClient, target_key: str, acc_name: str = "<acc>") -> Dict[str, Any]:
    ok = await ensure_client_connected(client, acc_name, max_retries=3)
    if not ok:
        return {"ok": False, "info": "entity error: Cannot send requests while disconnected"}
    try:
        entity = await client.get_entity(target_key)
    except Exception as e:
        msg = str(e)
        if "disconnected" in msg.lower() or "cannot send requests" in msg.lower() or isinstance(e, sqlite3.OperationalError):
            append_log(f"{acc_name}: get_entity (leave) упал ({e}) — пробуем reconnect и повторить.")
            try:
                if await ensure_client_connected(client, acc_name, max_retries=2):
                    entity = await client.get_entity(target_key)
                else:
                    return {"ok": False, "info": f"entity error: {e}"}
            except Exception as ee:
                return {"ok": False, "info": f"entity error: {ee}"}
        else:
            return {"ok": False, "info": f"entity error: {e}"}
    try:
        await client(LeaveChannelRequest(entity))
        return {"ok": True, "info": "Leave отправлен/выполнен"}
    except sqlite3.OperationalError as e:
        append_log(f"{acc_name}: sqlite OperationalError при leave: {e}")
        secs = 60
        state["cooldowns"][acc_name] = time.time() + secs
        return {"ok": False, "info": f"entity error: A wait of {secs} seconds is required (caused by sqlite database is locked)"}
    except Exception as e:
        msg = str(e)
        if "disconnected" in msg.lower() or "cannot send requests" in msg.lower():
            append_log(f"{acc_name}: LeaveChannelRequest упал ({e}) — пробуем reconnect и retry.")
            if await ensure_client_connected(client, acc_name, max_retries=2):
                try:
                    await client(LeaveChannelRequest(entity))
                    return {"ok": True, "info": "Leave отправлен/выполнен (после reconnect)"}
                except Exception as e2:
                    return {"ok": False, "info": f"Ошибка leave после retry: {e2}"}
        return {"ok": False, "info": f"Ошибка leave: {e}"}

async def check_membership(client: TelegramClient, target_key: str, acc_name: str = "<acc>") -> bool:
    """Надёжная проверка членства с retry при disconnect/sqlite locked."""
    ok = await ensure_client_connected(client, acc_name, max_retries=3)
    if not ok:
        append_log(f"{acc_name}: check_membership — клиент не подключён.")
        return False
    try:
        entity = await client.get_entity(target_key)
    except Exception as e:
        msg = str(e)
        append_log(f"check_membership: не удалось получить entity для {target_key}: {e}")
        if "disconnected" in msg.lower() or "cannot send requests" in msg.lower() or isinstance(e, sqlite3.OperationalError):
            append_log(f"{acc_name}: check_membership — пробуем reconnect и повторный get_entity.")
            if await ensure_client_connected(client, acc_name, max_retries=2):
                try:
                    entity = await client.get_entity(target_key)
                except Exception as ee:
                    append_log(f"{acc_name}: повторный get_entity не удался: {ee}")
                    return False
            else:
                return False
        else:
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
        msg = str(e)
        append_log(f"{acc_name}: check_membership: iter_participants упал для {target_key}: {e}")
        if "disconnected" in msg.lower() or "cannot send requests" in msg.lower() or isinstance(e, sqlite3.OperationalError):
            append_log(f"{acc_name}: check_membership — пробуем reconnect и повторить iter_participants.")
            if await ensure_client_connected(client, acc_name, max_retries=2):
                try:
                    async for participant in client.iter_participants(entity):
                        pid = getattr(participant, "id", None)
                        if pid == my_id:
                            append_log(f"check_membership: найден пользователь после retry (id={my_id}) в {target_key}")
                            return True
                    return False
                except Exception as ee:
                    append_log(f"{acc_name}: check_membership: retry упал: {ee}")
                    return False
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
# -------------------------
async def _wait_for_cooldown_or_controls(acc_name: str) -> bool:
    """Ожидание окончания cooldown для конкретного аккаунта."""
    while True:
        if state["stop_requested"]:
            return False
        now = time.time()
        until = state.get("cooldowns", {}).get(acc_name, 0)
        if now >= until:
            return True
        if not state["pause_event"].is_set():
            append_log("Задача приостановлена (ожидание возобновления)...")
            while not state["pause_event"].is_set():
                if state["stop_requested"]:
                    return False
                await asyncio.sleep(0.5)
            append_log("Пауза снята, продолжаем ожидание cooldown.")
        to_sleep = min(1.0, max(0.5, until - now))
        await asyncio.sleep(to_sleep)

# worker_subscribe with reconnect handling
async def worker_subscribe(account: Dict[str, Any], targets: List[str]):
    """Worker для подписки от имени одного аккаунта."""
    name = account["name"]
    client = account["client"]
    pos = state["positions_subscribe"].get(name, 0)
    total = len(targets)
    append_log(f"worker_subscribe: {name} стартует с позиции {pos}/{total}")
    while pos < total:
        if state["stop_requested"]:
            append_log(f"worker_subscribe {name}: stop_requested, выходим.")
            break
        while not state["pause_event"].is_set():
            append_log(f"worker_subscribe {name}: в паузе...")
            await asyncio.sleep(0.5)
            if state["stop_requested"]:
                break
        if state["stop_requested"]:
            break
        now = time.time()
        cd = state.get("cooldowns", {}).get(name, 0)
        if now < cd:
            append_log(f"{name} в cooldown ({int(cd - now)}s) — worker будет ждать и затем продолжит с позиции {pos}")
            ok = await _wait_for_cooldown_or_controls(name)
            if not ok:
                break
            continue

        target = targets[pos]
        append_log(f"{name} -> попытка подписки на {target} ({pos+1}/{total})")
        state["accounts_meta"].setdefault(name, {})["last_action"] = f"join->{target}"
        res = await join_target_with_account(client, target, name)
        state["stats"]["attempted"] = state.get("stats", {}).get("attempted", 0) + 1
        append_log(f"{name}: Результат подписки: {res.get('info')}")

        # Обработать disconnected -> запуск реконнекта и ожидание
        info_lower = str(res.get("info", "")).lower()
        if "cannot send requests while disconnected" in info_lower or "cannot send requests" in info_lower:
            append_log(f"{name}: обнаружено 'Cannot send requests while disconnected' — запускаем реконнект и приостанавливаем работу аккаунта.")
            state.setdefault("needs_reconnect", {})[name] = True
            for ccie in state.get("clients", []):
                if ccie.get("name") == name:
                    ccie["authorized"] = False
                    break
            try:
                await start_reconnect_for(name, client)
            except Exception:
                pass
            ok_reauth = await _wait_for_reauth_or_controls(name)
            if not ok_reauth:
                append_log(f"{name}: реконнект не завершился — прерываем worker.")
                break
            append_log(f"{name}: реконнект завершён — продолжаем с позиции {pos}.")
            state["positions_subscribe"][name] = pos
            continue

        # RPCError с wait -> cooldown
        if isinstance(res.get("info"), str) and res["info"].startswith("RPCError"):
            secs = parse_wait_seconds(res["info"])
            if secs > 0:
                state["cooldowns"][name] = time.time() + secs
                append_log(f"{name} помещён в cooldown на {secs}s из-за RPCError при подписке на {target}")
                try:
                    member = await check_membership(client, target, name)
                except Exception:
                    member = False
                async with state["_lock"]:
                    state["results"].setdefault(target, {})[name] = member
                if member:
                    append_log(f"{name} уже участник {target}")
                    pos += 1
                    state["positions_subscribe"][name] = pos
                    await save_good_targets()
                else:
                    state["positions_subscribe"][name] = pos
            else:
                async with state["_lock"]:
                    state["results"].setdefault(target, {})[name] = False
                state["positions_subscribe"][name] = pos + 1
                pos += 1
        else:
            try:
                member = await check_membership(client, target, name)
            except Exception as e:
                member = False
                append_log(f"{name}: ошибка проверки после join: {e}")
            async with state["_lock"]:
                state["results"].setdefault(target, {})[name] = member
            if member:
                append_log(f"{name} — подтверждён как участник {target}")
            else:
                append_log(f"{name} — не участник (заявка отправлена/ожидание) для {target}")
            await save_good_targets()
            pos += 1
            state["positions_subscribe"][name] = pos

        # обновляем статистику
        async with state["_lock"]:
            approved = 0
            for per in state["results"].values():
                for v in per.values():
                    if v:
                        approved += 1
            state["stats"]["approved"] = approved
            try:
                accs = [n for n in state["positions_subscribe"].keys()]
                if accs:
                    avg_pos = sum([state["positions_subscribe"].get(a, 0) for a in accs]) / len(accs)
                    state["stats"]["subscribe_progress"] = int((avg_pos / max(1, total)) * 100)
            except Exception:
                pass
        broadcast_status()

        delay = max(0.0, random.uniform(state.get("_action_delay_min", 5.0), state.get("_action_delay_max", 60.0)))
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            append_log(f"worker_subscribe {name}: прерван во время задержки.")
            break

    append_log(f"worker_subscribe {name}: завершился (позиция {pos}/{len(targets)})")
    state["positions_subscribe"][name] = pos

# worker_check
async def worker_check(account: Dict[str, Any], targets: List[str]):
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
            member = await check_membership(client, target, name)
        except Exception as e:
            append_log(f"{name}: ошибка при check_membership {e}")
            member = False
        async with state["_lock"]:
            state["results"].setdefault(target, {})[name] = member
        if member:
            append_log(f"{name} — участник {target}")
        else:
            append_log(f"{name} — НЕ участник {target}")
        await save_good_targets()
        async with state["_lock"]:
            approved = 0
            for per in state["results"].values():
                for v in per.values():
                    if v:
                        approved += 1
            state["stats"]["approved"] = approved
            try:
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

# worker_unsubscribe
async def worker_unsubscribe(account: Dict[str, Any], targets: List[str]):
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
        res = await leave_target_with_account(client, target, name)
        append_log(f"{name}: Результат отписки: {res.get('info')}")
        info_lower = str(res.get("info", "")).lower()
        if "cannot send requests while disconnected" in info_lower or "cannot send requests" in info_lower:
            append_log(f"{name}: при отписке получен disconnected — запускаем реконнект.")
            state.setdefault("needs_reconnect", {})[name] = True
            for ccie in state.get("clients", []):
                if ccie.get("name") == name:
                    ccie["authorized"] = False
                    break
            try:
                await start_reconnect_for(name, client)
            except Exception:
                pass
            ok_reauth = await _wait_for_reauth_or_controls(name)
            if not ok_reauth:
                append_log(f"{name}: реконнект не завершился — прерываем unsubscribe worker.")
                break
            append_log(f"{name}: реконнект завершён — продолжаем отписку с позиции {pos}.")
            state["positions_unsubscribe"][name] = pos
            continue
        if isinstance(res.get("info"), str) and res["info"].startswith("entity error"):
            secs = parse_wait_seconds(res.get("info"))
            if secs > 0:
                state["cooldowns"][name] = time.time() + secs
                append_log(f"{name} помещён в cooldown на {secs}s из-за entity error при отписке от {target}.")
                state["positions_unsubscribe"][name] = pos
            else:
                state["positions_unsubscribe"][name] = pos + 1
                pos += 1
        else:
            state["positions_unsubscribe"][name] = pos + 1
            pos += 1
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
# -------------------------
async def process_subscriptions(pause_event: asyncio.Event, sessions_list: List[Dict[str, str]],
                                targets_df: pd.DataFrame, subscribe_delay_min: float, subscribe_delay_max: float,
                                membership_check_minutes: int, membership_check_enabled: bool = True):
    """Организует фазу подписки — запускает worker'ов для каждого аккаунта."""
    append_log("=== Начало задания: Подписаться ===")
    state["stop_requested"] = False
    targets = []
    for _, row in targets_df.iterrows():
        key = row.get("username") or row.get("id") or row.get("title")
        if key:
            targets.append(key)
            state["results"].setdefault(key, {})
    total = len(targets)
    state["stats"].update({"total_targets": total, "attempted": 0, "approved": 0, "subscribe_progress": 0})
    broadcast_status()

    clients_info = await create_clients(sessions_list)
    clients = [c for c in clients_info if c.get("authorized")]
    state["clients"] = clients
    if not clients:
        append_log("Нет авторизованных аккаунтов. Прерываем фазу подписки.")
        return

    append_log(f"Аккаунты в работе: {', '.join([c['name'] for c in clients])}")

    if not os.path.exists(GOOD_TARGETS_FILE):
        pd.DataFrame([]).to_csv(GOOD_TARGETS_FILE, index=False, encoding="utf-8")
        append_log("good_targets.csv создан (пустой).")

    state["_action_delay_min"] = float(subscribe_delay_min)
    state["_action_delay_max"] = float(subscribe_delay_max)

    for c in clients:
        state["positions_subscribe"].setdefault(c["name"], 0)

    tasks = []
    for c in clients:
        t = asyncio.create_task(worker_subscribe(c, targets))
        tasks.append(t)

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        append_log("process_subscriptions: отменено (CancelledError).")
    except Exception as e:
        append_log(f"process_subscriptions: ошибка gather: {e}")

    append_log("Фаза подписки завершена.")
    # планируем отложенную проверку только если включено
    if membership_check_enabled:
        if state.get("scheduled_check_task"):
            try:
                prev = state["scheduled_check_task"]
                if prev and not prev.done():
                    prev.cancel()
            except Exception:
                pass
        state["scheduled_check_task"] = asyncio.create_task(_schedule_check_after_minutes(membership_check_minutes))
        append_log(f"Отложенная проверка запланирована через {membership_check_minutes} минут.")
    else:
        append_log("Отложенная проверка отключена (membership_check_enabled=False).")
    broadcast_status()
    if NOTIFY_ON_SUBSCRIBE_COMPLETE:
        await notify_task_complete("subscribe")

async def _schedule_check_after_minutes(minutes: int):
    """Вспомогательная функция для отложенной проверки."""
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
    append_log("Запущена полная проверка членства (run_full_membership_check).")
    state["stop_requested"] = False

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

    for c in clients:
        state["positions_check"].setdefault(c["name"], 0)

    tasks = []
    for c in clients:
        tasks.append(asyncio.create_task(worker_check(c, targets)))

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        append_log("run_full_membership_check: отменено (CancelledError).")
    except Exception as e:
        append_log(f"run_full_membership_check: ошибка gather: {e}")

    append_log("Полная проверка членства завершена.")
    await save_good_targets()
    if NOTIFY_ON_CHECK_COMPLETE:
        await notify_task_complete("check")
    broadcast_status()

async def process_unsubscribe(pause_event: asyncio.Event, sessions_list: List[Dict[str, str]], targets_df: pd.DataFrame):
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
                    res = await leave_target_with_account(client_entry["client"], target, name)
                    append_log(f"{name}: результат повторной отписки: {res.get('info')}")
                except Exception as e:
                    append_log(f"{name}: исключение при повторной отписке: {e}")
                made_progress = True
                delay = max(0.0, random.uniform(state.get("_action_delay_min", 5.0), state.get("_action_delay_max", 60.0)))
                await asyncio.sleep(delay)
            if not made_progress:
                await asyncio.sleep(2.0)
    except asyncio.CancelledError:
        append_log("process_unsubscribe: cancelled during pending processing.")
    append_log("Задача отписки завершена.")
    if NOTIFY_ON_UNSUBSCRIBE_COMPLETE:
        await notify_task_complete("unsubscribe")
    broadcast_status()

# -------------------------
# API / UI endpoints
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
                    membership_check_minutes: int = Form(60),
                    membership_check_enabled: int = Form(1)):
    """
    Запуск подписки. membership_check_enabled: 1 или 0.
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

    membership_check_enabled_bool = (int(membership_check_enabled) == 1)
    state["running_task"] = asyncio.create_task(process_subscriptions(
        pause_event=state["pause_event"],
        sessions_list=sessions,
        targets_df=targets_df,
        subscribe_delay_min=per_account_delay_min,
        subscribe_delay_max=per_account_delay_max,
        membership_check_minutes=int(membership_check_minutes),
        membership_check_enabled=membership_check_enabled_bool,
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
    if state.get("check_task") and not state.get("check_task").done():
        return {"ok": False, "msg": "Проверка уже запущена"}
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
    if state.get("unsubscribe_task") and not state.get("unsubscribe_task").done():
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
        "check_running": bool(state.get("check_task") and not state.get("check_task").done()),
        "unsubscribe_running": bool(state.get("unsubscribe_task") and not state.get("unsubscribe_task").done()),
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
        needs_reconnect = bool(state.get("needs_reconnect", {}).get(name, False))
        accounts.append({"name": name, "authorized": authorized, "cooldown_remaining": cooldown_remaining, "last_action": last_action, "needs_reconnect": needs_reconnect})
    return {"accounts": accounts}

@app.get("/download/good_targets")
async def download_good_targets():
    if not os.path.exists(GOOD_TARGETS_FILE):
        pd.DataFrame([]).to_csv(GOOD_TARGETS_FILE, index=False, encoding="utf-8")
        append_log("good_targets.csv не найден — создан пустой файл для скачивания.")
    return FileResponse(GOOD_TARGETS_FILE, media_type="text/csv", filename="good_targets.csv")

@app.post("/api/clear_good_targets")
async def api_clear_good_targets(confirm: int = Form(0)):
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
# Endpoints для загрузки/списка/удаления .session (UI)
# -------------------------
@app.post("/api/upload_session")
async def api_upload_session(file: UploadFile = File(...), api_id: str = Form(...), api_hash: str = Form(...), name: str = Form(...)):
    """
    Загрузить .session файл.
    Поля: file (.session), api_id, api_hash, name.
    Сохранит файл в DATA_DIR/sessions/<original_filename> и мета-запись в sessions_meta.json.
    """
    filename = file.filename or "uploaded.session"
    if not filename.endswith(".session"):
        append_log(f"Загрузка файла сессии: предупреждение — файл не имеет расширения .session ({filename}).")
    dest = os.path.join(SESSIONS_UPLOAD_DIR, filename)
    base, ext = os.path.splitext(filename)
    i = 1
    while os.path.exists(dest):
        dest = os.path.join(SESSIONS_UPLOAD_DIR, f"{base}_{i}{ext}")
        i += 1
    try:
        content = await file.read()
        with open(dest, "wb") as f:
            f.write(content)
    except Exception as e:
        append_log(f"Ошибка сохранения файла сессии: {e}")
        return JSONResponse({"ok": False, "msg": f"Ошибка сохранения: {e}"}, status_code=500)
    meta = _read_sessions_meta()
    meta.append({"filename": os.path.basename(dest), "api_id": str(api_id), "api_hash": str(api_hash), "name": name})
    _write_sessions_meta(meta)
    append_log(f"Файл сессии загружен: {os.path.basename(dest)} (name={name})")
    return {"ok": True, "msg": "Файл загружен"}

@app.get("/api/list_uploaded_sessions")
async def api_list_uploaded_sessions():
    meta = _read_sessions_meta()
    # добавим exists флаг
    for m in meta:
        fname = m.get("filename")
        if fname:
            path = os.path.join(SESSIONS_UPLOAD_DIR, fname)
            m["exists"] = os.path.exists(path)
    return {"ok": True, "sessions": meta}

@app.post("/api/delete_uploaded_session")
async def api_delete_uploaded_session(filename: str = Form(...)):
    meta = _read_sessions_meta()
    new_meta = [m for m in meta if os.path.basename(m.get("filename", "")) != os.path.basename(filename)]
    removed = len(meta) - len(new_meta)
    if removed == 0:
        return JSONResponse({"ok": False, "msg": "Не найдена запись в sessions_meta"}, status_code=404)
    _write_sessions_meta(new_meta)
    path = os.path.join(SESSIONS_UPLOAD_DIR, filename)
    try:
        if os.path.exists(path):
            os.remove(path)
            append_log(f"Удалён загруженный файл сессии: {filename}")
    except Exception as e:
        append_log(f"Ошибка удаления файла сессии {filename}: {e}")
    return {"ok": True, "removed": removed}

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
                    "check_running": bool(state.get("check_task") and not state.get("check_task").done()),
                    "unsubscribe_running": bool(state.get("unsubscribe_task") and not state.get("unsubscribe_task").done()),
                    "stats": state.get("stats", {}),
                    "accounts_meta": state.get("accounts_meta", {}),
                    "cooldowns": state.get("cooldowns", {}),
                    "needs_reconnect": state.get("needs_reconnect", {}),
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
# Self-ping
# -------------------------
def read_self_ping_config():
    enabled = os.getenv("SELF_PING_ENABLED", "false").lower() in ("1", "true", "yes")
    url = os.getenv("SELF_PING_URL", "") or None
    try:
        interval = int(os.getenv("SELF_PING_INTERVAL", "300"))
    except Exception:
        interval = 300
    return enabled, url, max(1, interval)

async def _self_ping_once(url: str, timeout: int = 10) -> Any:
    def _req():
        try:
            r = requests.get(url, timeout=timeout)
            return (r.status_code, r.text[:400] if r.text else "")
        except Exception as e:
            return ("error", str(e))
    return await asyncio.to_thread(_req)

async def self_ping_loop(url: str, interval: int, stop_event: asyncio.Event, logger: Optional[Callable[[str], None]] = None):
    if logger is None:
        def logger(x): print(x)
    logger(f"Self-ping loop стартует: url={url}, interval={interval}s")
    await asyncio.sleep(min(interval, 1.0))
    while not stop_event.is_set():
        start = time.time()
        try:
            status, body = await _self_ping_once(url, timeout=10)
            logger(f"HTTP Request: GET {url} -> {status}")
            append_log(f"HTTP Request: GET {url} -> {status}")
        except Exception as e:
            logger(f"Self-ping exception: {e}")
            append_log(f"Self-ping exception: {e}")
        elapsed = time.time() - start
        to_wait = max(0, interval - elapsed)
        waited = 0.0
        while waited < to_wait and not stop_event.is_set():
            step = min(1.0, to_wait - waited)
            await asyncio.sleep(step)
            waited += step
    logger("Self-ping loop остановлен по stop_event.")
    append_log("Self-ping loop остановлен по stop_event.")

@app.on_event("startup")
async def startup_event():
    append_log("Startup: приложение запустилось, проверяем self-ping конфигурацию...")
    enabled, url, interval = read_self_ping_config()
    if enabled and url:
        stop_ev = asyncio.Event()
        state["_self_ping_stop_event"] = stop_ev
        state["_self_ping_task"] = asyncio.create_task(self_ping_loop(url, interval, stop_ev, append_log))
        append_log(f"Self-ping запущен: {url} каждые {interval}s")
    else:
        append_log("Self-ping отключён (SELF_PING_ENABLED=false или SELF_PING_URL не задан).")

@app.on_event("shutdown")
async def shutdown_event():
    append_log("Shutdown: останавливаем фоновые таски...")
    try:
        if state.get("_self_ping_stop_event"):
            state["_self_ping_stop_event"].set()
        if state.get("_self_ping_task"):
            try:
                await state["_self_ping_task"]
            except Exception:
                pass
    except Exception:
        pass
    for c in state.get("clients", []):
        cl = c.get("client")
        try:
            if cl:
                await cl.disconnect()
        except Exception:
            pass
        copy_path = c.get("session_copy") or getattr(cl, "_session_file_copy", None)
        if copy_path and os.path.exists(copy_path):
            try:
                os.remove(copy_path)
                append_log(f"Удалена временная копия сессии: {os.path.basename(copy_path)}")
            except Exception:
                pass
    append_log("Shutdown: завершено.")

# EOF
