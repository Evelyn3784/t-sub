# health.py
"""
Self-ping помощник для приложения.
Этот модуль реализует асинхронный цикл, который сам пингует указанный URL
через заданный интервал (в секундах). Использует requests через asyncio.to_thread,
чтобы не вводить дополнительную зависимость async-http (например aiohttp).

Переменные окружения (опционально):
- SELF_PING_ENABLED (true/false) — включить/выключить self-ping (по умолчанию false)
- SELF_PING_URL — URL для пинга (обязателен, если включено)
- SELF_PING_INTERVAL — интервал пинга в секундах (int, по умолчанию 300)

Функции:
- self_ping_loop(url, interval, stop_event, logger) — асинхронный цикл пинга;
- read_config_from_env() — чтение конфигурации из окружения (не обязательно).
"""

import os
import asyncio
import time
from typing import Callable, Optional

try:
    import requests
except Exception:
    requests = None  # requests должен быть в окружении; если нет — ошибки будут логироваться

def read_config_from_env():
    """Читает конфиг из переменных окружения."""
    enabled = os.getenv("SELF_PING_ENABLED", "false").lower() in ("1", "true", "yes")
    url = os.getenv("SELF_PING_URL", "") or None
    try:
        interval = int(os.getenv("SELF_PING_INTERVAL", "300"))
    except Exception:
        interval = 300
    return enabled, url, max(1, interval)


async def self_ping_loop(url: str, interval: int, stop_event: asyncio.Event, logger: Optional[Callable[[str], None]] = None):
    """
    Асинхронный цикл: делает GET запрос к url каждые `interval` секунд.
    stop_event — asyncio.Event, когда он установлен — цикл завершится.
    logger — функция для логирования (например append_log), если None — печатает в stdout.

    Важно: используем requests внутри asyncio.to_thread, чтобы не требовать aiohttp.
    """
    if logger is None:
        def logger(msg: str):
            print(f"[self-ping] {msg}")

    if requests is None:
        logger("Модуль requests не доступен в окружении — self-ping не будет работать.")
        return

    logger(f"Self-ping loop стартует: url={url}, interval={interval}s")
    # небольшой стартовый jitter, чтобы избежать резонанса с другими таймерами
    await asyncio.sleep(min(interval, 1.0))

    while not stop_event.is_set():
        start = time.time()
        try:
            # выполняем блокирующий запрос в thread pool
            def _do_req():
                try:
                    r = requests.get(url, timeout=10)
                    return (r.status_code, (r.text[:200] if r.text else ""))
                except Exception as e:
                    return ("error", str(e))
            status, body = await asyncio.to_thread(_do_req)
            logger(f"HTTP Request: GET {url} -> {status}")
        except Exception as e:
            logger(f"Self-ping exception: {e}")

        # ждем интервал, но реагируем на stop_event
        elapsed = time.time() - start
        to_wait = max(0, interval - elapsed)
        # ждем небольшими шагами, чтобы можно было прервать быстро
        waited = 0.0
        while waited < to_wait and not stop_event.is_set():
            step = min(1.0, to_wait - waited)
            await asyncio.sleep(step)
            waited += step

    logger("Self-ping loop остановлен по stop_event.")
