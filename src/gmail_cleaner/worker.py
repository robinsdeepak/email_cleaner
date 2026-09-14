"""
Background Worker for Asynchronous Pipeline Tasks.

Allows long-running tasks (fetching, scanning, deleting, importing) to run
in a detached thread without blocking Streamlit or other interfaces.
Exposes thread-safe progress tracking, cooperative cancellation, and log tailing.
"""

import os
import threading
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from .logger import get_logger

logger = get_logger("worker")


class BackgroundWorker:
    """Singleton background task manager with thread-safe state inspection."""

    _instance: Optional["BackgroundWorker"] = None
    _instance_lock = threading.Lock()

    def __new__(cls) -> "BackgroundWorker":
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = super(BackgroundWorker, cls).__new__(cls)
                cls._instance._init_state()
            return cls._instance

    def _init_state(self) -> None:
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._cancel_event = threading.Event()
        self._task_name: Optional[str] = None
        self._start_time: Optional[float] = None
        self._end_time: Optional[float] = None
        self._progress_current: int = 0
        self._progress_total: int = 0
        self._status_message: str = "Idle"
        self._error: Optional[str] = None
        self._result: Any = None

    @property
    def is_running(self) -> bool:
        with self._lock:
            return self._thread is not None and self._thread.is_alive()

    @property
    def is_cancel_requested(self) -> bool:
        return self._cancel_event.is_set()

    def request_cancel(self) -> None:
        """Signal the running task to stop cooperatively."""
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                self._cancel_event.set()
                self._status_message = "Cancellation requested..."
                logger.warning(f"Cancellation requested for task: {self._task_name}")

    def update_progress(self, current: int, total: int = 0, message: str = "") -> None:
        """Update the live progress metrics for the currently running task."""
        with self._lock:
            self._progress_current = current
            if total > 0:
                self._progress_total = total
            if message:
                self._status_message = message

    def start_task(
        self,
        name: str,
        target_fn: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> bool:
        """
        Start a background task in a detached thread.
        Returns True if task was successfully scheduled, False if another task is running.
        """
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                logger.warning(f"Cannot start '{name}': task '{self._task_name}' is already running.")
                return False

            self._cancel_event.clear()
            self._task_name = name
            self._start_time = time.time()
            self._end_time = None
            self._progress_current = 0
            self._progress_total = 0
            self._status_message = f"Starting {name}..."
            self._error = None
            self._result = None

            def _runner() -> None:
                logger.info(f"🚀 [Worker] Starting background task: {name}")
                try:
                    res = target_fn(*args, **kwargs)
                    with self._lock:
                        self._result = res
                        self._end_time = time.time()
                        if self._cancel_event.is_set():
                            self._status_message = f"Task '{name}' cancelled by user."
                            logger.info(f"🛑 [Worker] Task '{name}' stopped on cancellation request.")
                        else:
                            self._status_message = f"Task '{name}' completed successfully."
                            logger.info(f"✅ [Worker] Task '{name}' completed in {self._end_time - self._start_time:.1f}s.")
                except Exception as exc:
                    with self._lock:
                        self._end_time = time.time()
                        self._error = str(exc)
                        self._status_message = f"Task '{name}' failed: {exc}"
                        logger.error(f"❌ [Worker] Task '{name}' failed: {exc}", exc_info=True)

            self._thread = threading.Thread(target=_runner, name=f"Worker-{name}", daemon=True)
            self._thread.start()
            return True

    def get_status(self) -> Dict[str, Any]:
        """Return a snapshot dictionary of the current worker state."""
        with self._lock:
            running = self._thread is not None and self._thread.is_alive()
            elapsed = 0.0
            if self._start_time:
                end = self._end_time if self._end_time else time.time()
                elapsed = round(end - self._start_time, 1)

            pct = 0.0
            if self._progress_total > 0:
                pct = min(100.0, round((self._progress_current / self._progress_total) * 100, 1))

            return {
                "is_running": running,
                "task_name": self._task_name,
                "status_message": self._status_message,
                "progress_current": self._progress_current,
                "progress_total": self._progress_total,
                "progress_pct": pct,
                "elapsed_seconds": elapsed,
                "cancel_requested": self._cancel_event.is_set(),
                "error": self._error,
                "result": self._result,
                "start_time": datetime.fromtimestamp(self._start_time).strftime("%H:%M:%S") if self._start_time else None,
            }

    @staticmethod
    def tail_logs(account: str, lines: int = 60) -> List[str]:
        """Read the last N lines of cleaner.log for the given account."""
        safe_name = account.replace("@", "_at_").replace(".", "_")
        log_path = os.path.join("outputs", safe_name, "logs", "cleaner.log")
        if not os.path.isfile(log_path):
            return [f"No log file found at {log_path} yet."]

        try:
            with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                all_lines = f.readlines()
                return all_lines[-lines:] if len(all_lines) > lines else all_lines
        except Exception as e:
            return [f"Error reading log file: {e}"]


# Singleton global instance
worker = BackgroundWorker()
