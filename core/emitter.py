"""
Splector Progress Emitter

Bridge between the core asyncio pipeline and Flask-SocketIO.
When SocketIO is available, emits events to the browser.
When running standalone (CLI), falls back to print/logging.

Thread-safe: socketio.emit() with async_mode='threading' is safe
to call from any thread.
"""

import logging
from datetime import datetime

logger = logging.getLogger("splector.emitter")


class ProgressEmitter:
    """Emits pipeline events to the frontend via WebSockets."""

    def __init__(self, socketio=None):
        self._sio = socketio

    # -------------------------------------------------
    # Internal emit helper
    # -------------------------------------------------

    def _emit(self, event: str, data: dict):
        if self._sio:
            self._sio.emit(event, data, namespace="/")
        else:
            # CLI fallback
            logger.info(f"[{event}] {data}")

    # -------------------------------------------------
    # Pipeline lifecycle
    # -------------------------------------------------

    def pipeline_status(self, status: str):
        """Status: 'running', 'paused', 'completed', 'cancelled', 'error'."""
        self._emit("pipeline_status", {"status": status})

    # -------------------------------------------------
    # Stage progress
    # -------------------------------------------------

    def stage_start(self, stage: int, total: int):
        self._emit("stage_start", {
            "stage": stage,
            "total": total,
        })

    def stage_progress(self, stage: int, current: int, total: int):
        self._emit("progress", {
            "stage": stage,
            "current": current,
            "total": total,
        })

    def stage_complete(self, stage: int):
        self._emit("stage_complete", {"stage": stage})

    # -------------------------------------------------
    # Live logging
    # -------------------------------------------------

    def log(self, level: str, message: str):
        timestamp = datetime.now().strftime("%H:%M:%S")
        self._emit("log", {
            "level": level,
            "message": message,
            "timestamp": timestamp,
        })
        # Also log to Python logger for server-side records
        getattr(logger, level.lower(), logger.info)(message)

    # -------------------------------------------------
    # Database stats
    # -------------------------------------------------

    def stats_update(self, data: dict):
        self._emit("stats", data)
