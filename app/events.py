"""
Splector WebSocket Event Handlers

Manages the pipeline lifecycle from the browser:
  - pipeline:start  → Spawns background thread with asyncio event loop
  - pipeline:stop   → Sets cancel_event + resumes paused workers
  - pipeline:pause  → Clears pause_event (blocks workers at gate)
  - pipeline:resume → Sets pause_event (unblocks workers)

CRITICAL ARCHITECTURE:
  The asyncio pipeline runs in a BACKGROUND THREAD with its own event loop.
  Flask + SocketIO run in the main thread with async_mode='threading'.
  Cross-thread signaling uses loop.call_soon_threadsafe() for asyncio.Events.
"""

import asyncio
import threading
import logging

from flask_socketio import emit

from app import socketio
from core.config import load_config
from core.emitter import ProgressEmitter
from core.scraper import run_pipeline, db_writer_loop
from core.document_processor import run_document_processing
from core.turso_sync import import_from_turso, export_to_turso

logger = logging.getLogger("splector.events")

# =========================================================
# SHARED PIPELINE STATE
# =========================================================

pipeline_state = {
    "thread": None,         # threading.Thread running the pipeline
    "loop": None,           # asyncio event loop in the bg thread
    "pause_event": None,    # asyncio.Event — set = running, clear = paused
    "cancel_event": None,   # asyncio.Event — set = cancel requested
    "config": None,         # PipelineConfig reference (mutable at runtime)
    "status": "idle",       # idle | running | paused | stopping
}


# =========================================================
# BACKGROUND THREAD RUNNER
# =========================================================

def _proactor_exception_handler(loop, context):
    """
    Custom handler for the ProactorEventLoop on Windows.
    Suppresses the harmless [WinError 10054] and [WinError 10038] noise
    that Proactor emits when remote servers reset connections.
    All other exceptions are re-raised to the default handler.
    """
    exception = context.get("exception")
    if isinstance(exception, (ConnectionResetError, ConnectionAbortedError)):
        return  # Silently swallow — remote server hung up, no action needed
    if isinstance(exception, OSError):
        # WinError 10054 = "Connection reset by remote host"
        # WinError 10038 = "Operation on non-socket" (stale FD after close)
        if getattr(exception, "winerror", None) in (10054, 10038):
            return
    # Anything else → let the default handler deal with it
    loop.default_exception_handler(context)


def _run_pipeline_in_thread(config, emitter, stages=None):
    if stages is None:
        stages = [1, 2, 3, 4]
    """
    Runs in a background thread.
    Creates its own asyncio event loop and runs the pipeline to completion.

    CRITICAL: We use the default ProactorEventLoop on Windows (NOT Selector).
    SelectorEventLoop is limited to 512 file descriptors and crashes
    under high concurrency. ProactorEventLoop has no such limit.
    """
    # DO NOT set WindowsSelectorEventLoopPolicy — it caps at 512 FDs.
    # The default ProactorEventLoop handles unlimited connections.
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Inject the custom handler to silence Proactor WinError noise
    loop.set_exception_handler(_proactor_exception_handler)
    pipeline_state["loop"] = loop

    # Create asyncio events in THIS loop's context
    pause_event = asyncio.Event()
    pause_event.set()  # Start unpaused

    cancel_event = asyncio.Event()

    pipeline_state["pause_event"] = pause_event
    pipeline_state["cancel_event"] = cancel_event

    try:
        loop.run_until_complete(
            run_pipeline(config, emitter, pause_event, cancel_event, stages)
        )
    except Exception as e:
        logger.error(f"Pipeline thread error: {e}")
        emitter.pipeline_status("error")
        emitter.log("ERROR", f"Pipeline thread crashed: {e}")
    finally:
        loop.close()
        pipeline_state["loop"] = None
        pipeline_state["pause_event"] = None
        pipeline_state["cancel_event"] = None
        pipeline_state["config"] = None
        pipeline_state["thread"] = None
        pipeline_state["status"] = "idle"
        emitter.pipeline_idle()
        logger.info("Pipeline thread exited.")




def _run_document_processor_in_thread(config, emitter):
    """
    Runs the Phase 2 Document Processing Pipeline in a background thread.
    Creates its own asyncio event loop and db_writer_loop.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.set_exception_handler(_proactor_exception_handler)
    pipeline_state["loop"] = loop

    pause_event = asyncio.Event()
    pause_event.set()  # Start unpaused
    cancel_event = asyncio.Event()

    pipeline_state["pause_event"] = pause_event
    pipeline_state["cancel_event"] = cancel_event

    async def _run():
        # Create dedicated db_queue and db_writer for this pipeline run
        db_queue = asyncio.Queue()
        db_writer_task = asyncio.create_task(
            db_writer_loop(db_queue, config.abs_db_path)
        )

        try:
            await run_document_processing(
                config, emitter, pause_event, cancel_event, db_queue
            )
            emitter.pipeline_status("completed")
        except asyncio.CancelledError:
            emitter.log("WARNING", "Document processor was cancelled.")
            emitter.pipeline_status("cancelled")
        except Exception as e:
            emitter.log("ERROR", f"Document processor failed: {type(e).__name__}: {e}")
            emitter.pipeline_status("error")
        finally:
            # Send poison pill to db_writer and wait for clean shutdown
            await db_queue.put(None)
            await db_writer_task
            emitter.log("INFO", "Database writer shut down cleanly.")

    try:
        loop.run_until_complete(_run())
    except Exception as e:
        logger.error(f"Document processor thread error: {e}")
        emitter.pipeline_status("error")
        emitter.log("ERROR", f"Document processor thread crashed: {e}")
    finally:
        loop.close()
        pipeline_state["loop"] = None
        pipeline_state["pause_event"] = None
        pipeline_state["cancel_event"] = None
        pipeline_state["thread"] = None
        pipeline_state["status"] = "idle"
        emitter.pipeline_idle()
        logger.info("Document processor thread exited.")

# =========================================================
# CLOUD SYNC RUNNER
# =========================================================

def _run_sync_in_thread(config, emitter, sync_type: str):
    """
    Runs Turso import/export in a background thread.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.set_exception_handler(_proactor_exception_handler)
    pipeline_state["loop"] = loop

    try:
        if sync_type == "import":
            loop.run_until_complete(import_from_turso(config.abs_db_path, emitter))
        elif sync_type == "export":
            loop.run_until_complete(export_to_turso(config.abs_db_path, emitter))
        emitter.pipeline_status("completed")
    except Exception as e:
        logger.error(f"Cloud sync thread error: {e}")
        emitter.pipeline_status("error")
        emitter.log("ERROR", f"Cloud sync failed: {e}")
    finally:
        loop.close()
        pipeline_state["loop"] = None
        pipeline_state["thread"] = None
        pipeline_state["status"] = "idle"
        logger.info(f"Cloud sync ({sync_type}) thread exited.")

# =========================================================
# SOCKETIO EVENT: START
# =========================================================

@socketio.on("pipeline:start")
def handle_start(data=None):
    if pipeline_state["status"] in ("running", "paused"):
        emit("pipeline_status", {
            "status": pipeline_state["status"],
            "message": "Pipeline is already active.",
        })
        return

    task = data.get("task", "run_main_server") if data else "run_main_server"
    logger.info(f"Received pipeline:start event for task: {task}")
    pipeline_state["status"] = "running"

    config = load_config()
    emitter = ProgressEmitter(socketio=socketio)

    if task == "run_aux_server":
        target_fn = _run_pipeline_in_thread
        args = (config, emitter, [2, 3, 4])
    elif task == "run_download_data":
        target_fn = _run_document_processor_in_thread
        args = (config, emitter)
    else:  # run_main_server
        target_fn = _run_pipeline_in_thread
        args = (config, emitter, [1, 2, 3, 4])

    # Store config reference so proxy_decision handler can mutate allow_local_ip
    pipeline_state["config"] = config

    thread = threading.Thread(
        target=target_fn,
        args=args,
        daemon=True,
        name=f"SplectorPipeline-{task}",
    )
    pipeline_state["thread"] = thread
    thread.start()

    emit("pipeline_status", {"status": "running"})


# =========================================================
# SOCKETIO EVENT: PAUSE
# =========================================================

@socketio.on("pipeline:pause")
def handle_pause(data=None):
    if pipeline_state["status"] != "running":
        return

    loop = pipeline_state["loop"]
    evt = pipeline_state["pause_event"]

    if loop and evt:
        loop.call_soon_threadsafe(evt.clear)  # Clear = paused
        pipeline_state["status"] = "paused"
        logger.info("Pipeline paused.")

        # Notify frontend
        socketio.emit("pipeline_status", {"status": "paused"})
        socketio.emit("log", {
            "level": "WARNING",
            "message": "Pipeline paused by user.",
            "timestamp": "",
        })


# =========================================================
# SOCKETIO EVENT: RESUME
# =========================================================

@socketio.on("pipeline:resume")
def handle_resume(data=None):
    if pipeline_state["status"] != "paused":
        return

    loop = pipeline_state["loop"]
    evt = pipeline_state["pause_event"]

    if loop and evt:
        loop.call_soon_threadsafe(evt.set)  # Set = running
        pipeline_state["status"] = "running"
        logger.info("Pipeline resumed.")

        socketio.emit("pipeline_status", {"status": "running"})
        socketio.emit("log", {
            "level": "INFO",
            "message": "Pipeline resumed.",
            "timestamp": "",
        })


# =========================================================
# SOCKETIO EVENT: STOP
# =========================================================

@socketio.on("pipeline:stop")
def handle_stop(data=None):
    if pipeline_state["status"] not in ("running", "paused"):
        return

    loop = pipeline_state["loop"]
    cancel_evt = pipeline_state["cancel_event"]
    pause_evt = pipeline_state["pause_event"]

    if loop and cancel_evt:
        # Set cancel flag
        loop.call_soon_threadsafe(cancel_evt.set)
        # Also resume if paused, so workers can see the cancel flag and exit
        if pause_evt:
            loop.call_soon_threadsafe(pause_evt.set)

        pipeline_state["status"] = "stopping"
        logger.info("Pipeline stop requested.")

        socketio.emit("pipeline_status", {"status": "stopping"})
        socketio.emit("log", {
            "level": "WARNING",
            "message": "Pipeline stop requested. Finishing current tasks...",
            "timestamp": "",
        })


# =========================================================
# SOCKETIO EVENT: PROXY DECISION
# =========================================================
# Fired by the frontend modal when all CF workers are exhausted.
# The user picks either 'continue_local' (use raw IP) or 'cancel'.
# =========================================================

@socketio.on("proxy_decision")
def handle_proxy_decision(data=None):
    if not data:
        return

    action = data.get("action")
    loop = pipeline_state["loop"]
    pause_evt = pipeline_state["pause_event"]
    cancel_evt = pipeline_state["cancel_event"]
    config = pipeline_state.get("config")

    if action == "continue_local":
        logger.info("User approved local IP fallback.")
        if config:
            config.allow_local_ip = True
        if loop and pause_evt:
            loop.call_soon_threadsafe(pause_evt.set)
        socketio.emit("pipeline_status", {"status": "running"})
        socketio.emit("log", {
            "level": "INFO",
            "message": "Pipeline resuming on local IP...",
            "timestamp": "",
        })
        pipeline_state["status"] = "running"

    elif action == "cancel":
        logger.info("User cancelled pipeline after proxy exhaustion.")
        if loop and cancel_evt:
            loop.call_soon_threadsafe(cancel_evt.set)
        # Also unblock pause so workers can see the cancel flag and exit
        if loop and pause_evt:
            loop.call_soon_threadsafe(pause_evt.set)
        pipeline_state["status"] = "stopping"
        socketio.emit("pipeline_status", {"status": "stopping"})
        socketio.emit("log", {
            "level": "WARNING",
            "message": "Pipeline cancelled by user after proxy exhaustion.",
            "timestamp": "",
        })


# =========================================================
# SOCKETIO EVENT: STATUS QUERY
# =========================================================

@socketio.on("pipeline:status")
def handle_status(data=None):
    emit("pipeline_status", {"status": pipeline_state["status"]})


# =========================================================
# SOCKETIO EVENT: CLOUD SYNC
# =========================================================

@socketio.on("pipeline:sync_import")
def handle_sync_import(data=None):
    if pipeline_state["status"] in ("running", "paused", "stopping"):
        emit("pipeline_status", {
            "status": pipeline_state["status"],
            "message": "Pipeline is active. Cannot sync.",
        })
        return

    logger.info("Received pipeline:sync_import event")
    pipeline_state["status"] = "running"
    emit("pipeline_status", {"status": "running"})

    config = load_config()
    emitter = ProgressEmitter(socketio=socketio)
    
    thread = threading.Thread(
        target=_run_sync_in_thread,
        args=(config, emitter, "import"),
        daemon=True,
        name="SplectorPipeline-SyncImport",
    )
    pipeline_state["thread"] = thread
    thread.start()

@socketio.on("pipeline:sync_export")
def handle_sync_export(data=None):
    if pipeline_state["status"] in ("running", "paused", "stopping"):
        emit("pipeline_status", {
            "status": pipeline_state["status"],
            "message": "Pipeline is active. Cannot sync.",
        })
        return

    logger.info("Received pipeline:sync_export event")
    pipeline_state["status"] = "running"
    emit("pipeline_status", {"status": "running"})

    config = load_config()
    emitter = ProgressEmitter(socketio=socketio)
    
    thread = threading.Thread(
        target=_run_sync_in_thread,
        args=(config, emitter, "export"),
        daemon=True,
        name="SplectorPipeline-SyncExport",
    )
    pipeline_state["thread"] = thread
    thread.start()


# =========================================================
# SOCKETIO EVENT: CONNECTION
# =========================================================

@socketio.on("connect")
def handle_connect():
    logger.info("Client connected.")
    emit("pipeline_status", {"status": pipeline_state["status"]})


@socketio.on("disconnect")
def handle_disconnect():
    logger.info("Client disconnected.")
