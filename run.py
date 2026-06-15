import logging
import sys
import os
from dotenv import load_dotenv

# Load environment variables (Turso, etc.)
load_dotenv()

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"

# Get the root logger — ALL child loggers (werkzeug, splector.*, etc.) inherit from this.
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

# File handler → splector_debug.log (persistent crash record)
file_handler = logging.FileHandler("splector_debug.log", encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=LOG_DATEFMT))
root_logger.addHandler(file_handler)

# Console handler → stdout (keeps terminal output working)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=LOG_DATEFMT))
root_logger.addHandler(console_handler)

from app import create_app, socketio

app = create_app()

if __name__ == "__main__":
    from core.config import load_config
    from core.emitter import ProgressEmitter
    from core.auto_sync import start_auto_sync
    from core.stats_worker import run_telemetry_loop
    import asyncio
    import threading

    config = load_config()
    emitter = ProgressEmitter(socketio=socketio)

    from core.seeder import auto_seed_database
    archive_path = os.path.join(config.base_dir, 'data', 'archive', 'links.xlsx')
    auto_seed_database(config.abs_db_path, archive_path)

    def _proactor_exception_handler(loop, context):
        exception = context.get("exception")
        if isinstance(exception, (ConnectionResetError, ConnectionAbortedError)):
            return
        if isinstance(exception, OSError):
            if getattr(exception, "winerror", None) in (10054, 10038):
                return
        loop.default_exception_handler(context)

    def run_auto_sync():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.set_exception_handler(_proactor_exception_handler)
        try:
            loop.run_until_complete(start_auto_sync(config.abs_db_path, emitter))
        except Exception as e:
            logging.error(f"Auto-sync thread crashed: {e}")
        finally:
            loop.close()

    def run_telemetry():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.set_exception_handler(_proactor_exception_handler)
        try:
            loop.run_until_complete(run_telemetry_loop(config.abs_db_path, emitter, config.base_dir))
        except Exception as e:
            logging.error(f"Telemetry thread crashed: {e}")
        finally:
            loop.close()

    threading.Thread(target=run_auto_sync, daemon=True, name="AutoSyncThread").start()
    threading.Thread(target=run_telemetry, daemon=True, name="TelemetryThread").start()

    print("\n" + "=" * 56)
    print("  SPLECTOR DASHBOARD")
    print("  http://localhost:5000")
    print("=" * 56 + "\n")

    socketio.run(
        app,
        host="0.0.0.0",
        port=5001,
        debug=True,
        use_reloader=False,   # Reloader can cause duplicate threads
        allow_unsafe_werkzeug=True,
    )
