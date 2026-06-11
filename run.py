"""
Splector Dashboard — Entry Point

Usage:
    python run.py

Opens the dashboard at http://localhost:5000
"""

import logging
import sys

# =========================================================
# PERSISTENT FILE LOGGING
# =========================================================
# logging.basicConfig() is silently ignored when Flask/Werkzeug
# has already configured the root logger. We must explicitly
# create and attach handlers BEFORE importing the app.
# =========================================================

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
    print("\n" + "=" * 56)
    print("  SPLECTOR DASHBOARD")
    print("  http://localhost:5000")
    print("=" * 56 + "\n")

    socketio.run(
        app,
        host="0.0.0.0",
        port=5000,
        debug=True,
        use_reloader=False,   # Reloader can cause duplicate threads
        allow_unsafe_werkzeug=True,
    )
