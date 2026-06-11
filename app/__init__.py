"""
Splector Flask Application Factory

Initializes Flask + SocketIO with async_mode='threading'.
CRITICAL: No eventlet/gevent — threading mode prevents
monkey-patching conflicts with the native asyncio pipeline.
"""

from flask import Flask
from flask_socketio import SocketIO

# SocketIO instance — shared across modules
socketio = SocketIO()


def create_app() -> Flask:
    app = Flask(__name__)
    app.config["SECRET_KEY"] = "splector-dashboard-secret-key"

    # Initialize SocketIO with threading mode
    # This is CRITICAL — eventlet would conflict with aiohttp/asyncio
    socketio.init_app(app, async_mode="threading", cors_allowed_origins="*")

    # Register HTTP routes
    from app.routes import main_bp
    app.register_blueprint(main_bp)

    # Register SocketIO event handlers
    from app import events  # noqa: F401 — import triggers handler registration

    return app
