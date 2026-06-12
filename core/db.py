import sqlite3
import os

def get_db_connection(db_path: str, check_same_thread: bool = True):
    return sqlite3.connect(str(db_path), check_same_thread=check_same_thread)

def sync_db(conn):
    """Safely sync local db changes to Turso cloud (Deprecated - No-op)."""
    pass
