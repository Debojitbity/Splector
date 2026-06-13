import asyncio
import sqlite3
import os
import logging
from typing import Any

try:
    import libsql_client
except ImportError:
    libsql_client = None

from core.turso_sync import _get_columns, _ensure_tables, _generate_insert_query

logger = logging.getLogger("splector.auto_sync")

async def start_auto_sync(db_path: str, emitter: Any):
    """Background worker that continuously syncs the local db to Turso."""
    # Run loop
    while True:
        try:
            url = os.environ.get("TURSO_DATABASE_URL")
            token = os.environ.get("TURSO_AUTH_TOKEN")
            
            if not url or not token:
                # Silently skip if not configured
                await asyncio.sleep(600)
                continue
                
            if not libsql_client:
                # Silently skip if libsql-client not installed
                await asyncio.sleep(600)
                continue

            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            
            # Read last sync time
            cursor = conn.execute("SELECT value FROM system_state WHERE key = 'last_turso_sync'")
            row = cursor.fetchone()
            last_sync_time = row['value'] if row else '1970-01-01 00:00:00'
            
            # Fetch new records that were modified after the last sync time
            cursor = conn.execute(
                "SELECT * FROM document_refs WHERE timestamp > ? ORDER BY timestamp ASC",
                (last_sync_time,)
            )
            new_records = cursor.fetchall()
            
            if new_records:
                doc_cols = _get_columns(conn, "document_refs")
                conn.close()
                
                async with libsql_client.create_client(url=url, auth_token=token) as client:
                    await _ensure_tables(client, db_path)
                    
                    query = _generate_insert_query("document_refs", doc_cols)
                    batch_size = 500
                    statements = []
                    
                    max_timestamp = last_sync_time
                    for r in new_records:
                        statements.append(libsql_client.Statement(query, args=tuple(r[c] for c in doc_cols)))
                        if r['timestamp'] > max_timestamp:
                            max_timestamp = r['timestamp']
                            
                    for i in range(0, len(statements), batch_size):
                        chunk = statements[i:i + batch_size]
                        await client.batch(chunk)
                    
                    # Update the state internally after a successful push
                    conn = sqlite3.connect(str(db_path))
                    conn.execute("UPDATE system_state SET value = ? WHERE key = 'last_turso_sync'", (max_timestamp,))
                    conn.commit()
                    conn.close()
                    
                    emitter.log("INFO", f"Auto-sync: Successfully synced {len(new_records)} records to Turso cloud.")
            else:
                conn.close()
                
        except Exception as e:
            emitter.log("ERROR", f"Auto-sync failed: {e}")
            
        await asyncio.sleep(600)  # Wait 10 minutes
