"""
Manual Batch Sync Engine for Turso
Uses libsql_client to push and pull data from the cloud.
"""

import os
import sqlite3
import logging

try:
    import libsql_client
except ImportError:
    libsql_client = None

logger = logging.getLogger("splector.turso_sync")

def _get_columns(conn: sqlite3.Connection, table_name: str) -> list:
    """Run PRAGMA table_info to extract exact column names dynamically."""
    cursor = conn.execute(f"PRAGMA table_info({table_name})")
    return [row[1] for row in cursor.fetchall()]

async def _ensure_tables(client, local_db_path: str):
    """Ensure cloud DB has the required tables by mirroring the local schema."""
    conn = sqlite3.connect(str(local_db_path))
    cursor = conn.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name IN ('domains', 'document_refs')")
    statements = [row[0] for row in cursor.fetchall() if row[0]]
    conn.close()
    
    if statements:
        await client.batch(statements)

def _generate_insert_query(table_name: str, keys: list) -> str:
    cols = ", ".join(keys)
    placeholders = ", ".join(["?"] * len(keys))
    return f"INSERT OR REPLACE INTO {table_name} ({cols}) VALUES ({placeholders})"

def _map_cloud_data_to_local_schema(rows, cloud_cols, local_cols):
    """
    Maps incoming Turso rows safely to the current local schema.
    Only includes columns that exist in BOTH the cloud and local DB.
    """
    valid_cols = [c for c in local_cols if c in cloud_cols]
    if not valid_cols:
        return [], []
    
    col_indices = [cloud_cols.index(c) for c in valid_cols]
    
    mapped_data = []
    for row in rows:
        mapped_data.append(tuple(row[i] for i in col_indices))
        
    return valid_cols, mapped_data

async def export_to_turso(local_db_path: str, emitter):
    """Fetch all rows from local DB and push to Turso using dynamic schema mirroring."""
    url = os.environ.get("TURSO_DATABASE_URL")
    token = os.environ.get("TURSO_AUTH_TOKEN")
    
    if not url or not token:
        emitter.log("ERROR", "TURSO_DATABASE_URL or TURSO_AUTH_TOKEN not set in .env")
        return
        
    if not libsql_client:
        emitter.log("ERROR", "libsql-client is not installed.")
        return
        
    emitter.log("INFO", "Starting export to Turso cloud...")
    
    # Connect to local DB and extract dynamic schema & data
    conn = sqlite3.connect(str(local_db_path))
    conn.row_factory = sqlite3.Row
    
    domain_cols = _get_columns(conn, "domains")
    doc_cols = _get_columns(conn, "document_refs")
    
    try:
        domains = conn.execute("SELECT * FROM domains").fetchall()
    except Exception as e:
        domains = []
        emitter.log("WARNING", f"Could not read local domains: {e}")
        
    try:
        docs = conn.execute("SELECT * FROM document_refs").fetchall()
    except Exception as e:
        docs = []
        emitter.log("WARNING", f"Could not read local document_refs: {e}")
        
    conn.close()
    
    emitter.log("INFO", f"Read {len(domains)} domains and {len(docs)} documents from local DB.")
    
    try:
        async with libsql_client.create_client(url=url, auth_token=token) as client:
            # 1. Dynamic Table Creation
            await _ensure_tables(client, local_db_path)
            
            # 2. Dynamic Batch Inserts for domains
            if domains and domain_cols:
                query = _generate_insert_query("domains", domain_cols)
                batch_size = 500
                statements = []
                for d in domains:
                    # Args array strictly maps to the exact PRAGMA column order
                    statements.append(libsql_client.Statement(query, args=tuple(d[c] for c in domain_cols)))
                
                for i in range(0, len(statements), batch_size):
                    chunk = statements[i:i + batch_size]
                    await client.batch(chunk)
                    emitter.log("INFO", f"Exported domains batch {i//batch_size + 1}/{(len(statements)-1)//batch_size + 1}")
            
            # Dynamic Batch Inserts for document_refs
            if docs and doc_cols:
                query = _generate_insert_query("document_refs", doc_cols)
                batch_size = 500
                statements = []
                for d in docs:
                    # Args array strictly maps to the exact PRAGMA column order
                    statements.append(libsql_client.Statement(query, args=tuple(d[c] for c in doc_cols)))
                    
                for i in range(0, len(statements), batch_size):
                    chunk = statements[i:i + batch_size]
                    await client.batch(chunk)
                    emitter.log("INFO", f"Exported docs batch {i//batch_size + 1}/{(len(statements)-1)//batch_size + 1}")
                    
        emitter.log("INFO", "Export to Turso completed successfully.")
    except Exception as e:
        emitter.log("ERROR", f"Export failed: {e}")

async def import_from_turso(local_db_path: str, emitter):
    """Fetch all rows from Turso and upsert to local DB, mapping dynamically to local schema."""
    url = os.environ.get("TURSO_DATABASE_URL")
    token = os.environ.get("TURSO_AUTH_TOKEN")
    
    if not url or not token:
        emitter.log("ERROR", "TURSO_DATABASE_URL or TURSO_AUTH_TOKEN not set in .env")
        return
        
    if not libsql_client:
        emitter.log("ERROR", "libsql-client is not installed.")
        return
        
    emitter.log("INFO", "Starting import from Turso cloud...")
    
    # 1. Fetch current local schema via PRAGMA
    conn = sqlite3.connect(str(local_db_path))
    local_domain_cols = _get_columns(conn, "domains")
    local_doc_cols = _get_columns(conn, "document_refs")
    conn.close()
    
    try:
        async with libsql_client.create_client(url=url, auth_token=token) as client:
            await _ensure_tables(client, local_db_path)
            
            try:
                domain_rs = await client.execute("SELECT * FROM domains")
                cloud_domains = domain_rs.rows
                cloud_domain_cols = domain_rs.columns
            except Exception:
                cloud_domains = []
                cloud_domain_cols = []
                
            try:
                docs_rs = await client.execute("SELECT * FROM document_refs")
                cloud_docs = docs_rs.rows
                cloud_doc_cols = docs_rs.columns
            except Exception:
                cloud_docs = []
                cloud_doc_cols = []
            
        emitter.log("INFO", f"Downloaded {len(cloud_domains)} domains and {len(cloud_docs)} documents from Turso.")
        
        # 2. Safely map cloud data to local schema
        insert_domain_cols, mapped_domains = _map_cloud_data_to_local_schema(
            cloud_domains, cloud_domain_cols, local_domain_cols
        )
        
        insert_doc_cols, mapped_docs = _map_cloud_data_to_local_schema(
            cloud_docs, cloud_doc_cols, local_doc_cols
        )
        
        conn = sqlite3.connect(str(local_db_path))
        try:
            conn.execute("BEGIN TRANSACTION")
            
            if mapped_domains and insert_domain_cols:
                query = _generate_insert_query("domains", insert_domain_cols)
                conn.executemany(query, mapped_domains)
            
            if mapped_docs and insert_doc_cols:
                query = _generate_insert_query("document_refs", insert_doc_cols)
                conn.executemany(query, mapped_docs)
                
            conn.commit()
            emitter.log("INFO", "Import from Turso completed successfully.")
        except Exception as e:
            conn.rollback()
            emitter.log("ERROR", f"Local DB write failed: {e}")
        finally:
            conn.close()
            
    except Exception as e:
        emitter.log("ERROR", f"Import failed: {e}")
