import asyncio
import sqlite3
import os
import tiktoken
from langdetect import detect, DetectorFactory

# Set seed for consistent language detection
DetectorFactory.seed = 0

def _ensure_schema(db_path: str):
    """Ensure that the database schema is ready for telemetry data."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 1. Ensure system_stats exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS system_stats (
            metric_key TEXT PRIMARY KEY,
            metric_value TEXT
        )
    """)
    
    # 2. Add columns to document_refs if they don't exist
    columns = [
        ('word_count', 'INTEGER'),
        ('token_count', 'INTEGER'),
        ('language', 'TEXT'),
        ('file_size_bytes', 'INTEGER'),
        ('workflow_state', "TEXT DEFAULT 'UNREVIEWED'")
    ]
    
    for col_name, col_type in columns:
        try:
            cursor.execute(f"ALTER TABLE document_refs ADD COLUMN {col_name} {col_type}")
        except sqlite3.OperationalError:
            # OperationalError is raised if the column already exists
            pass
            
    conn.commit()
    conn.close()


def _process_file(file_path: str) -> tuple[int, int, str, int]:
    """
    Synchronous helper to process a single file.
    Returns: (word_count, token_count, language, file_size_bytes)
    """
    if not os.path.exists(file_path):
        return (0, 0, 'Unknown', 0)
        
    try:
        file_size = os.path.getsize(file_path)
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            text = f.read()
            
        word_count = len(text.split())
        
        # Calculate token count using tiktoken
        encoding = tiktoken.get_encoding("cl100k_base")
        token_count = len(encoding.encode(text, disallowed_special=()))
        
        # Detect language on the first 2,000 characters
        snippet = text[:2000].strip()
        if not snippet:
            language = 'Unknown'
        else:
            try:
                language = detect(snippet)
            except Exception:
                language = 'Others'
                
        return (word_count, token_count, language, file_size)
    except Exception as e:
        return (0, 0, 'Error', 0)


async def run_telemetry_loop(db_path: str, emitter):
    """
    Background daemon that processes downloaded text files for telemetry.
    Maintains a live state in the system_stats table.
    """
    # Ensure database schema is ready
    _ensure_schema(db_path)
    
    last_logged_state = None
    
    while True:
        try:
            # Open DB connection for checking the backlog
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Step A: State & Backlog Tracking
            cursor.execute("""
                SELECT COUNT(*) FROM document_refs 
                WHERE processing_status = 'SUCCESS' 
                AND word_count IS NULL 
                AND prepared_file_path IS NOT NULL
            """)
            backlog_count = cursor.fetchone()[0]
            
            # Update telemetry_backlog
            cursor.execute(
                "INSERT OR REPLACE INTO system_stats (metric_key, metric_value) VALUES ('telemetry_backlog', ?)",
                (str(backlog_count),)
            )
            
            if backlog_count > 0:
                current_state = 'SCANNING'
                cursor.execute(
                    "INSERT OR REPLACE INTO system_stats (metric_key, metric_value) VALUES ('daemon_status', 'SCANNING')"
                )
            else:
                current_state = 'IDLE'
                cursor.execute(
                    "INSERT OR REPLACE INTO system_stats (metric_key, metric_value) VALUES ('daemon_status', 'IDLE')"
                )
            
            conn.commit()
            
            # Anti-Spam Logging Logic
            if current_state != last_logged_state:
                emitter.log("INFO", f"[TELEMETRY] Daemon state changed to: {current_state} (Backlog: {backlog_count})")
                last_logged_state = current_state
                
            if backlog_count == 0:
                conn.close()
                await asyncio.sleep(60)
                continue
                
            # Step B: Batch Processing
            cursor.execute("""
                SELECT record_id, prepared_file_path FROM document_refs 
                WHERE processing_status = 'SUCCESS' 
                AND word_count IS NULL 
                AND prepared_file_path IS NOT NULL
                LIMIT 500
            """)
            batch = cursor.fetchall()
            conn.close()
            
            processed = 0
            for record_id, file_path in batch:
                # Heavy math is offloaded to a background thread to prevent blocking WebSocket loop
                word_count, token_count, language, file_size_bytes = await asyncio.to_thread(_process_file, file_path)
                
                # Open a new connection per update to avoid holding the lock
                conn = sqlite3.connect(db_path)
                conn.execute("""
                    UPDATE document_refs 
                    SET word_count = ?, token_count = ?, language = ?, file_size_bytes = ?
                    WHERE record_id = ?
                """, (word_count, token_count, language, file_size_bytes, record_id))
                conn.commit()
                conn.close()
                processed += 1
                
            # Step C: Global Aggregation
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT COUNT(*), SUM(word_count), MIN(word_count), MAX(word_count), 
                       MIN(token_count), MAX(token_count), MIN(file_size_bytes), MAX(file_size_bytes)
                FROM document_refs 
                WHERE word_count IS NOT NULL
            """)
            agg = cursor.fetchone()
            total_files = agg[0] or 0
            total_words = agg[1] or 0
            min_words = agg[2] or 0
            max_words = agg[3] or 0
            min_tokens = agg[4] or 0
            max_tokens = agg[5] or 0
            min_size = agg[6] or 0
            max_size = agg[7] or 0
            
            cursor.execute("INSERT OR REPLACE INTO system_stats (metric_key, metric_value) VALUES ('total_files_processed', ?)", (str(total_files),))
            cursor.execute("INSERT OR REPLACE INTO system_stats (metric_key, metric_value) VALUES ('total_word_count', ?)", (str(total_words),))
            cursor.execute("INSERT OR REPLACE INTO system_stats (metric_key, metric_value) VALUES ('min_word_count', ?)", (str(min_words),))
            cursor.execute("INSERT OR REPLACE INTO system_stats (metric_key, metric_value) VALUES ('max_word_count', ?)", (str(max_words),))
            cursor.execute("INSERT OR REPLACE INTO system_stats (metric_key, metric_value) VALUES ('min_token_count', ?)", (str(min_tokens),))
            cursor.execute("INSERT OR REPLACE INTO system_stats (metric_key, metric_value) VALUES ('max_token_count', ?)", (str(max_tokens),))
            cursor.execute("INSERT OR REPLACE INTO system_stats (metric_key, metric_value) VALUES ('min_file_size', ?)", (str(min_size),))
            cursor.execute("INSERT OR REPLACE INTO system_stats (metric_key, metric_value) VALUES ('max_file_size', ?)", (str(max_size),))
            
            # Aggregate language grouping
            cursor.execute("""
                SELECT language, COUNT(*) FROM document_refs 
                WHERE language IS NOT NULL 
                GROUP BY language
            """)
            langs = cursor.fetchall()
            
            lang_counts = {'en': 0, 'hi': 0, 'others': 0}
            for lang, count in langs:
                if lang == 'en':
                    lang_counts['en'] += count
                elif lang == 'hi':
                    lang_counts['hi'] += count
                else:
                    lang_counts['others'] += count
                    
            cursor.execute("INSERT OR REPLACE INTO system_stats (metric_key, metric_value) VALUES ('count_english', ?)", (str(lang_counts['en']),))
            cursor.execute("INSERT OR REPLACE INTO system_stats (metric_key, metric_value) VALUES ('count_hindi', ?)", (str(lang_counts['hi']),))
            cursor.execute("INSERT OR REPLACE INTO system_stats (metric_key, metric_value) VALUES ('count_others', ?)", (str(lang_counts['others']),))
                
            conn.commit()
            conn.close()
            
            remaining = max(0, backlog_count - processed)
            emitter.log("INFO", f"[TELEMETRY] Batch complete. {remaining} files remaining.")
            
            # Step D: Rest
            await asyncio.sleep(5)
            
        except Exception as e:
            if last_logged_state != 'ERROR':
                emitter.log("ERROR", f"[TELEMETRY] Fatal error in telemetry loop: {e}")
                last_logged_state = 'ERROR'
            await asyncio.sleep(60)
