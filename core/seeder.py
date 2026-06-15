import sqlite3
import sys
import logging
from datetime import datetime

def auto_seed_database(db_path: str, archive_path: str):
    """
    Seeds the database from the given Excel file if the domains table is empty.
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Ensure the table exists in case the seeder runs before other setup
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS domains (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                domain TEXT NOT NULL,
                source_sheet TEXT NOT NULL,
                reachable INTEGER NOT NULL DEFAULT 0,
                status_code TEXT,
                reason TEXT,
                response_time_ms REAL,
                final_url TEXT,
                added_at TEXT NOT NULL,
                UNIQUE(domain)
            )
        """)
        
        # Check if table has data
        cursor.execute("SELECT COUNT(*) FROM domains")
        count = cursor.fetchone()[0]
        
        if count > 0:
            conn.close()
            return
            
    except Exception as e:
        logging.error(f"[ERROR] Failed to check database state: {e}")
        if 'conn' in locals(): conn.close()
        return

    # Table is empty, proceed to read Excel
    try:
        import pandas as pd
        # Wrap pd.read_excel in try/except. If the Excel file is missing or the sheet name is wrong,
        # catch the exception, print a critical error to the terminal, and cleanly exit the application
        df = pd.read_excel(archive_path, sheet_name='production', engine='openpyxl')
    except Exception as e:
        print(f"[CRITICAL ERROR] Failed to load seeder Excel archive from {archive_path}: {e}")
        sys.exit(1)
        
    try:
        if 'domain' not in df.columns:
            print("[CRITICAL ERROR] 'domain' column not found in the production sheet.")
            sys.exit(1)
            
        records = []
        added_at = datetime.utcnow().isoformat()
        
        for _, row in df.iterrows():
            d = str(row['domain']).strip()
            if not d:
                continue
                
            reachable = 1
            if 'reachable' in df.columns:
                r_val = str(row['reachable']).strip().lower()
                reachable = 1 if r_val in ('true', '1', '1.0') else 0
                
            records.append((d, "production", reachable, None, None, None, None, added_at))
        
        cursor.executemany("""
            INSERT OR IGNORE INTO domains (
                domain, source_sheet, reachable, status_code, reason, response_time_ms, final_url, added_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, records)
        
        conn.commit()
        print(f"[INFO] Database was empty. Auto-seeded {len(records)} domains from archive Excel file.")
        
    except Exception as e:
        print(f"[ERROR] Failed to seed database: {e}")
    finally:
        conn.close()
