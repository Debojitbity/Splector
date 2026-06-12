import pandas as pd
import logging
import urllib.parse
from pathlib import Path
from core.db import get_db_connection
from datetime import datetime

def migrate_links():
    print("Starting Splector SQLite Migration...")
    base_dir = Path(__file__).resolve().parent.parent
    excel_path = base_dir / "data" / "links.xlsx"
    db_path = base_dir / "data" / "crawler.db"

    if not excel_path.exists():
        print(f"Excel file not found at {excel_path}")
        return False

    sheets = ["production", "temporary", "unstable"]
    
    try:
        excel = pd.ExcelFile(excel_path, engine="openpyxl")
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return False

    records = []
    seen_domains = set()

    for sheet in sheets:
        if sheet in excel.sheet_names:
            # We load the dataframe. The file is binary so encoding isn't an argument to read_excel,
            # but we explicitly parse URLs with utf-8 encoding via urllib.parse.unquote
            df = pd.read_excel(excel_path, sheet_name=sheet, engine="openpyxl")
            df = df.dropna(subset=["domain"])
            
            for _, row in df.iterrows():
                raw_domain = str(row["domain"]).strip()
                # The unicode/encoding safeguard
                clean_domain = urllib.parse.unquote(raw_domain, encoding='utf-8')
                
                if clean_domain in seen_domains:
                    continue
                seen_domains.add(clean_domain)
                
                if "reachable" in df.columns:
                    r_val = str(row["reachable"]).strip().lower()
                    reachable = 1 if r_val == "true" or r_val == "1" or r_val == "1.0" else 0
                else:
                    reachable = 0
                
                status_code = str(row.get("status_code", "")) if "status_code" in df.columns else ""
                reason = str(row.get("reason", "")) if "reason" in df.columns else ""
                
                response_time_ms = None
                if "response_time_ms" in df.columns:
                    rt = row.get("response_time_ms")
                    if not pd.isna(rt):
                        try:
                            response_time_ms = float(rt)
                        except ValueError:
                            pass
                
                final_url = str(row.get("final_url", "")) if "final_url" in df.columns else ""
                added_at = datetime.utcnow().isoformat()

                records.append((
                    clean_domain,
                    sheet,
                    reachable,
                    status_code,
                    reason,
                    response_time_ms,
                    final_url,
                    added_at
                ))

    if not records:
        print("No valid records found to migrate.")
        return False

    # The Atomic Migration Strategy
    with get_db_connection(db_path) as conn:
        try:
            conn.execute("BEGIN TRANSACTION")
            
            conn.execute("""
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
            
            conn.execute("CREATE INDEX IF NOT EXISTS idx_domains_source ON domains(source_sheet)")
            
            conn.executemany("""
            INSERT OR REPLACE INTO domains (
                domain, source_sheet, reachable, status_code, reason, response_time_ms, final_url, added_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, records)
            
            conn.commit()
            print(f"Successfully migrated {len(records)} domains into crawler.db")
            return True
            
        except Exception as e:
            conn.rollback()
            print(f"Migration transaction failed and rolled back! Error: {e}")
            raise

if __name__ == "__main__":
    migrate_links()
