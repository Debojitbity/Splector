import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime
from core.db import get_db_connection

def initial_trust_score(source_type):
    if source_type == "production":
        return 9.0
    elif source_type == "temporary":
        return 5.0
    elif source_type == "unstable":
        return 2.0
    return 1.0

def calculate_reliability_score(reachable, status_code, reason, response_time_ms, final_url):
    if reason == "DNS Failure": return 0.0
    if reason == "External Redirect Blocked": return 0.5
    if reason == "Connection Failed": return 1.5
    if reason == "Timeout": return 2.5
    if reason == "SSL Error": return 3.0
    if reason == "Too Many Redirects": return 2.0
    if not reachable: return 2.0

    score = 10.0
    try:
        code = int(status_code)
        if 200 <= code < 300: score += 0
        elif 300 <= code < 400: score -= 1.0
        elif 400 <= code < 500: score -= 3.0
        elif 500 <= code < 600: score -= 4.0
    except Exception:
        score -= 2.0

    try:
        rt = float(response_time_ms)
        if rt <= 200: score += 0
        elif rt <= 500: score -= 0.3
        elif rt <= 1000: score -= 0.8
        elif rt <= 2000: score -= 1.5
        elif rt <= 5000: score -= 3.0
        else: score -= 5.0
    except Exception:
        score -= 2.0

    return round(max(0, min(10, score)), 2)

def classify_domain_status(reliability_score, reachable, reason):
    if reason == "DNS Failure": return "DEAD"
    if reason == "External Redirect Blocked": return "COMPROMISED"
    if reliability_score >= 8: return "EXCELLENT"
    if reliability_score >= 6: return "GOOD"
    if reliability_score >= 4: return "SLOW"
    if reliability_score >= 2: return "UNSTABLE"
    return "CRITICAL"

def calculate_historical_reliability(domain, current_score, conn):
    history_df = pd.read_sql_query("""
    SELECT reliability_score FROM domain_history
    WHERE domain = ? ORDER BY checked_at DESC LIMIT 10
    """, conn, params=[domain])

    if history_df.empty: return current_score
    historical_avg = history_df["reliability_score"].mean()
    return round((historical_avg * 0.7) + (current_score * 0.3), 2)

def run_domain_grader(config, emitter):
    emitter.pipeline_status("running")
    emitter.log("INFO", "══════════════════════════════════════════════════════")
    emitter.log("INFO", "SPLECTOR RELIABILITY ENGINE")
    emitter.log("INFO", "══════════════════════════════════════════════════════")

    base_dir = Path(config.abs_db_path).parent
    db_file = base_dir / "crawler.db"
    master_report = base_dir / "master_report.csv"

    emitter.stage_start(1, 1)

    try:
        conn = get_db_connection(db_file)
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS domain_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            domain TEXT NOT NULL,
            trust_score REAL,
            reliability_score REAL,
            reachable INTEGER,
            status_code TEXT,
            reason TEXT,
            response_time_ms REAL,
            final_url TEXT,
            checked_at TEXT
        )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_domain ON domain_history(domain)")
        conn.commit()

        emitter.log("INFO", "[LOADING DOMAINS FROM SQLITE]")
        crawl_df = pd.read_sql_query("SELECT * FROM domains", conn)
        crawl_df = crawl_df.rename(columns={"source_sheet": "source_type"})

        emitter.log("INFO", "[PROCESSING DOMAINS]")
        checked_at = datetime.utcnow().isoformat()
        master_rows = []
        inserted = 0

        for _, row in crawl_df.iterrows():
            domain = str(row["domain"]).strip()
            reachable = bool(row["reachable"])
            status_code = str(row["status_code"])
            reason = str(row["reason"])
            response_time_ms = row["response_time_ms"]
            final_url = str(row["final_url"])
            source_type = row["source_type"]

            trust_score = initial_trust_score(source_type)
            current_reliability = calculate_reliability_score(reachable, status_code, reason, response_time_ms, final_url)
            reliability_score = calculate_historical_reliability(domain, current_reliability, conn)
            domain_status = classify_domain_status(reliability_score, reachable, reason)

            cursor.execute("""
            INSERT INTO domain_history (
                domain, trust_score, reliability_score, reachable, status_code, reason, response_time_ms, final_url, checked_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (domain, trust_score, reliability_score, int(reachable), status_code, reason, response_time_ms, final_url, checked_at))

            inserted += 1

            master_rows.append({
                "domain": domain, "status": domain_status, "trust_score": trust_score,
                "reliability_score": reliability_score, "reachable": reachable, "status_code": status_code,
                "reason": reason, "response_time_ms": response_time_ms, "final_url": final_url,
                "checked_at": checked_at
            })

        conn.commit()

        master_df = pd.DataFrame(master_rows)
        master_df = master_df.sort_values(by=["reliability_score", "trust_score"], ascending=False)
        master_df.to_csv(master_report, index=False)

        excellent = len(master_df[master_df["status"] == "EXCELLENT"])
        good = len(master_df[master_df["status"] == "GOOD"])
        slow = len(master_df[master_df["status"] == "SLOW"])
        unstable = len(master_df[master_df["status"] == "UNSTABLE"])
        critical = len(master_df[master_df["status"] == "CRITICAL"])
        dead = len(master_df[master_df["status"] == "DEAD"])
        compromised = len(master_df[master_df["status"] == "COMPROMISED"])

        emitter.log("INFO", "══════════════════════════════════════════════════════")
        emitter.log("INFO", "SPLECTOR RELIABILITY ENGINE COMPLETE")
        emitter.log("INFO", "══════════════════════════════════════════════════════")
        emitter.log("INFO", f"Database: {db_file}")
        emitter.log("INFO", f"Master Report: {master_report}")
        emitter.log("INFO", f"Inserted Rows: {inserted}")
        emitter.log("INFO", "")
        emitter.log("INFO", "STATUS BREAKDOWN")
        emitter.log("INFO", f"EXCELLENT   : {excellent}")
        emitter.log("INFO", f"GOOD        : {good}")
        emitter.log("INFO", f"SLOW        : {slow}")
        emitter.log("INFO", f"UNSTABLE    : {unstable}")
        emitter.log("INFO", f"CRITICAL    : {critical}")
        emitter.log("INFO", f"DEAD        : {dead}")
        emitter.log("INFO", f"COMPROMISED : {compromised}")

        conn.close()

        emitter.stage_progress(1, 1, 1)
        emitter.stage_complete(1)

    except Exception as e:
        emitter.log("ERROR", f"Reliability Engine Failed: {e}")
        raise e
