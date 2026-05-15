import sqlite3
import pandas as pd

from pathlib import Path
from datetime import datetime

# =========================================================
# PATHS
# =========================================================

BASE_DIR = Path(__file__).resolve().parent.parent

EXCEL_FILE = BASE_DIR / "data" / "links.xlsx"

DB_FILE = BASE_DIR / "data" / "crawler.db"

MASTER_REPORT = (
    BASE_DIR /
    "data" /
    "master_report.csv"
)

# =========================================================
# DATABASE
# =========================================================

conn = sqlite3.connect(DB_FILE)

cursor = conn.cursor()

# =========================================================
# HISTORY TABLE
# =========================================================

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

# =========================================================
# INDEX
# =========================================================

cursor.execute("""
CREATE INDEX IF NOT EXISTS idx_domain
ON domain_history(domain)
""")

conn.commit()

# =========================================================
# LOAD EXCEL
# =========================================================

print("\n[LOADING EXCEL DATA]")

production_df = pd.read_excel(
    EXCEL_FILE,
    sheet_name="production",
    engine="openpyxl"
)

temporary_df = pd.read_excel(
    EXCEL_FILE,
    sheet_name="temporary",
    engine="openpyxl"
)

unstable_df = pd.read_excel(
    EXCEL_FILE,
    sheet_name="unstable",
    engine="openpyxl"
)

# =========================================================
# TAG SOURCE TYPE
# =========================================================

production_df["source_type"] = "production"

temporary_df["source_type"] = "temporary"

unstable_df["source_type"] = "unstable"

# =========================================================
# MERGE
# =========================================================

crawl_df = pd.concat([
    production_df,
    temporary_df,
    unstable_df
], ignore_index=True)

crawl_df = crawl_df.drop_duplicates(
    subset=["domain"]
)

# =========================================================
# BASE TRUST SCORE
# =========================================================

def initial_trust_score(source_type):

    if source_type == "production":
        return 9.0

    elif source_type == "temporary":
        return 5.0

    elif source_type == "unstable":
        return 2.0

    return 1.0

# =========================================================
# RELIABILITY ENGINE
# =========================================================

def calculate_reliability_score(

    reachable,
    status_code,
    reason,
    response_time_ms,
    final_url

):

    # =====================================================
    # HARD FAILURES
    # =====================================================

    if reason == "DNS Failure":
        return 0.0

    if reason == "External Redirect Blocked":
        return 0.5

    if reason == "Connection Failed":
        return 1.5

    if reason == "Timeout":
        return 2.5

    if reason == "SSL Error":
        return 3.0

    if reason == "Too Many Redirects":
        return 2.0

    # =====================================================
    # NOT REACHABLE
    # =====================================================

    if not reachable:
        return 2.0

    # =====================================================
    # BASE
    # =====================================================

    score = 10.0

    # =====================================================
    # STATUS CODE
    # =====================================================

    try:

        code = int(status_code)

        if 200 <= code < 300:
            score += 0

        elif 300 <= code < 400:
            score -= 1.0

        elif 400 <= code < 500:
            score -= 3.0

        elif 500 <= code < 600:
            score -= 4.0

    except Exception:

        score -= 2.0

    # =====================================================
    # RESPONSE TIME
    # =====================================================

    try:

        rt = float(response_time_ms)

        if rt <= 200:
            score += 0

        elif rt <= 500:
            score -= 0.3

        elif rt <= 1000:
            score -= 0.8

        elif rt <= 2000:
            score -= 1.5

        elif rt <= 5000:
            score -= 3.0

        else:
            score -= 5.0

    except Exception:

        score -= 2.0

    # =====================================================
    # FINAL CLAMP
    # =====================================================

    score = max(0, min(10, score))

    return round(score, 2)

# =========================================================
# DOMAIN STATUS
# =========================================================

def classify_domain_status(

    reliability_score,
    reachable,
    reason

):

    if reason == "DNS Failure":
        return "DEAD"

    if reason == "External Redirect Blocked":
        return "COMPROMISED"

    if reliability_score >= 8:
        return "EXCELLENT"

    if reliability_score >= 6:
        return "GOOD"

    if reliability_score >= 4:
        return "SLOW"

    if reliability_score >= 2:
        return "UNSTABLE"

    return "CRITICAL"

# =========================================================
# HISTORICAL RELIABILITY
# =========================================================

def calculate_historical_reliability(

    domain,
    current_score

):

    history_df = pd.read_sql_query("""

    SELECT reliability_score

    FROM domain_history

    WHERE domain = ?

    ORDER BY checked_at DESC

    LIMIT 10

    """, conn, params=[domain])

    # =====================================================
    # FIRST RUN
    # =====================================================

    if history_df.empty:
        return current_score

    historical_avg = history_df[
        "reliability_score"
    ].mean()

    # =====================================================
    # WEIGHTED SCORE
    # =====================================================

    final_score = (
        (historical_avg * 0.7) +
        (current_score * 0.3)
    )

    return round(final_score, 2)

# =========================================================
# PROCESS
# =========================================================

print("\n[PROCESSING DOMAINS]")

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

    # =====================================================
    # TRUST SCORE
    # =====================================================

    trust_score = initial_trust_score(
        source_type
    )

    # =====================================================
    # CURRENT RELIABILITY
    # =====================================================

    current_reliability = (
        calculate_reliability_score(

            reachable=reachable,
            status_code=status_code,
            reason=reason,
            response_time_ms=response_time_ms,
            final_url=final_url
        )
    )

    # =====================================================
    # HISTORICAL RELIABILITY
    # =====================================================

    reliability_score = (
        calculate_historical_reliability(

            domain=domain,
            current_score=current_reliability
        )
    )

    # =====================================================
    # STATUS
    # =====================================================

    domain_status = classify_domain_status(

        reliability_score=reliability_score,
        reachable=reachable,
        reason=reason
    )

    # =====================================================
    # INSERT HISTORY
    # =====================================================

    cursor.execute("""

    INSERT INTO domain_history (

        domain,
        trust_score,
        reliability_score,
        reachable,
        status_code,
        reason,
        response_time_ms,
        final_url,
        checked_at

    )

    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)

    """, (

        domain,
        trust_score,
        reliability_score,
        int(reachable),
        status_code,
        reason,
        response_time_ms,
        final_url,
        checked_at

    ))

    inserted += 1

    # =====================================================
    # MASTER REPORT
    # =====================================================

    master_rows.append({

        "domain": domain,

        "status": domain_status,

        "trust_score": trust_score,

        "reliability_score": reliability_score,

        "reachable": reachable,

        "status_code": status_code,

        "reason": reason,

        "response_time_ms": response_time_ms,

        "final_url": final_url,

        "checked_at": checked_at
    })

# =========================================================
# SAVE DB
# =========================================================

conn.commit()

# =========================================================
# MASTER REPORT
# =========================================================

master_df = pd.DataFrame(master_rows)

master_df = master_df.sort_values(

    by=[
        "reliability_score",
        "trust_score"
    ],

    ascending=False
)

master_df.to_csv(

    MASTER_REPORT,

    index=False
)

# =========================================================
# SUMMARY
# =========================================================

excellent = len(
    master_df[
        master_df["status"] == "EXCELLENT"
    ]
)

good = len(
    master_df[
        master_df["status"] == "GOOD"
    ]
)

slow = len(
    master_df[
        master_df["status"] == "SLOW"
    ]
)

unstable = len(
    master_df[
        master_df["status"] == "UNSTABLE"
    ]
)

critical = len(
    master_df[
        master_df["status"] == "CRITICAL"
    ]
)

dead = len(
    master_df[
        master_df["status"] == "DEAD"
    ]
)

compromised = len(
    master_df[
        master_df["status"] == "COMPROMISED"
    ]
)

# =========================================================
# OUTPUT
# =========================================================

print("\n" + "=" * 80)
print("SPLECTOR RELIABILITY ENGINE COMPLETE")
print("=" * 80)

print(f"""
Database:
{DB_FILE}

Master Report:
{MASTER_REPORT}

Inserted Rows:
{inserted}

STATUS BREAKDOWN

EXCELLENT   : {excellent}
GOOD        : {good}
SLOW        : {slow}
UNSTABLE    : {unstable}
CRITICAL    : {critical}
DEAD        : {dead}
COMPROMISED : {compromised}
""")

# =========================================================
# CLOSE
# =========================================================

conn.close()