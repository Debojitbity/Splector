"""
Splector Mega Pipeline 4.0 (Worker Rotation & SQLite)

Architecture:
  - asyncio.Queue WORKER POOL for concurrent URL processing
  - Dedicated db_writer_loop for ZERO-LOCK SQLite writes
  - ProgressEmitter for real-time WebSocket dashboard updates
  - Pause/Cancel support via asyncio.Events

Features (v4.0):
  1) Cloudflare Worker ROTATION (array of proxies, auto-retire on 429/1015)
  2) Manual Local-IP Fallback (pipeline pauses, user approves via modal)
  3) 2-Dimensional Lexical Triage (URL + Anchor Text)
  4) Zero-Loss URL Sanitization (Preserves DOM context)
  5) Level 3 Child Extraction (Grabs actual PDFs/HTMLs from notice boards)
"""

from __future__ import annotations

import asyncio
import logging
import os
from dotenv import load_dotenv

load_dotenv()
SECRET_TOKEN = os.getenv("CF_TOKEN")
import re
import urllib.parse
from datetime import datetime
from typing import List, Tuple
from urllib.parse import urlparse, urljoin

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup

from core.config import PipelineConfig
from core.emitter import ProgressEmitter
from core.db import get_db_connection, sync_db

logger = logging.getLogger("splector.pipeline")

# =========================================================
# GLOBAL FILTER DEFINITIONS (preserved from 2.0)
# =========================================================

MEDIA_EXTENSIONS = (
    ".pdf", ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".webp", ".ico",
    ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".zip", ".rar", ".7z",
    ".tar", ".gz", ".exe", ".msi", ".mp3", ".mp4", ".avi", ".mkv", ".csv",
    ".xml", ".json", ".css", ".js",
)

TARGET_KEYWORDS = [
    r"recruit", r"vacanc", r"career", r"job", r"advert", r"employ",
    r"notice", r"circular", r"whats.?new", r"opening", r"hiring",
    r"bharti", r"niyukti", r"opportunity", r"appointment",
]

NOISE_KEYWORDS = [
    r"tender", r"syllabus", r"result", r"exam", r"student", r"gallery",
    r"alumni", r"rti", r"about", r"contact", r"history", r"login",
    r"register", r"forgot", r"event", r"photo", r"video", r"act-rule",
    r"archive", r"corrigendum", r"admit[-_]?card", r"comment",
    r"facebook", r"twitter", r"t\.me", r"whatsapp", r"youtube",
    r"instagram", r"google", r"sarkariresult", r"cdn[-_]?cgi",
    r"email[-_]?protected", r"wp[-_]?content", r"javax", r"action",
    r"layout", r"ubermenu", r"noopener", r"noreferrer", r"sitelogo",
]

target_pattern = re.compile("|".join(TARGET_KEYWORDS), re.IGNORECASE)
noise_pattern = re.compile("|".join(NOISE_KEYWORDS), re.IGNORECASE)

ANCHOR_JOB_HINTS = re.compile(
    r"(apply|advt|advertisement|notification|post of|recruitment|vacancy|click here)",
    re.IGNORECASE,
)


# =========================================================
# CORE WATERFALL NETWORK FETCHER (v4.0 — Worker Rotation)
# =========================================================
# RATE-LIMIT CODES:
#   429 = HTTP Too Many Requests
#  1015 = Cloudflare-specific rate-limit
# On these codes the worker is PERMANENTLY retired for this
# session. When ALL workers are retired, the pipeline PAUSES
# and waits for user approval before touching the local IP.
# =========================================================

RATE_LIMIT_CODES = {1015}
PROXY_SKIP_CODES = {403, 502, 503}


async def fetch_waterfall(
    session: aiohttp.ClientSession,
    target_url: str,
    config: PipelineConfig,
    emitter: ProgressEmitter,
    pause_event: asyncio.Event,
) -> str | None:
    """
    Cloudflare Worker Rotation Fetcher.

    1. Iterates through config._active_workers.
    2. On 429/1015 → retires that worker permanently for this run.
    3. When all workers exhausted → emits 'proxy_exhausted',
       clears pause_event, and blocks until the user decides.
    4. If user approves local IP → single local attempt.
    5. If user cancels → returns None (cancel_event handles shutdown).
    """
    html = None

    # --- ATTEMPT: Cloudflare Workers (rotate through active list) ---
    workers_to_try = list(config._active_workers)  # snapshot
    for worker_url in workers_to_try:
        proxy_url = f"{worker_url}{urllib.parse.quote(target_url, safe=':/')}"
        try:
            request_headers = dict(config.http_headers)
            if SECRET_TOKEN:
                request_headers["x-proxy-token"] = SECRET_TOKEN

            async with session.get(
                proxy_url,
                headers=request_headers,
                timeout=aiohttp.ClientTimeout(total=config.timeout_seconds),
                ssl=False,
                allow_redirects=True,
            ) as response:
                if response.status in RATE_LIMIT_CODES:
                    # --- Worker hit daily limit: retire it ---
                    if worker_url in config._active_workers:
                        config._active_workers.remove(worker_url)
                        emitter.log(
                            "WARNING",
                            f"Worker rate-limited ({response.status}): "
                            f"{worker_url[:50]}… — retired. "
                            f"{len(config._active_workers)} workers remaining.",
                        )
                    continue  # Try next worker

                if response.status == 429:
                    # Burst rate limit (Cloudflare or target). Wait and try next proxy.
                    await asyncio.sleep(1.5)
                    continue

                if response.status in PROXY_SKIP_CODES or response.status >= 400:
                    continue  # Skip but don't retire

                # --- Success ---
                html = await response.text(errors="ignore")
                if html:
                    return html

        except Exception:
            continue  # Network error on this worker, try next

    # --- ALL WORKERS EXHAUSTED CHECK ---
    if not config._active_workers:
        # Emit exhaustion signal to frontend
        emitter.log(
            "ERROR",
            "All Cloudflare Workers exhausted. Waiting for user decision...",
        )
        emitter._emit("proxy_exhausted", {
            "message": "All Cloudflare Worker limits have been reached.",
        })

        # --- PAUSE the pipeline: block until user clicks a button ---
        pause_event.clear()
        await pause_event.wait()

        # --- User responded ---
        if config.allow_local_ip:
            emitter.log("INFO", f"Local IP fallback approved. Fetching: {target_url[:80]}…")
            try:
                async with session.get(
                    target_url,
                    headers=config.http_headers,
                    timeout=aiohttp.ClientTimeout(total=config.timeout_seconds),
                    ssl=False,
                    allow_redirects=True,
                ) as response:
                    if response.status < 400:
                        html = await response.text(errors="ignore")
            except Exception:
                pass
            return html
        else:
            # User chose cancel — return None, cancel_event handles shutdown
            return None

    # --- No worker returned HTML but workers still exist (non-rate-limit failures) ---
    # Only attempt local if user has already approved it
    if config.allow_local_ip:
        try:
            async with session.get(
                target_url,
                headers=config.http_headers,
                timeout=aiohttp.ClientTimeout(total=config.timeout_seconds),
                ssl=False,
                allow_redirects=True,
            ) as response:
                if response.status < 400:
                    html = await response.text(errors="ignore")
        except Exception:
            pass

    return html


# =========================================================
# HELPER FUNCTIONS (preserved from 2.0)
# =========================================================

def clean_domain_string(raw_domain: str) -> str:
    domain = raw_domain.strip().lower()
    domain = re.sub(r"^https?://", "", domain)
    domain = re.sub(r"^www\.", "", domain)
    return domain.split("/")[0]


def is_internal_and_not_media(target_url: str, base_domain: str) -> bool:
    try:
        parsed = urlparse(target_url)
        target_host = parsed.netloc.lower()
        if not (
            target_host == base_domain
            or target_host.endswith(f".{base_domain}")
            or not target_host
        ):
            return False
        if parsed.path.lower().endswith(MEDIA_EXTENSIONS):
            return False
        if parsed.scheme in ["javascript", "mailto", "tel"]:
            return False
        return True
    except Exception:
        return False


def clean_and_decode_url(url: str) -> str:
    parsed = urllib.parse.urlparse(str(url))
    url_no_fragment = urllib.parse.urlunparse(parsed._replace(fragment=""))
    return urllib.parse.unquote(url_no_fragment)


def is_valid_child_link(
    absolute_url: str, anchor_text: str, parent_domain: str,
) -> bool:
    parsed = urlparse(absolute_url)
    if parsed.netloc and not parsed.netloc.endswith(
        parent_domain.replace("www.", "")
    ):
        return False
    search_str = f"{absolute_url} {anchor_text}"
    if noise_pattern.search(search_str):
        return False
    if parsed.path.lower().endswith(".pdf"):
        return True
    if ANCHOR_JOB_HINTS.search(search_str):
        return True
    return False


# =========================================================
# DB WRITER LOOP — ZERO-LOCK SQLITE
# =========================================================
# CRITICAL: This is the ONLY coroutine that touches SQLite.
# All workers push (table, values) tuples onto db_queue.
# This loop batches writes and commits periodically.
# =========================================================

async def db_writer_loop(db_queue: asyncio.Queue, db_path: str):
    """
    Dedicated single-writer coroutine. Prevents 'database is locked' errors.

    CRITICAL: The entire body is wrapped in try/finally so that conn.close()
    is GUARANTEED even if the event loop crashes, the coroutine is cancelled,
    or an unhandled exception kills the pipeline thread.
    """

    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = get_db_connection(str(db_path), check_same_thread=False)

    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        cursor = conn.cursor()

        # --- Create tables ---
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stage2_discovered (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                base_domain  TEXT NOT NULL,
                discovered_url TEXT NOT NULL,
                anchor_text  TEXT,
                discovered_at TEXT DEFAULT (datetime('now'))
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stage3_filtered (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                base_domain  TEXT NOT NULL,
                filtered_url TEXT NOT NULL,
                anchor_text  TEXT,
                filtered_at  TEXT DEFAULT (datetime('now'))
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stage4_final_docs (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                parent_index_page TEXT NOT NULL,
                final_target_url  TEXT NOT NULL,
                anchor_text       TEXT,
                extracted_at      TEXT DEFAULT (datetime('now'))
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at      TEXT,
                completed_at    TEXT,
                status          TEXT,
                domains_loaded  INTEGER DEFAULT 0,
                urls_discovered INTEGER DEFAULT 0,
                urls_filtered   INTEGER DEFAULT 0,
                final_docs      INTEGER DEFAULT 0
            )
        """)
        # Phase 2: Unified document processing audit trail
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS document_refs (
                record_id          TEXT PRIMARY KEY,
                source_url         TEXT NOT NULL,
                doc_type           TEXT NOT NULL,
                prepared_file_path TEXT,
                archive_file_path  TEXT,
                processing_status  TEXT NOT NULL,
                workflow_state     TEXT DEFAULT 'UNREVIEWED',
                timestamp          TEXT DEFAULT (datetime('now')),
                UNIQUE(source_url)
            )
        """)
        
        # State tracking for auto sync
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS system_state (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        cursor.execute("INSERT OR IGNORE INTO system_state (key, value) VALUES ('last_turso_sync', '1970-01-01 00:00:00')")
        
        conn.commit()

        pending = 0
        BATCH_SIZE = 50

        while True:
            try:
                item = await asyncio.wait_for(db_queue.get(), timeout=2.0)
            except asyncio.TimeoutError:
                # Auto-commit partial batches on timeout
                if pending > 0:
                    conn.commit()
                    sync_db(conn)
                    pending = 0
                continue
            except asyncio.CancelledError:
                # Coroutine was cancelled — break out to finally
                logger.warning("db_writer_loop cancelled externally.")
                break

            # --- Poison pill: shut down ---
            if item is None:
                db_queue.task_done()
                break

            table, values = item

            try:
                if table == "stage2_discovered":
                    cursor.execute(
                        "INSERT INTO stage2_discovered "
                        "(base_domain, discovered_url, anchor_text) "
                        "VALUES (?, ?, ?)",
                        values,
                    )
                elif table == "stage3_filtered":
                    cursor.execute(
                        "INSERT INTO stage3_filtered "
                        "(base_domain, filtered_url, anchor_text) "
                        "VALUES (?, ?, ?)",
                        values,
                    )
                elif table == "stage4_final_docs":
                    cursor.execute(
                        "INSERT INTO stage4_final_docs "
                        "(parent_index_page, final_target_url, anchor_text) "
                        "VALUES (?, ?, ?)",
                        values,
                    )
                elif table == "document_refs":
                    cursor.execute(
                        "INSERT OR REPLACE INTO document_refs "
                        "(record_id, source_url, doc_type, "
                        " prepared_file_path, archive_file_path, "
                        " processing_status, timestamp) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?)",
                        values,
                    )
                elif table == "pipeline_start":
                    cursor.execute(
                        "INSERT INTO pipeline_runs (started_at, status) "
                        "VALUES (?, ?)",
                        values,
                    )
                elif table == "pipeline_end":
                    cursor.execute(
                        "UPDATE pipeline_runs "
                        "SET completed_at=?, status=?, domains_loaded=?, "
                        "    urls_discovered=?, urls_filtered=?, final_docs=? "
                        "WHERE id = (SELECT MAX(id) FROM pipeline_runs)",
                        values,
                    )
                elif table == "clear_stage":
                    # values is a single-element tuple: ('stage2_discovered',)
                    # Whitelist table names to prevent SQL injection
                    allowed = {
                        "stage2_discovered",
                        "stage3_filtered",
                        "stage4_final_docs",
                        "document_refs",
                    }
                    tbl = values[0]
                    if tbl in allowed:
                        cursor.execute(f"DELETE FROM {tbl}")
                elif table == "COMMIT":
                    if pending > 0:
                        conn.commit()
                        try:
                            sync_db(conn)
                        except NameError:
                            pass # If sync_db isn't defined, safely ignore
                        pending = 0

                if table != "COMMIT":
                    pending += 1
                    if pending >= BATCH_SIZE:
                        conn.commit()
                        try:
                            sync_db(conn)
                        except NameError:
                            pass
                        pending = 0

            except Exception as e:
                logger.error(f"DB write error [{table}]: {e}")

            db_queue.task_done()

        # --- Final flush before close ---
        if pending > 0:
            conn.commit()
            sync_db(conn)
        logger.info("db_writer_loop shut down cleanly.")

    except Exception as e:
        logger.error(f"db_writer_loop fatal error: {e}")

    finally:
        # GUARANTEED: Release the SQLite lock no matter what happened above.
        try:
            conn.close()
            logger.info("SQLite connection closed.")
        except Exception as e:
            logger.error(f"Error closing SQLite connection: {e}")


# =========================================================
# STAGE 2: HOMEPAGE DISCOVERY PROCESSOR
# =========================================================

async def _crawl_homepage(
    session: aiohttp.ClientSession,
    raw_domain: str,
    config: PipelineConfig,
    emitter: ProgressEmitter = None,
    pause_event: asyncio.Event = None,
) -> List[Tuple[str, str, str]]:
    """Crawl a single domain's homepage and extract internal links."""
    base_domain = clean_domain_string(raw_domain)
    if not base_domain:
        return []

    target_url = f"https://{base_domain}"
    html = await fetch_waterfall(session, target_url, config, emitter, pause_event)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    results = []

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].strip()
        if not href or href.startswith("#"):
            continue

        absolute_url = urljoin(target_url, href)
        anchor_text = a_tag.get_text(separator=" ", strip=True)

        if is_internal_and_not_media(absolute_url, base_domain):
            results.append((base_domain, absolute_url, anchor_text))

    return results


# =========================================================
# STAGE 4: DEEP-SCRAPE PROCESSOR
# =========================================================

async def _scrape_index_page(
    session: aiohttp.ClientSession,
    parent_url: str,
    config: PipelineConfig,
    emitter: ProgressEmitter = None,
    pause_event: asyncio.Event = None,
) -> List[Tuple[str, str, str]]:
    """Scrape an index page for child job document links."""
    parent_domain = urlparse(parent_url).netloc
    html = await fetch_waterfall(session, parent_url, config, emitter, pause_event)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    results = []

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].strip()
        if not href or href.startswith("#"):
            continue

        child_anchor = a_tag.get_text(separator=" ", strip=True)
        absolute_url = clean_and_decode_url(urljoin(parent_url, href))

        if is_valid_child_link(absolute_url, child_anchor, parent_domain):
            results.append((parent_url, absolute_url, child_anchor))

    return results


# =========================================================
# GENERIC WORKER POOL RUNNER
# =========================================================

async def _run_worker_pool(
    session: aiohttp.ClientSession,
    items: list,
    process_fn,
    db_table: str,
    db_queue: asyncio.Queue,
    config: PipelineConfig,
    emitter: ProgressEmitter,
    stage_num: int,
    pause_event: asyncio.Event,
    cancel_event: asyncio.Event,
) -> int:
    """
    Generic worker pool.
    Workers pull items from an asyncio.Queue, process them,
    and push results to db_queue for the db_writer_loop.
    Returns the total number of results written.
    """
    if not items:
        emitter.stage_progress(stage_num, 0, 0)
        return 0

    work_queue = asyncio.Queue()
    for item in items:
        work_queue.put_nowait(item)

    total = len(items)
    completed = 0
    written = 0
    counter_lock = asyncio.Lock()
    semaphore = asyncio.Semaphore(config.concurrency_limit)

    async def worker():
        nonlocal completed, written

        while not cancel_event.is_set():
            # --- Pause gate ---
            # If pause_event is cleared (paused), this blocks until resumed.
            # If cancel is set while paused, events.py also sets pause_event
            # so the worker wakes up and sees cancel_event.
            if not pause_event.is_set():
                await pause_event.wait()
                continue

            # --- Pull next item ---
            try:
                item = work_queue.get_nowait()
            except asyncio.QueueEmpty:
                return  # Queue exhausted, worker exits

            # --- Process with semaphore ---
            result_count = 0
            try:
                async with semaphore:
                    results = await process_fn(
                        session, item, config,
                        emitter=emitter, pause_event=pause_event,
                    )
                    result_count = len(results)
                    for result in results:
                        await db_queue.put((db_table, result))
            except Exception as e:
                emitter.log(
                    "WARNING",
                    f"Worker error on {str(item)[:60]}: {type(e).__name__}",
                )

            # --- Update counters ---
            async with counter_lock:
                completed += 1
                written += result_count
            emitter.stage_progress(stage_num, completed, total)

    # Spawn workers (capped at queue size)
    num_workers = min(config.concurrency_limit, total)
    workers = [asyncio.create_task(worker()) for _ in range(num_workers)]
    await asyncio.gather(*workers, return_exceptions=True)

    return written


# =========================================================
# STAGE 3: LEXICAL TRIAGE & SANITIZATION
# =========================================================

def _lexical_filter_and_sanitize(
    db_path: str,
) -> Tuple[int, int, List[Tuple[str, str, str]]]:
    """
    Reads stage2_discovered from SQLite, applies 2D regex triage,
    sanitizes URLs, deduplicates, and returns filtered rows.

    Returns: (initial_count, final_count, list_of_filtered_row_tuples)
    """
    conn = get_db_connection(str(db_path), check_same_thread=False)
    df = pd.read_sql_query(
        "SELECT base_domain, discovered_url, anchor_text "
        "FROM stage2_discovered",
        conn,
    )
    conn.close()

    initial_count = len(df)
    if initial_count == 0:
        return 0, 0, []

    df["discovered_url"] = df["discovered_url"].astype(str)
    df["anchor_text"] = df["anchor_text"].fillna("").astype(str)

    # --- 2D Filter (URL + Anchor Text simultaneously) ---
    search_space = df["discovered_url"] + " " + df["anchor_text"]
    has_target = search_space.str.contains(target_pattern, regex=True)
    df_filtered = df[has_target].copy()

    search_space_filtered = (
        df_filtered["discovered_url"] + " " + df_filtered["anchor_text"]
    )
    has_noise = search_space_filtered.str.contains(noise_pattern, regex=True)
    df_final = df_filtered[~has_noise].copy()

    # --- Sanitize: decode hex, strip fragments, deduplicate ---
    df_final["sanitized_url"] = df_final["discovered_url"].apply(
        clean_and_decode_url
    )
    df_dedup = df_final.drop_duplicates(
        subset=["sanitized_url"], keep="first"
    ).copy()

    final_count = len(df_dedup)

    # Build result tuples for db_queue
    filtered_rows = [
        (row["base_domain"], row["sanitized_url"], row["anchor_text"])
        for _, row in df_dedup.iterrows()
    ]

    return initial_count, final_count, filtered_rows


# =========================================================
# MAIN PIPELINE ORCHESTRATOR
# =========================================================

async def run_pipeline(
    config: PipelineConfig,
    emitter: ProgressEmitter,
    pause_event: asyncio.Event,
    cancel_event: asyncio.Event,
    stages: list = [1, 2, 3, 4],
):
    """
    Main pipeline entry point. Runs all 4 stages sequentially.
    The db_writer_loop runs as a background task throughout.
    """

    emitter.pipeline_status("running")
    emitter.log("INFO", "══════════════════════════════════════════════════════")
    emitter.log("INFO", "SPLECTOR MEGA PIPELINE 4.0 (WORKER ROTATION)")
    emitter.log("INFO", "══════════════════════════════════════════════════════")

    # --- Initialize worker rotation state ---
    config._active_workers = list(config.cf_workers)
    config.allow_local_ip = False
    emitter.log(
        "INFO",
        f"Cloudflare Workers loaded: {len(config._active_workers)} proxies active.",
    )

    # --- Initialize DB queue and writer ---
    db_queue = asyncio.Queue()
    db_writer_task = asyncio.create_task(
        db_writer_loop(db_queue, config.abs_db_path)
    )

    stats = {
        "domains_loaded": 0,
        "urls_discovered": 0,
        "urls_filtered": 0,
        "final_docs": 0,
    }

    try:
        # Record pipeline run
        await db_queue.put((
            "pipeline_start",
            (datetime.utcnow().isoformat(), "running"),
        ))

        # Clear previous stage data
        await db_queue.put(("clear_stage", ("stage2_discovered",)))
        await db_queue.put(("clear_stage", ("stage3_filtered",)))
        await db_queue.put(("clear_stage", ("stage4_final_docs",)))

        # Wait for clears to flush
        await db_queue.put(("COMMIT", None))
        await db_queue.join()

        # =====================================================
        # STAGE 1: Load Domains
        # =====================================================
        if 1 in stages:
            emitter.stage_start(1, 1)
            emitter.log("INFO", "Stage 1: Loading reachable domains from Excel...")

            try:
                conn_r = get_db_connection(config.abs_db_path)
                valid_rows = pd.read_sql_query(
                    "SELECT domain FROM domains WHERE source_sheet = ? AND reachable = 1", 
                    conn_r, params=[config.input_sheet]
                )
                conn_r.close()
                
                domains = (
                    valid_rows["domain"]
                    .dropna()
                    .astype(str)
                    .str.strip()
                    .tolist()
                )
                stats["domains_loaded"] = len(domains)

                emitter.log(
                    "INFO",
                    f"Stage 1 Complete: Loaded {len(domains):,} reachable domains.",
                )
                emitter.stage_progress(1, 1, 1)
                emitter.stage_complete(1)
                emitter.stats_update(stats)

            except Exception as e:
                emitter.log("ERROR", f"Stage 1 Failed: {e}")
                emitter.pipeline_status("error")
                return

            if cancel_event.is_set():
                emitter.log("WARNING", "Pipeline cancelled by user.")
                emitter.pipeline_status("cancelled")
                return

        # =====================================================
        # STAGE 2: Homepage Discovery via Proxy Waterfall
        # =====================================================
        if 2 in stages:
            if "domains" not in locals():
                emitter.log("INFO", "Stage 1 bypassed. Loading domains directly from production sheet in SQLite...")
                conn_r = get_db_connection(config.abs_db_path)
                valid_rows_bypass = pd.read_sql_query(
                    "SELECT domain FROM domains WHERE source_sheet = ? AND reachable = 1", 
                    conn_r, params=[config.input_sheet]
                )
                conn_r.close()
                
                domains = valid_rows_bypass["domain"].dropna().astype(str).str.strip().tolist()
                stats["domains_loaded"] = len(domains)
                emitter.stats_update(stats)

            emitter.stage_start(2, len(domains))
            emitter.log(
                "INFO",
                f"Stage 2: Discovering internal links from {len(domains):,} domains...",
            )

            timeout = aiohttp.ClientTimeout(total=config.timeout_seconds + 5)
            connector = aiohttp.TCPConnector(
                limit=config.concurrency_limit,
                ssl=False,
                ttl_dns_cache=300,
            )

            async with aiohttp.ClientSession(
                timeout=timeout, connector=connector,
            ) as session:
                await _run_worker_pool(
                    session=session,
                    items=domains,
                    process_fn=_crawl_homepage,
                    db_table="stage2_discovered",
                    db_queue=db_queue,
                    config=config,
                    emitter=emitter,
                    stage_num=2,
                    pause_event=pause_event,
                    cancel_event=cancel_event,
                )

            # Flush all stage 2 writes before reading in stage 3
            await db_queue.put(("COMMIT", None))
            await db_queue.join()

            # Read final count from DB
            conn_r = get_db_connection(config.abs_db_path)
            discovered_count = conn_r.execute(
                "SELECT COUNT(*) FROM stage2_discovered"
            ).fetchone()[0]
            conn_r.close()

            stats["urls_discovered"] = discovered_count
            emitter.log(
                "INFO",
                f"Stage 2 Complete: Discovered {discovered_count:,} internal URLs.",
            )
            emitter.stage_complete(2)
            emitter.stats_update(stats)

            if cancel_event.is_set():
                emitter.log("WARNING", "Pipeline cancelled by user.")
                emitter.pipeline_status("cancelled")
                return

        # =====================================================
        # STAGE 3: Lexical Triage & Sanitization
        # =====================================================
        if 3 in stages:
            emitter.stage_start(3, 1)
            emitter.log(
                "INFO",
                "Stage 3: Applying 2D Lexical Triage & URL Sanitization...",
            )

            try:
                initial_count, final_count, filtered_rows = (
                    _lexical_filter_and_sanitize(config.abs_db_path)
                )

                # Write filtered results to DB via db_queue
                for row in filtered_rows:
                    await db_queue.put(("stage3_filtered", row))

                # Flush stage 3 writes
                await db_queue.put(("COMMIT", None))
                await db_queue.join()

                stats["urls_filtered"] = final_count
                emitter.log(
                    "INFO",
                    f"Stage 3 Complete: {initial_count:,} → {final_count:,} "
                    f"URLs after triage ({initial_count - final_count:,} dropped).",
                )
                emitter.stage_progress(3, 1, 1)
                emitter.stage_complete(3)
                emitter.stats_update(stats)

            except Exception as e:
                emitter.log("ERROR", f"Stage 3 Failed: {e}")
                if not config.continue_on_stage_error:
                    emitter.pipeline_status("error")
                    return

            if cancel_event.is_set():
                emitter.log("WARNING", "Pipeline cancelled by user.")
                emitter.pipeline_status("cancelled")
                return

        # =====================================================
        # STAGE 4: Level 3 Deep-Scrape
        # =====================================================
        if 4 in stages:
            conn_r = get_db_connection(config.abs_db_path)
            parent_urls_df = pd.read_sql_query(
                "SELECT filtered_url FROM stage3_filtered", conn_r,
            )
            conn_r.close()
            parent_urls = (
                parent_urls_df["filtered_url"]
                .dropna()
                .astype(str)
                .tolist()
            )

            emitter.stage_start(4, len(parent_urls))
            emitter.log(
                "INFO",
                f"Stage 4: Deep-scraping {len(parent_urls):,} index pages...",
            )

            connector2 = aiohttp.TCPConnector(
                limit=config.concurrency_limit,
                ssl=False,
                ttl_dns_cache=300,
            )
            timeout2 = aiohttp.ClientTimeout(total=config.timeout_seconds + 5)

            async with aiohttp.ClientSession(
                timeout=timeout2, connector=connector2,
            ) as session:
                await _run_worker_pool(
                    session=session,
                    items=parent_urls,
                    process_fn=_scrape_index_page,
                    db_table="stage4_final_docs",
                    db_queue=db_queue,
                    config=config,
                    emitter=emitter,
                    stage_num=4,
                    pause_event=pause_event,
                    cancel_event=cancel_event,
                )

            # Flush all stage 4 writes
            await db_queue.put(("COMMIT", None))
            await db_queue.join()

            conn_r = get_db_connection(config.abs_db_path)
            final_doc_count = conn_r.execute(
                "SELECT COUNT(*) FROM stage4_final_docs"
            ).fetchone()[0]
            conn_r.close()

            stats["final_docs"] = final_doc_count
            emitter.log(
                "INFO",
                f"Stage 4 Complete: Extracted {final_doc_count:,} final job documents.",
            )
            emitter.stage_complete(4)
            emitter.stats_update(stats)

        # =====================================================
        # FINALIZE
        # =====================================================
        emitter.log("INFO", "══════════════════════════════════════════════════════")
        emitter.log("INFO", "PIPELINE COMPLETE")
        emitter.log("INFO", f"  Domains loaded  : {stats['domains_loaded']:,}")
        emitter.log("INFO", f"  URLs discovered  : {stats['urls_discovered']:,}")
        emitter.log("INFO", f"  URLs filtered    : {stats['urls_filtered']:,}")
        emitter.log("INFO", f"  Final documents  : {stats['final_docs']:,}")
        emitter.log("INFO", "══════════════════════════════════════════════════════")

        # Update pipeline run record
        await db_queue.put((
            "pipeline_end",
            (
                datetime.utcnow().isoformat(),
                "completed",
                stats["domains_loaded"],
                stats["urls_discovered"],
                stats["urls_filtered"],
                stats["final_docs"],
            ),
        ))

        emitter.pipeline_status("completed")

    except asyncio.CancelledError:
        emitter.log("WARNING", "Pipeline was cancelled.")
        emitter.pipeline_status("cancelled")

    except Exception as e:
        emitter.log("ERROR", f"Pipeline failed: {type(e).__name__}: {e}")
        emitter.pipeline_status("error")

    finally:
        # Send poison pill to db_writer and wait for clean shutdown
        await db_queue.put(None)
        await db_writer_task
        emitter.log("INFO", "Database writer shut down cleanly.")
