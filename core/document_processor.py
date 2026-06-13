"""
Splector Phase 2 — Document Processing Orchestrator (Stage 5)

Reads final target URLs from Phase 1 (stage4_final_docs table),
classifies them as PDF or HTML, and dispatches to the appropriate
processing engine.

CRITICAL ARCHITECTURE:
  - Downloading is I/O-bound → asyncio event loop (aiohttp)
  - PDF text extraction/OCR is CPU-bound → ProcessPoolExecutor
  - A shared ProcessPoolExecutor is created once and reused across
    all PDF workers to avoid spawning excessive processes.

Integration:
  - Uses the existing db_writer_loop via db_queue for all SQLite writes
  - Uses ProgressEmitter for real-time WebSocket dashboard updates
  - Supports pause/cancel via asyncio.Events
"""

from __future__ import annotations

import asyncio
import collections
import logging
import os
import re
from concurrent.futures import ProcessPoolExecutor
from urllib.parse import urlparse

import aiohttp

from core.config import PipelineConfig
from core.emitter import ProgressEmitter
from core.pdf_parser import process_pdf
from core.html_parser import process_html
from core.db import get_db_connection

logger = logging.getLogger("splector.document_processor")


# =========================================================
# URL PRE-PROCESSING (Input Normalization)
# =========================================================

def _normalize_urls(db_path: str) -> list[str]:
    """
    Read target URLs from stage4_final_docs, validate, and deduplicate.

    Returns:
        List of unique, validated URLs (http:// or https:// only).
    """
    conn = get_db_connection(db_path, check_same_thread=False)

    # Pre-initialize document_refs in case of a cold start
    conn.execute("""
        CREATE TABLE IF NOT EXISTS document_refs (
            record_id          TEXT PRIMARY KEY,
            source_url         TEXT NOT NULL,
            doc_type           TEXT NOT NULL,
            prepared_file_path TEXT,
            archive_file_path  TEXT,
            processing_status  TEXT NOT NULL,
            timestamp          TEXT DEFAULT (datetime('now')),
            UNIQUE(source_url)
        )
    """)
    conn.commit()

    cursor = conn.execute(
        "SELECT DISTINCT final_target_url FROM stage4_final_docs "
        "WHERE final_target_url NOT IN ("
        "SELECT source_url FROM document_refs WHERE processing_status IN ('SUCCESS', 'SEED_METADATA_ONLY', 'REJECTED_UNSUPPORTED'))"
    )
    raw_urls = [row[0] for row in cursor.fetchall() if row[0]]
    conn.close()

    # Validation: keep only http/https URLs
    url_pattern = re.compile(r"^https?://", re.IGNORECASE)
    valid_urls = [
        url.strip() for url in raw_urls
        if url.strip() and url_pattern.match(url.strip())
    ]

    # Deduplicate (preserving order)
    seen = set()
    unique_urls = []
    for url in valid_urls:
        if url not in seen:
            seen.add(url)
            unique_urls.append(url)

    return unique_urls


def _classify_url(url: str) -> str:
    """
    Classify a URL as 'PDF', 'UNSUPPORTED', or 'HTML' based on its path extension.
    """
    parsed = urlparse(url)
    path_lower = parsed.path.lower()
    
    bad_exts = ('.zip', '.exe', '.rar', '.tar', '.gz', '.7z', 
                '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
                '.jpg', '.jpeg', '.png', '.gif', '.mp4', '.mp3')

    if path_lower.endswith(".pdf"):
        return "PDF"
    elif path_lower.endswith(bad_exts):
        return "UNSUPPORTED"
    else:
        return "HTML"


# =========================================================
# WORKER FUNCTION
# =========================================================

async def _process_single_document(
    session: aiohttp.ClientSession,
    url: str,
    config: PipelineConfig,
    executor: ProcessPoolExecutor,
    emitter: ProgressEmitter,
) -> dict:
    """
    Process a single document URL. Routes to PDF or HTML engine
    based on URL classification.

    Returns:
        Audit record dict for document_refs table.
    """
    doc_type = _classify_url(url)
    
    from core.snowflake import generate_id
    record_id = generate_id()

    if doc_type == "UNSUPPORTED":
        from datetime import datetime, timezone
        return {
            "record_id": record_id,
            "source_url": url,
            "doc_type": "UNSUPPORTED",
            "prepared_file_path": None,
            "archive_file_path": None,
            "processing_status": "REJECTED_UNSUPPORTED",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    if not config.enable_file_downloads:
        # Instantly bypass network I/O and CPU to seed the database
        from datetime import datetime, timezone

        return {
            "record_id": record_id,
            "source_url": url,
            "doc_type": doc_type,
            "prepared_file_path": None,
            "archive_file_path": None,
            "processing_status": "SEED_METADATA_ONLY",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    if doc_type == "PDF":
        return await process_pdf(url, session, config, executor, emitter, record_id)
    else:
        record = await process_html(url, session, config, emitter, record_id)

        # Catch disguised PDFs discovered during the HTTP handshake
        if record.get("processing_status") == "RETRY_AS_PDF":
            # Log the dynamic reroute for visibility
            emitter.log("INFO", f"[DYNAMIC REROUTE] Disguised PDF detected: {url}")
            return await process_pdf(url, session, config, executor, emitter, record_id)

        return record


# =========================================================
# MAIN ORCHESTRATOR
# =========================================================

async def run_document_processing(
    config: PipelineConfig,
    emitter: ProgressEmitter,
    pause_event: asyncio.Event,
    cancel_event: asyncio.Event,
    db_queue: asyncio.Queue,
):
    """
    Stage 5: Document Processing Pipeline.

    Reads URLs from stage4_final_docs, processes each through the
    PDF or HTML engine, and writes audit records to document_refs
    via the shared db_queue.

    Args:
        config: Pipeline configuration.
        emitter: WebSocket progress emitter.
        pause_event: asyncio.Event — set = running, clear = paused.
        cancel_event: asyncio.Event — set = cancel requested.
        db_queue: Shared queue for the db_writer_loop.
    """
    stage_num = 5

    emitter.log("INFO", "══════════════════════════════════════════════════════")
    emitter.log("INFO", "STAGE 5: DOCUMENT PROCESSING PIPELINE")
    emitter.log("INFO", "══════════════════════════════════════════════════════")

    # --- Ensure output directories exist ---
    for dir_path in [
        config.abs_phase2_pdf_folder,
        config.abs_phase2_html_folder,
        config.abs_phase2_prepared_data,
        config.abs_phase2_reference_store,
    ]:
        os.makedirs(dir_path, exist_ok=True)

    # --- Pre-Processing: Normalize URLs ---
    emitter.log("INFO", "Loading target URLs from stage4_final_docs...")

    try:
        urls = _normalize_urls(config.abs_db_path)
    except Exception as e:
        emitter.log("ERROR", f"Failed to load URLs: {e}")
        return

    total = len(urls)
    if total == 0:
        emitter.log("WARNING", "No valid URLs found in stage4_final_docs. Skipping.")
        emitter.stage_progress(stage_num, 0, 0)
        return

    pdf_count = sum(1 for u in urls if _classify_url(u) == "PDF")
    html_count = total - pdf_count

    emitter.log(
        "INFO",
        f"Pre-processing complete: {total:,} unique URLs "
        f"({pdf_count:,} PDFs, {html_count:,} HTML pages)",
    )
    emitter.stage_start(stage_num, total)

    # --- Circuit Breaker State ---
    domain_consecutive_errors = collections.defaultdict(int)
    blacklisted_for_this_run = set()

    # --- Create shared ProcessPoolExecutor for CPU-bound OCR ---
    executor = ProcessPoolExecutor(max_workers=config.phase2_ocr_max_workers)

    # --- Stats counters ---
    completed = 0
    success_count = 0
    rejected_count = 0
    error_count = 0

    # --- aiohttp session ---
    connector = aiohttp.TCPConnector(
        limit=config.phase2_download_concurrency,
        ssl=False,
        ttl_dns_cache=300,
    )
    timeout = aiohttp.ClientTimeout(total=config.timeout_seconds + 20)

    # --- Worker pool ---
    work_queue = asyncio.Queue()
    for url in urls:
        work_queue.put_nowait(url)

    counter_lock = asyncio.Lock()
    semaphore = asyncio.Semaphore(config.phase2_download_concurrency)

    async def worker(session: aiohttp.ClientSession):
        nonlocal completed, success_count, rejected_count, error_count

        while not cancel_event.is_set():
            # Pause gate
            if not pause_event.is_set():
                await pause_event.wait()
                continue

            # Pull next URL
            try:
                url = work_queue.get_nowait()
            except asyncio.QueueEmpty:
                return  # Queue exhausted

            domain = urlparse(url).netloc
            if domain in blacklisted_for_this_run:
                # CRITICAL: Do NOT push an error record.
                async with counter_lock:
                    completed += 1
                emitter.stage_progress(stage_num, completed, total)
                continue

            # Process with semaphore
            try:
                async with semaphore:
                    record = await _process_single_document(
                        session, url, config, executor, emitter
                    )

                    status = record["processing_status"]
                    if status == "SUCCESS" or status.startswith("REJECTED"):
                        domain_consecutive_errors[domain] = 0
                    else:
                        domain_consecutive_errors[domain] += 1
                        if domain_consecutive_errors[domain] >= 5:
                            if domain not in blacklisted_for_this_run:
                                blacklisted_for_this_run.add(domain)
                                emitter.log("WARNING", f"[CIRCUIT BREAKER] Domain {domain} failed 5 times. Skipping remaining URLs for this run.")

                    # Push audit record to db_queue
                    await db_queue.put((
                        "document_refs",
                        (
                            record["record_id"],
                            record["source_url"],
                            record["doc_type"],
                            record["prepared_file_path"],
                            record["archive_file_path"],
                            record["processing_status"],
                            record["timestamp"],
                        ),
                    ))

                    # Update stats
                    async with counter_lock:
                        completed += 1
                        status = record["processing_status"]
                        if status == "SUCCESS":
                            success_count += 1
                        elif status.startswith("REJECTED"):
                            rejected_count += 1
                        else:
                            error_count += 1

                    emitter.stage_progress(stage_num, completed, total)

            except Exception as e:
                emitter.log(
                    "WARNING",
                    f"Worker error on {url[:60]}: {type(e).__name__}",
                )
                async with counter_lock:
                    completed += 1
                    error_count += 1
                emitter.stage_progress(stage_num, completed, total)

    # --- Spawn workers ---
    async with aiohttp.ClientSession(
        timeout=timeout, connector=connector,
    ) as session:
        num_workers = min(config.phase2_download_concurrency, total)
        workers = [
            asyncio.create_task(worker(session))
            for _ in range(num_workers)
        ]
        await asyncio.gather(*workers, return_exceptions=True)

    # --- Shutdown executor ---
    executor.shutdown(wait=False)

    # --- Flush all DB writes ---
    await db_queue.join()

    # --- Log final stats ---
    emitter.log("INFO", "══════════════════════════════════════════════════════")
    emitter.log("INFO", "STAGE 5 COMPLETE")
    emitter.log("INFO", f"  Total processed : {completed:,}")
    emitter.log("INFO", f"  Successful      : {success_count:,}")
    emitter.log("INFO", f"  Rejected (low)  : {rejected_count:,}")
    emitter.log("INFO", f"  Errors          : {error_count:,}")
    emitter.log("INFO", "══════════════════════════════════════════════════════")

    emitter.stage_complete(stage_num)
    emitter.stats_update({
        "docs_processed": completed,
        "docs_success": success_count,
        "docs_rejected": rejected_count,
        "docs_errors": error_count,
    })
