"""
Splector Phase 2 — HTML Processing Engine

Pipeline per HTML document:
  1. Async download raw response to HTML_FOLDER
  2. Parse DOM with BeautifulSoup, strip <script>, <style>, <header>, <footer>
  3. Extract readable inner text
  4. Sanitize to UTF-8 plain text with single-space tokenization
  5. Validate (10-char alphabetical minimum)
  6. Save clean text to PREPARED_DATA
  7. SHA-256 hash → archive raw to REFERENCE_STORE/{hash}.html
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import shutil
from datetime import datetime

import aiohttp
import aiofiles
from bs4 import BeautifulSoup

from core.config import PipelineConfig
from core.snowflake import generate_id

logger = logging.getLogger("splector.html_parser")


# =========================================================
# FILENAME SANITIZER
# =========================================================

def _safe_filename_from_url(url: str) -> str:
    """Convert a URL into a filesystem-safe filename."""
    clean = re.sub(r"^https?://", "", url)
    clean = re.sub(r'[<>:"|?*]', "_", clean)
    clean = clean.replace("/", "_")
    clean = clean.replace("\\", "_")
    return clean[:200]


# =========================================================
# SHA-256 HASHER
# =========================================================

def compute_sha256(file_path: str) -> str:
    """Compute SHA-256 hash of a file's raw binary content."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


# =========================================================
# HTML TEXT EXTRACTION
# =========================================================

def extract_html_text(file_path: str) -> str:
    """
    Parse an HTML file with BeautifulSoup, strip metadata/code elements,
    and return sanitized plain text.

    Strips: <script>, <style>, <header>, <footer>, <nav>, <noscript>,
            <meta>, <link>, <svg>, <button>

    Returns:
        Sanitized UTF-8 text string with single-space tokenization.
    """
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        html_content = f.read()

    soup = BeautifulSoup(html_content, "html.parser")

    # Remove unwanted elements per spec
    for tag in soup([
        "script",
        "style",
        "header",
        "footer",
        "nav",
        "noscript",
        "meta",
        "link",
        "svg",
        "button",
    ]):
        tag.extract()

    # Extract visible text
    raw_text = soup.get_text(separator=" ", strip=True)

    # Sanitize: collapse whitespace to single spaces, UTF-8 output
    clean = re.sub(r"\s+", " ", raw_text).strip()

    return clean


# =========================================================
# ASYNC HTML DOWNLOAD
# =========================================================

async def download_html(
    session: aiohttp.ClientSession,
    url: str,
    config: PipelineConfig,
) -> str | None:
    """
    Download raw HTML response to the staging folder.

    Returns:
        Absolute path to the downloaded file, or None on failure.
    """
    os.makedirs(config.abs_phase2_html_folder, exist_ok=True)

    filename = _safe_filename_from_url(url) + ".html"
    file_path = os.path.join(config.abs_phase2_html_folder, filename)

    try:
        async with session.get(
            url,
            headers=config.http_headers,
            timeout=aiohttp.ClientTimeout(total=config.timeout_seconds),
            ssl=False,
            allow_redirects=True,
        ) as response:
            if response.status >= 400:
                logger.warning(f"HTML download HTTP {response.status}: {url}")
                return None

            content_type = response.headers.get('Content-Type', '').lower()
            if 'application/pdf' in content_type:
                return "RETRY_AS_PDF"

            # Read as text, then write as UTF-8
            html_text = await response.text(errors="ignore")

            async with aiofiles.open(file_path, "w", encoding="utf-8") as f:
                await f.write(html_text)

        return file_path

    except Exception as e:
        logger.error(f"HTML download failed [{type(e).__name__}]: {url}")
        return None


# =========================================================
# FULL HTML PROCESSING PIPELINE
# =========================================================

async def process_html(
    url: str,
    session: aiohttp.ClientSession,
    config: PipelineConfig,
    emitter,
    record_id: str,
) -> dict:
    """
    Complete HTML processing pipeline for a single URL.

    Returns an audit record dict for the document_refs table:
        {record_id, source_url, doc_type, prepared_file_path,
         archive_file_path, processing_status, timestamp}
    """
    record = {
        "record_id": record_id,
        "source_url": url,
        "doc_type": "HTML",
        "prepared_file_path": None,
        "archive_file_path": None,
        "processing_status": None,
        "timestamp": datetime.utcnow().isoformat(),
    }

    # --- 1. DOWNLOAD ---
    file_path = await download_html(session, url, config)
    
    if file_path == "RETRY_AS_PDF":
        return {"processing_status": "RETRY_AS_PDF"}
        
    if not file_path:
        record["processing_status"] = "ERROR_DOWNLOAD"
        return record

    try:
        # --- 2. EXTRACT TEXT ---
        # HTML parsing is lightweight enough to run in the event loop.
        # No ProcessPoolExecutor needed here.
        clean_text = extract_html_text(file_path)
        emitter.log("INFO", f"[HTML - DIRECT] Parsed {url}")
    except Exception as e:
        logger.error(f"HTML extraction failed [{type(e).__name__}]: {url} — {e}")
        record["processing_status"] = "ERROR_DOWNLOAD"
        return record

    # --- 3. VALIDATE: Alphabetical character threshold ---
    alpha_count = sum(1 for c in clean_text if c.isalpha())
    if alpha_count < config.phase2_min_char_threshold:
        record["processing_status"] = "REJECTED_LOW_CHAR_COUNT"
        logger.info(
            f"HTML rejected ({alpha_count} alpha chars < "
            f"{config.phase2_min_char_threshold}): {url}"
        )
        # Archive raw file for audit trail — never silently drop
        try:
            sha256_hash = compute_sha256(file_path)
            archive_name = f"{sha256_hash}.html"
            archive_path = os.path.join(
                config.abs_phase2_reference_store, archive_name
            )
            os.makedirs(config.abs_phase2_reference_store, exist_ok=True)
            shutil.move(file_path, archive_path)
            record["archive_file_path"] = archive_path
        except Exception:
            pass
        return record

    # --- 4. SAVE CLEAN TEXT to PREPARED_DATA ---
    os.makedirs(config.abs_phase2_prepared_data, exist_ok=True)
    text_filename = f"{record_id}.txt"
    text_path = os.path.join(config.abs_phase2_prepared_data, text_filename)

    with open(text_path, "w", encoding="utf-8") as f:
        f.write(clean_text)

    record["prepared_file_path"] = text_path

    # --- 5. ARCHIVE: SHA-256 hash → REFERENCE_STORE ---
    sha256_hash = compute_sha256(file_path)
    archive_name = f"{sha256_hash}.html"
    archive_path = os.path.join(config.abs_phase2_reference_store, archive_name)
    os.makedirs(config.abs_phase2_reference_store, exist_ok=True)

    # Atomic: move raw file to content-addressable storage
    if os.path.exists(archive_path):
        os.remove(file_path)  # Duplicate — discard staging copy
    else:
        shutil.move(file_path, archive_path)

    record["archive_file_path"] = archive_path
    record["processing_status"] = "SUCCESS"

    return record
