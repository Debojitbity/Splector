"""
Splector Phase 2 — PDF Processing Engine

Architecture:
  - Downloading is I/O-bound → runs in the asyncio event loop
  - Text extraction & OCR are CPU-bound → offloaded to ProcessPoolExecutor
    via asyncio.get_running_loop().run_in_executor()

Pipeline per PDF:
  1. Async download raw binary to PDF_FOLDER
  2. Extract digital text (PyMuPDF/fitz) — first N pages only
  3. If text < threshold → OCR with Tesseract (multi-language)
  4. Sanitize to UTF-8 plain text
  5. Validate (10-char alphabetical minimum)
  6. Save clean text to PREPARED_DATA
  7. SHA-256 hash → archive raw to REFERENCE_STORE/{hash}.pdf
"""

from __future__ import annotations

import hashlib
import io
import logging
import os
import re
import shutil
from pathlib import Path
from datetime import datetime

import aiohttp
import aiofiles

from core.config import PipelineConfig
from core.snowflake import generate_id

logger = logging.getLogger("splector.pdf_parser")


# =========================================================
# FILENAME SANITIZER
# =========================================================

def _safe_filename_from_url(url: str) -> str:
    """Convert a URL into a filesystem-safe filename."""
    # Strip scheme
    clean = re.sub(r"^https?://", "", url)
    # Replace unsafe chars
    clean = re.sub(r'[<>:"|?*]', "_", clean)
    clean = clean.replace("/", "_")
    clean = clean.replace("\\", "_")
    # Truncate for Windows path safety
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
# PDF TEXT EXTRACTION (CPU-BOUND — runs in ProcessPoolExecutor)
# =========================================================

def extract_pdf_text(
    file_path: str,
    tesseract_cmd: str,
    ocr_languages: str,
    text_threshold: int,
    page_limit: int,
) -> str:
    """
    Extract text from a PDF file. Falls back to OCR if digital text
    is below the threshold.

    CRITICAL: This function is CPU-bound and MUST be called via
    run_in_executor(ProcessPoolExecutor, ...) to avoid blocking
    the asyncio event loop.

    Args:
        file_path: Absolute path to the PDF file.
        tesseract_cmd: Path to the Tesseract executable.
        ocr_languages: Tesseract language string (e.g. 'eng+hin').
        text_threshold: Char count below which OCR is triggered.
        page_limit: Maximum number of pages to process.

    Returns:
        Sanitized UTF-8 text string.

    Raises:
        Exception on unrecoverable errors (corrupt PDF, Tesseract missing, etc.)
    """
    import fitz  # PyMuPDF — imported here because this runs in a subprocess
    import pytesseract
    from PIL import Image

    # Configure Tesseract path for this process
    pytesseract.pytesseract.tesseract_cmd = tesseract_cmd

    # ---------------------------------------------------------
    # 1. TRIM: Extract up to first N pages
    # ---------------------------------------------------------
    source_doc = fitz.open(file_path)
    trimmed_doc = fitz.open()

    pages_to_keep = min(page_limit, len(source_doc))
    trimmed_doc.insert_pdf(source_doc, to_page=pages_to_keep - 1)
    source_doc.close()

    # ---------------------------------------------------------
    # 2. TRIAGE: Check for native digital text
    # ---------------------------------------------------------
    text_length = 0
    for page in trimmed_doc:
        text_length += len(page.get_text("text").strip())

    # ---------------------------------------------------------
    # 3. OCR (If Required): Rebuild as searchable PDF
    # ---------------------------------------------------------
    if text_length < text_threshold:
        logger.info(
            f"Digital text ({text_length} chars) below threshold "
            f"({text_threshold}). Routing to OCR: {file_path}"
        )
        searchable_pdf = fitz.open()

        for page in trimmed_doc:
            pix = page.get_pixmap(dpi=150)
            img_bytes = pix.tobytes("png")
            image = Image.open(io.BytesIO(img_bytes))

            pdf_page_bytes = pytesseract.image_to_pdf_or_hocr(
                image, extension="pdf", lang=ocr_languages
            )

            temp_page = fitz.open("pdf", pdf_page_bytes)
            searchable_pdf.insert_pdf(temp_page)
            temp_page.close()

        trimmed_doc.close()
        trimmed_doc = searchable_pdf

    # ---------------------------------------------------------
    # 4. EXTRACT FINAL TEXT
    # ---------------------------------------------------------
    raw_text = ""
    for page in trimmed_doc:
        raw_text += page.get_text("text")
    trimmed_doc.close()

    # ---------------------------------------------------------
    # 5. SANITIZE: Strip non-essential content, normalize whitespace
    #    Output is strictly UTF-8 (NOT ASCII — preserves regional chars)
    # ---------------------------------------------------------
    # Remove HTML tags if any leaked through
    clean = re.sub(r"<[^>]+>", "", raw_text)
    # Collapse all whitespace to single spaces
    clean = re.sub(r"\s+", " ", clean).strip()

    return clean


# =========================================================
# ASYNC PDF DOWNLOAD
# =========================================================

async def download_pdf(
    session: aiohttp.ClientSession,
    url: str,
    config: PipelineConfig,
) -> str | None:
    """
    Stream-download a PDF binary to the staging folder.

    Returns:
        Absolute path to the downloaded file, or None on failure.
    """
    os.makedirs(config.abs_phase2_pdf_folder, exist_ok=True)

    filename = _safe_filename_from_url(url) + ".pdf"
    file_path = os.path.join(config.abs_phase2_pdf_folder, filename)

    try:
        async with session.get(
            url,
            headers=config.http_headers,
            timeout=aiohttp.ClientTimeout(total=config.timeout_seconds + 15),
            ssl=False,
            allow_redirects=True,
        ) as response:
            if response.status >= 400:
                logger.warning(f"PDF download HTTP {response.status}: {url}")
                return None

            async with aiofiles.open(file_path, "wb") as f:
                async for chunk in response.content.iter_chunked(65536):
                    await f.write(chunk)

        return file_path

    except Exception as e:
        logger.error(f"PDF download failed [{type(e).__name__}]: {url}")
        return None


# =========================================================
# FULL PDF PROCESSING PIPELINE
# =========================================================

async def process_pdf(
    url: str,
    session: aiohttp.ClientSession,
    config: PipelineConfig,
    executor,
) -> dict:
    """
    Complete PDF processing pipeline for a single URL.

    Returns an audit record dict for the document_refs table:
        {record_id, source_url, doc_type, prepared_file_path,
         archive_file_path, processing_status, timestamp}
    """
    import asyncio

    record = {
        "record_id": generate_id(),
        "source_url": url,
        "doc_type": "PDF",
        "prepared_file_path": None,
        "archive_file_path": None,
        "processing_status": None,
        "timestamp": datetime.utcnow().isoformat(),
    }

    # --- 1. DOWNLOAD ---
    file_path = await download_pdf(session, url, config)
    if not file_path:
        record["processing_status"] = "ERROR_DOWNLOAD"
        return record

    loop = asyncio.get_running_loop()

    try:
        # --- 2. EXTRACT TEXT (CPU-bound → ProcessPoolExecutor) ---
        clean_text = await loop.run_in_executor(
            executor,
            extract_pdf_text,
            file_path,
            config.tesseract_cmd_path,
            config.phase2_ocr_languages,
            config.phase2_text_char_threshold,
            config.phase2_pdf_page_limit,
        )
    except Exception as e:
        logger.error(f"PDF extraction/OCR failed [{type(e).__name__}]: {url} — {e}")
        record["processing_status"] = "ERROR_OCR"
        # Still archive the raw file even on extraction failure
        try:
            sha256_hash = compute_sha256(file_path)
            archive_name = f"{sha256_hash}.pdf"
            archive_path = os.path.join(
                config.abs_phase2_reference_store, archive_name
            )
            os.makedirs(config.abs_phase2_reference_store, exist_ok=True)
            shutil.move(file_path, archive_path)
            record["archive_file_path"] = archive_path
        except Exception:
            pass
        return record

    # --- 3. VALIDATE: Alphabetical character threshold ---
    alpha_count = sum(1 for c in clean_text if c.isalpha())
    if alpha_count < config.phase2_min_char_threshold:
        record["processing_status"] = "REJECTED_LOW_CHAR_COUNT"
        logger.info(
            f"PDF rejected ({alpha_count} alpha chars < "
            f"{config.phase2_min_char_threshold}): {url}"
        )
        # Archive raw file anyway for audit trail
        try:
            sha256_hash = compute_sha256(file_path)
            archive_name = f"{sha256_hash}.pdf"
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
    text_filename = _safe_filename_from_url(url) + ".txt"
    text_path = os.path.join(config.abs_phase2_prepared_data, text_filename)

    with open(text_path, "w", encoding="utf-8") as f:
        f.write(clean_text)

    record["prepared_file_path"] = text_path

    # --- 5. ARCHIVE: SHA-256 hash → REFERENCE_STORE ---
    sha256_hash = compute_sha256(file_path)
    archive_name = f"{sha256_hash}.pdf"
    archive_path = os.path.join(config.abs_phase2_reference_store, archive_name)
    os.makedirs(config.abs_phase2_reference_store, exist_ok=True)

    # Atomic: move raw file to content-addressable storage
    # If hash already exists (duplicate), just remove the staging file
    if os.path.exists(archive_path):
        os.remove(file_path)
    else:
        shutil.move(file_path, archive_path)

    record["archive_file_path"] = archive_path
    record["processing_status"] = "SUCCESS"

    return record
