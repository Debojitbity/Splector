"""
Splector Pipeline Configuration

Centralized config that loads from config.json with sensible defaults.
The dashboard settings modal writes updates back to this file.
"""

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

# =========================================================
# BASE PATHS
# =========================================================

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_FILE = BASE_DIR / "config.json"

# =========================================================
# SAVEABLE FIELDS (persisted to config.json)
# =========================================================

SAVEABLE_FIELDS = [
    "cf_worker_proxy_url",
    "input_excel",
    "input_sheet",
    "concurrency_limit",
    "timeout_seconds",
    "db_path",
    "continue_on_stage_error",
    "sheets",
    # Phase 2: Document Processing
    "tesseract_cmd_path",
    "phase2_pdf_folder",
    "phase2_html_folder",
    "phase2_prepared_data",
    "phase2_reference_store",
    "phase2_ocr_languages",
    "phase2_min_char_threshold",
    "phase2_text_char_threshold",
    "phase2_pdf_page_limit",
]


# =========================================================
# PIPELINE CONFIG DATACLASS
# =========================================================

@dataclass
class PipelineConfig:
    cf_worker_proxy_url: str = ""
    input_excel: str = "data/links.xlsx"
    input_sheet: str = "production"
    concurrency_limit: int = 80
    timeout_seconds: int = 15
    db_path: str = "data/crawler.db"
    continue_on_stage_error: bool = True
    sheets: List[str] = field(
        default_factory=lambda: ["production", "temporary", "unstable"]
    )

    # --- Phase 2: Document Processing ---
    tesseract_cmd_path: str = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
    phase2_pdf_folder: str = "data/phase2_pdf"
    phase2_html_folder: str = "data/phase2_html"
    phase2_prepared_data: str = "data/phase2_prepared"
    phase2_reference_store: str = "data/phase2_archive"
    phase2_ocr_languages: str = "eng+hin"
    phase2_min_char_threshold: int = 10
    phase2_text_char_threshold: int = 50
    phase2_pdf_page_limit: int = 3

    # --- Computed at runtime (not saved to config.json) ---
    http_headers: dict = field(default_factory=dict, repr=False)

    def __post_init__(self):
        if not self.http_headers:
            self.http_headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/149.0.0.0 Safari/537.36"
                ),
                "Accept": (
                    "text/html,application/xhtml+xml,application/xml;"
                    "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
                ),
                "Accept-Language": "en-US,en;q=0.9",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            }

    # --- Resolved absolute paths ---

    @property
    def base_dir(self) -> str:
        return str(BASE_DIR)

    @property
    def abs_db_path(self) -> str:
        return str(BASE_DIR / self.db_path)

    @property
    def abs_input_excel(self) -> str:
        return str(BASE_DIR / self.input_excel)

    # --- Phase 2 resolved paths ---

    @property
    def abs_phase2_pdf_folder(self) -> str:
        return str(BASE_DIR / self.phase2_pdf_folder)

    @property
    def abs_phase2_html_folder(self) -> str:
        return str(BASE_DIR / self.phase2_html_folder)

    @property
    def abs_phase2_prepared_data(self) -> str:
        return str(BASE_DIR / self.phase2_prepared_data)

    @property
    def abs_phase2_reference_store(self) -> str:
        return str(BASE_DIR / self.phase2_reference_store)

    def to_dict(self) -> dict:
        """Return only saveable fields for JSON serialization."""
        return {k: getattr(self, k) for k in SAVEABLE_FIELDS}


# =========================================================
# LOAD / SAVE
# =========================================================

def load_config() -> PipelineConfig:
    """Load config from config.json, falling back to defaults."""
    data = {}
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

    # Filter to only known dataclass fields
    valid_keys = {f for f in PipelineConfig.__dataclass_fields__}
    filtered = {k: v for k, v in data.items() if k in valid_keys}
    return PipelineConfig(**filtered)


def save_config(updates: dict) -> PipelineConfig:
    """Merge updates into config.json and return the new config."""
    current = {}
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            current = json.load(f)

    # Only persist saveable fields
    for key, value in updates.items():
        if key in SAVEABLE_FIELDS:
            current[key] = value

    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(current, f, indent=2)

    return load_config()
