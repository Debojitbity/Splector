"""
Splector Mega Pipeline 2.0 (Enterprise Architecture)

Features included:
1) Cloudflare Waterfall Routing (Proxy -> Local Fallback)
2) 2-Dimensional Lexical Triage (URL + Anchor Text)
3) Zero-Loss URL Sanitization (Preserves DOM context)
4) Level 3 Child Extraction (Grabs actual PDFs/HTMLs from notice boards)
"""

from __future__ import annotations

import asyncio
import csv
import logging
import os
import re
import urllib.parse
from dataclasses import dataclass
from typing import List

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
import aiofiles
from tqdm.asyncio import tqdm

# =========================================================
# CONFIGURATION
# =========================================================

@dataclass
class PipelineConfig:
    # --- CLOUDFLARE PROXY INTEGRATION ---
    # Add your worker URL here. Ensure it ends with '?target='
    # Leave empty string ("") to disable proxy and only use local IP.
    cf_worker_proxy_url: str = "https://your-worker-name.your-subdomain.workers.dev/?target="

    # --- INPUT/OUTPUT PATHS ---
    input_excel: str = os.path.join("data", "links.xlsx")
    input_sheet: str = "production"
    output_discovered_internal: str = os.path.join("data", "database", "discovered_internal_links.csv")
    output_filtered: str = os.path.join("data", "database", "filtered_urls.csv")
    output_sanitized: str = os.path.join("data", "database", "sanitized_filtered_urls.csv")
    output_final_job_documents: str = os.path.join("data", "database", "final_job_documents.csv")

    # --- NETWORK SETTINGS ---
    concurrency_limit: int = 80
    timeout_seconds: int = 15
    continue_on_stage_error: bool = True

    http_headers: dict = None
    
    def __post_init__(self):
        self.http_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }

CFG = PipelineConfig()

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)

def log_info(msg: str): logging.info(msg)
def log_warning(msg: str): logging.warning(msg)

# =========================================================
# GLOBAL FILTER DEFINITIONS
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
    r"bharti", r"niyukti", r"opportunity", r"appointment"
]

NOISE_KEYWORDS = [
    r"tender", r"syllabus", r"result", r"exam", r"student", r"gallery", 
    r"alumni", r"rti", r"about", r"contact", r"history", r"login", 
    r"register", r"forgot", r"event", r"photo", r"video", r"act-rule",
    r"archive", r"corrigendum", r"admit[-_]?card", r"comment",
    r"facebook", r"twitter", r"t\.me", r"whatsapp", r"youtube", 
    r"instagram", r"google", r"sarkariresult", r"cdn[-_]?cgi", 
    r"email[-_]?protected", r"wp[-_]?content", r"javax", r"action", 
    r"layout", r"ubermenu", r"noopener", r"noreferrer", r"sitelogo"
]

target_pattern = re.compile("|".join(TARGET_KEYWORDS), re.IGNORECASE)
noise_pattern = re.compile("|".join(NOISE_KEYWORDS), re.IGNORECASE)

ANCHOR_JOB_HINTS = re.compile(r"(apply|advt|advertisement|notification|post of|recruitment|vacancy|click here)", re.IGNORECASE)

# =========================================================
# CORE WATERFALL NETWORK FETCHER
# =========================================================

async def fetch_waterfall(session: aiohttp.ClientSession, target_url: str) -> str | None:
    """Attempts Cloudflare Edge Proxy first. Falls back to local IP if blocked."""
    html = None
    
    # ATTEMPT 1: Cloudflare Worker
    if CFG.cf_worker_proxy_url:
        proxy_url = f"{CFG.cf_worker_proxy_url}{urllib.parse.quote(target_url, safe=':/')}"
        try:
            async with session.get(proxy_url, headers=CFG.http_headers, timeout=CFG.timeout_seconds, ssl=False, allow_redirects=True) as response:
                # 1000 = CF Loop, 502 = Proxy Fail, 403 = WAF Block
                if response.status not in [403, 502, 503, 1000] and response.status < 400:
                    html = await response.text(errors="ignore")
        except Exception:
            pass # Proxy attempt failed, continue to fallback
            
    # ATTEMPT 2: Local Datacenter IP
    if not html:
        try:
            async with session.get(target_url, headers=CFG.http_headers, timeout=CFG.timeout_seconds, ssl=False, allow_redirects=True) as local_response:
                if local_response.status < 400:
                    html = await local_response.text(errors="ignore")
        except Exception:
            pass
            
    return html

# =========================================================
# HELPER FUNCTIONS
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
        if not (target_host == base_domain or target_host.endswith(f".{base_domain}") or not target_host):
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

# =========================================================
# STAGE 1 & 2: HOMEPAGE DISCOVERY
# =========================================================

async def crawl_homepage(session: aiohttp.ClientSession, raw_domain: str, semaphore: asyncio.Semaphore, lock: asyncio.Lock):
    base_domain = clean_domain_string(raw_domain)
    if not base_domain: return
    
    target_url = f"https://{base_domain}"
    
    async with semaphore:
        html = await fetch_waterfall(session, target_url)
        if not html: return
            
        soup = BeautifulSoup(html, "html.parser")
        valid_links = set()

        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"].strip()
            if not href or href.startswith("#"): continue
            
            absolute_url = urljoin(target_url, href)
            # EXTRACT ANCHOR TEXT
            anchor_text = a_tag.get_text(separator=" ", strip=True)

            if is_internal_and_not_media(absolute_url, base_domain):
                valid_links.add((base_domain, absolute_url, anchor_text))

        if valid_links:
            async with lock:
                async with aiofiles.open(CFG.output_discovered_internal, mode="a", encoding="utf-8-sig", newline="") as f:
                    writer = csv.writer(f)
                    for row in valid_links:
                        await writer.writerow(row)

# =========================================================
# STAGE 3: LEXICAL TRIAGE & SANITIZATION
# =========================================================

def lexical_filter_and_sanitize():
    """Applies Regex against URL AND Text, removes fragments, decodes Unicode, drops dupes."""
    log_info("Starting Lexical Filter & Sanitization...")
    
    df = pd.read_csv(CFG.output_discovered_internal)
    initial_count = len(df)
    
    # Clean NaNs and force strings
    df['discovered_internal_url'] = df['discovered_internal_url'].astype(str)
    df['anchor_text'] = df['anchor_text'].fillna("").astype(str)
    
    # 1. 2D Filter (Look at URL AND Text simultaneously)
    search_space = df['discovered_internal_url'] + " " + df['anchor_text']
    
    has_target = search_space.str.contains(target_pattern, regex=True)
    df_filtered = df[has_target].copy()
    
    search_space_filtered = df_filtered['discovered_internal_url'] + " " + df_filtered['anchor_text']
    has_noise = search_space_filtered.str.contains(noise_pattern, regex=True)
    df_final = df_filtered[~has_noise].copy()
    
    # Save the raw filtered list just in case
    os.makedirs(os.path.dirname(CFG.output_filtered), exist_ok=True)
    df_final.to_csv(CFG.output_filtered, index=False, encoding='utf-8-sig')
    
    # 2. Sanitize Data (Decode hex and strip #fragments)
    df_final['sanitized_url'] = df_final['discovered_internal_url'].apply(clean_and_decode_url)
    
    # Drop duplicates created by stripping fragments. Keeps the first valid anchor text.
    df_dedup = df_final.drop_duplicates(subset=['sanitized_url'], keep='first').copy()
    
    # Reorganize columns to drop old URL and rename new one
    df_dedup = df_dedup.drop(columns=['discovered_internal_url'])
    df_dedup = df_dedup.rename(columns={'sanitized_url': 'discovered_internal_url'})
    df_dedup = df_dedup[['base_domain', 'discovered_internal_url', 'anchor_text']] # Reorder
    
    final_count = len(df_dedup)
    
    os.makedirs(os.path.dirname(CFG.output_sanitized), exist_ok=True)
    df_dedup.to_csv(CFG.output_sanitized, index=False, encoding='utf-8-sig')
    
    log_info(f"Triage Complete. Dropped {initial_count - final_count:,} noise/duplicate links.")
    return initial_count, final_count

# =========================================================
# STAGE 4: LEVEL 3 INDEX DEEP-SCRAPE
# =========================================================

def is_valid_child_link(absolute_url: str, anchor_text: str, parent_domain: str) -> bool:
    parsed = urlparse(absolute_url)
    
    if parsed.netloc and not parsed.netloc.endswith(parent_domain.replace("www.", "")):
        return False
        
    search_str = f"{absolute_url} {anchor_text}"
    if noise_pattern.search(search_str):
        return False
        
    if parsed.path.lower().endswith('.pdf'):
        return True
        
    if ANCHOR_JOB_HINTS.search(search_str):
        return True
        
    return False

async def scrape_index_page(session: aiohttp.ClientSession, parent_url: str, semaphore: asyncio.Semaphore, lock: asyncio.Lock):
    parent_domain = urlparse(parent_url).netloc
    
    async with semaphore:
        html = await fetch_waterfall(session, parent_url)
        if not html: return

        soup = BeautifulSoup(html, "html.parser")
        valid_children = set()

        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"].strip()
            if not href or href.startswith("#"): continue
                
            child_anchor = a_tag.get_text(separator=" ", strip=True)
            absolute_url = clean_and_decode_url(urljoin(parent_url, href))

            if is_valid_child_link(absolute_url, child_anchor, parent_domain):
                valid_children.add((parent_url, absolute_url, child_anchor))

        if valid_children:
            async with lock:
                async with aiofiles.open(CFG.output_final_job_documents, mode="a", encoding="utf-8-sig", newline="") as f:
                    writer = csv.writer(f)
                    for row in valid_children:
                        await writer.writerow(row)

# =========================================================
# MAIN EXECUTION ORCHESTRATOR
# =========================================================

async def run_pipeline():
    print("==================================================")
    print("SPLECTOR MEGA PIPELINE 2.0 (WATERFALL EDITION)")
    print("==================================================\n")
    
    # -----------------------------------------------------
    # STAGE 1: Load Domains
    # -----------------------------------------------------
    try:
        df = pd.read_excel(CFG.input_excel, sheet_name=CFG.input_sheet)
        valid_rows = df[df["reachable"].astype(str).str.strip().str.lower() == "true"]
        domains = valid_rows["domain"].dropna().astype(str).str.strip().tolist()
        log_info(f"Stage 1 Complete: Loaded {len(domains):,} reachable domains.")
    except Exception as e:
        log_warning(f"Stage 1 Failed: {e}")
        return

    # -----------------------------------------------------
    # STAGE 2: Discovery Crawl
    # -----------------------------------------------------
    os.makedirs(os.path.dirname(CFG.output_discovered_internal), exist_ok=True)
    with open(CFG.output_discovered_internal, mode="w", encoding="utf-8-sig", newline="") as f:
        csv.writer(f).writerow(["base_domain", "discovered_internal_url", "anchor_text"])

    semaphore = asyncio.Semaphore(CFG.concurrency_limit)
    csv_write_lock = asyncio.Lock()
    timeout = aiohttp.ClientTimeout(total=CFG.timeout_seconds + 5)
    connector = aiohttp.TCPConnector(limit=CFG.concurrency_limit, ssl=False, ttl_dns_cache=300)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        log_info("Starting Stage 2: Homepage Discovery via Proxy Waterfall...")
        tasks = [crawl_homepage(session, domain, semaphore, csv_write_lock) for domain in domains]
        await tqdm.gather(*tasks, desc="Stage 2: Scanning Domains")

    # -----------------------------------------------------
    # STAGE 3: Lexical Triage & Sanitization
    # -----------------------------------------------------
    try:
        init_count, final_count = lexical_filter_and_sanitize()
    except Exception as e:
        log_warning(f"Stage 3 Failed: {e}")
        return

    # -----------------------------------------------------
    # STAGE 4: Level 3 Deep-Scrape
    # -----------------------------------------------------
    os.makedirs(os.path.dirname(CFG.output_final_job_documents), exist_ok=True)
    with open(CFG.output_final_job_documents, mode="w", encoding="utf-8-sig", newline="") as f:
        csv.writer(f).writerow(["parent_index_page", "final_target_url", "anchor_text"])

    try:
        df_sanitized = pd.read_csv(CFG.output_sanitized)
        parent_urls = df_sanitized['discovered_internal_url'].dropna().astype(str).tolist()
    except Exception as e:
        log_warning(f"Could not load sanitized URLs: {e}")
        return

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        log_info(f"Starting Stage 4: Deep-Scraping {len(parent_urls):,} Index Pages...")
        tasks = [scrape_index_page(session, url, semaphore, csv_write_lock) for url in parent_urls]
        await tqdm.gather(*tasks, desc="Stage 4: Extracting Children")

    print("\n==================================================")
    print("PIPELINE COMPLETE")
    print(f"Final Target Job Documents saved to: {CFG.output_final_job_documents}")
    print("==================================================")

if __name__ == "__main__":
    asyncio.run(run_pipeline())