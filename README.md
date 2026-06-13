# Splector

I needed a way to gather job updates from thousands of government portals every single day. I don't have an enterprise budget, I don't have a team, and I really don't want to pay for a server.

Hence this project.

This isn't a standard, heavy-duty scraping cluster. It’s a quiet, zero-cost, serverless data pipeline. It is designed to be highly concurrent, fault-tolerant, and completely invisible, running entirely on free-tier infrastructure.

## The Blueprint

The goal is simple: extract clean, structured data from messy, rate-limited websites without spending a penny.

My CRON server is Github Actions, proxy is handled by Cloudflare Workers, database is managed at Turso and my colleagues are *Antigravity* and *Codex*.

## Current Status

**Work in Progress.** Just getting started. Truly I'm not going to push 100 files 10 times a day just because AI is with me, I want to do things on my own with the certainty of an Intelligent Tool is there to back me up.

I'm just building a massive pipeline the only way I know how: with absolute resourcefulness. If you are here looking for a massive Kubernetes cluster, you are in the wrong place. If you want to see how to duct-tape free tools into a real system, stick around.

## Development Phase

### Phase 1: Discovery & Normalization

Objective: Map target government domains and isolate unique, actionable document URLs.

Architecture: Asynchronous web scraper utilizing aiohttp and a 4-tier SQLite staging schema (stage1 to stage4).

Transient State Management: Intermediate scratchpad tables are strictly pruned per run to prevent link rot.

Deduplication Moat: Extracted URLs are cross-referenced against the permanent document_refs ledger. URLs marked SUCCESS, SEED_METADATA_ONLY, or REJECTED_UNSUPPORTED are dropped instantly to prevent redundant network I/O.

Fault Tolerance: In-memory circuit breakers automatically drop domains after 5 consecutive timeouts, preserving pipeline throughput.

### Phase 2: Document Processing & Extraction (Stage 5)
Objective: Download files, extract clean text, and synchronize metadata to the cloud.

Smart Classification: URLs are pre-filtered to instantly reject unsupported binary payloads (.zip, .exe, etc.). Dynamic HTTP Content-Type header inspection catches disguised PDFs (e.g., download.php) and safely reroutes them from the HTML parser to the PDF engine.

CPU Optimization: An advanced routing heuristic checks the first 3 pages of a PDF. Digital PDFs bypass the CPU bottleneck and parse via PyMuPDF in milliseconds. Scanned images are routed to a ProcessPoolExecutor utilizing 8 to 10 concurrent Tesseract workers.

Data Standardization: Extracted text is saved locally using strict Snow ID (UUID) filenames (e.g., [record_id].txt) to completely eliminate Windows OS MAX_PATH truncation errors.

Cloud Operations: A background Delta-Sync loop checks the local high-water mark every 10 minutes and pushes new rows to the Turso edge database via HTTPS REST batches.