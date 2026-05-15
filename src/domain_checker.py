import asyncio
import aiohttp
import pandas as pd
import socket
import time
import shutil
import random

from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse

from aiohttp import (
    ClientConnectorError,
    ClientSSLError,
    ClientResponseError,
    ClientOSError,
    InvalidURL,
    ServerDisconnectedError,
    TooManyRedirects
)

# =========================================================
# BASE PATHS
# =========================================================

BASE_DIR = Path(__file__).resolve().parent.parent

EXCEL_FILE = BASE_DIR / "data" / "links.xlsx"

TEMP_FILE = BASE_DIR / "data" / "links_temp.xlsx"

# =========================================================
# CONFIG
# =========================================================

SHEETS = [
    "production",
    "temporary",
    "unstable"
]

CONCURRENCY_LIMIT = 50
TIMEOUT_SECONDS = 15

# =========================================================
# ALLOWED REDIRECT SUFFIXES
# =========================================================

ALLOWED_SUFFIXES = [
    "gov.in",
    "nic.in",
    "ac.in",
    "edu.in",
    "res.in",
    "mil.in"
]

# =========================================================
# USER AGENTS
# =========================================================

USER_AGENTS = [

    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),

    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) "
        "Gecko/20100101 Firefox/125.0"
    ),

    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36 Edg/124.0"
    ),

    (
        "Mozilla/5.0 (Linux; Android 13; SM-S918B) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Mobile Safari/537.36"
    ),

    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.4 Safari/605.1.15"
    )
]

# =========================================================
# BACKUP
# =========================================================

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

BACKUP_FILE = (
    BASE_DIR /
    "data" /
    f"links_snapshot_{timestamp}.xlsx"
)

print(f"\n[BACKUP] Creating snapshot: {BACKUP_FILE.name}")

shutil.copy2(EXCEL_FILE, BACKUP_FILE)

# =========================================================
# SANITIZER
# =========================================================

def sanitize_excel(value):

    if not isinstance(value, str):
        return value

    value = value.strip()

    if value.startswith(("=", "+", "-", "@")):
        value = "'" + value

    return value

# =========================================================
# DOMAIN POLICY
# =========================================================

def is_allowed_domain(url):

    try:

        parsed = urlparse(url)

        hostname = parsed.hostname

        if not hostname:
            return False

        hostname = hostname.lower()

        return any(
            hostname == suffix or
            hostname.endswith("." + suffix)
            for suffix in ALLOWED_SUFFIXES
        )

    except Exception:
        return False

# =========================================================
# HEALTH CLASSIFICATION
# =========================================================

def classify_health(reason, reachable):

    if reachable:
        return "LIVE"

    if reason == "DNS Failure":
        return "DEAD"

    if reason in [
        "Timeout",
        "Connection Failed",
        "SSL Error",
        "ServerDisconnectedError",
        "ClientResponseError",
        "ClientOSError",
        "Too Many Redirects",
        "External Redirect Blocked"
    ]:
        return "UNSTABLE"

    return "UNKNOWN"

# =========================================================
# REQUEST
# =========================================================

async def check_domain(session, domain, semaphore):

    async with semaphore:

        result = {
            "domain": sanitize_excel(domain),
            "reachable": False,
            "status_code": "",
            "reason": "",
            "response_time_ms": "",
            "final_url": ""
        }

        urls = [
            f"https://{domain}",
            f"http://{domain}"
        ]

        for url in urls:

            start = time.perf_counter()

            try:

                async with session.get(
                    url,
                    timeout=TIMEOUT_SECONDS,
                    allow_redirects=True,
                    ssl=False
                ) as response:

                    final_url = str(response.url)

                    if not is_allowed_domain(final_url):

                        result["reason"] = (
                            "External Redirect Blocked"
                        )

                        result["final_url"] = sanitize_excel(
                            final_url
                        )

                        return result

                    await response.content.read(256)

                    elapsed = round(
                        (time.perf_counter() - start) * 1000,
                        2
                    )

                    result.update({
                        "reachable": True,
                        "status_code": response.status,
                        "reason": "OK",
                        "response_time_ms": elapsed,
                        "final_url": sanitize_excel(
                            final_url
                        )
                    })

                    print(
                        f"[OK] "
                        f"{domain} "
                        f"{response.status} "
                        f"{elapsed}ms"
                    )

                    return result

            except asyncio.TimeoutError:
                result["reason"] = "Timeout"

            except ClientSSLError:
                result["reason"] = "SSL Error"

            except ClientConnectorError as e:

                if isinstance(e.os_error, socket.gaierror):
                    result["reason"] = "DNS Failure"
                else:
                    result["reason"] = "Connection Failed"

            except TooManyRedirects:
                result["reason"] = "Too Many Redirects"

            except ServerDisconnectedError:
                result["reason"] = "ServerDisconnectedError"

            except ClientResponseError:
                result["reason"] = "ClientResponseError"

            except ClientOSError:
                result["reason"] = "ClientOSError"

            except InvalidURL:
                result["reason"] = "InvalidURL"

            except Exception as e:
                result["reason"] = type(e).__name__

        print(f"[FAIL] {domain} -> {result['reason']}")

        return result

# =========================================================
# PROCESS SHEET
# =========================================================

async def process_sheet(sheet_name, old_df, session):

    print(f"\n{'='*80}")
    print(f"PROCESSING: {sheet_name.upper()}")
    print(f"{'='*80}")

    domains = (
        old_df["domain"]
        .dropna()
        .astype(str)
        .str.strip()
        .unique()
        .tolist()
    )

    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

    tasks = [
        check_domain(session, domain, semaphore)
        for domain in domains
    ]

    results = []

    for task in asyncio.as_completed(tasks):

        result = await task
        results.append(result)

    new_df = pd.DataFrame(results)

    comparison = old_df.merge(
        new_df,
        on="domain",
        how="outer",
        suffixes=("_old", "_new")
    )

    recovered = len(
        comparison[
            (comparison["reachable_old"] == False) &
            (comparison["reachable_new"] == True)
        ]
    )

    degraded = len(
        comparison[
            (comparison["reachable_old"] == True) &
            (comparison["reachable_new"] == False)
        ]
    )

    reason_changed = len(
        comparison[
            comparison["reason_old"] !=
            comparison["reason_new"]
        ]
    )

    new_df["health"] = new_df.apply(
        lambda row: classify_health(
            row["reason"],
            row["reachable"]
        ),
        axis=1
    )

    summary = {
        "Sheet": sheet_name,
        "Total Domains": len(new_df),
        "Live": len(new_df[new_df["health"] == "LIVE"]),
        "Dead": len(new_df[new_df["health"] == "DEAD"]),
        "Unstable": len(
            new_df[new_df["health"] == "UNSTABLE"]
        ),
        "Recovered": recovered,
        "Degraded": degraded,
        "Reason Changed": reason_changed,
        "Average Response (ms)": round(
            pd.to_numeric(
                new_df["response_time_ms"],
                errors="coerce"
            ).mean(),
            2
        )
    }

    return new_df, summary

# =========================================================
# MAIN
# =========================================================

async def main():

    print("\n[LOADING WORKBOOK]")

    excel = pd.ExcelFile(
        EXCEL_FILE,
        engine="openpyxl"
    )

    timeout = aiohttp.ClientTimeout(
        total=TIMEOUT_SECONDS
    )

    connector = aiohttp.TCPConnector(
        ssl=False,
        limit=CONCURRENCY_LIMIT,
        ttl_dns_cache=300
    )

    headers = {
        "User-Agent": random.choice(USER_AGENTS)
    }

    global_summary = []

    async with aiohttp.ClientSession(
        headers=headers,
        timeout=timeout,
        connector=connector
    ) as session:

        with pd.ExcelWriter(
            TEMP_FILE,
            engine="openpyxl"
        ) as writer:

            total_domains = 0
            total_live = 0
            total_dead = 0
            total_unstable = 0

            for sheet in SHEETS:

                if sheet not in excel.sheet_names:
                    continue

                old_df = pd.read_excel(
                    EXCEL_FILE,
                    sheet_name=sheet,
                    engine="openpyxl"
                )

                new_df, summary = await process_sheet(
                    sheet,
                    old_df,
                    session
                )

                export_df = new_df[[
                    "domain",
                    "reachable",
                    "status_code",
                    "reason",
                    "response_time_ms",
                    "final_url"
                ]]

                export_df.to_excel(
                    writer,
                    sheet_name=sheet,
                    index=False
                )

                global_summary.append(summary)

                total_domains += summary["Total Domains"]
                total_live += summary["Live"]
                total_dead += summary["Dead"]
                total_unstable += summary["Unstable"]

            health_score = round(
                (total_live / total_domains) * 100,
                2
            )

            summary_df = pd.DataFrame(global_summary)

            totals_df = pd.DataFrame([
                {
                    "Metric": "Total Domains",
                    "Value": total_domains
                },
                {
                    "Metric": "Live Domains",
                    "Value": total_live
                },
                {
                    "Metric": "Dead Domains",
                    "Value": total_dead
                },
                {
                    "Metric": "Unstable Domains",
                    "Value": total_unstable
                },
                {
                    "Metric": "Infrastructure Health Score",
                    "Value": f"{health_score}%"
                },
                {
                    "Metric": "Snapshot Backup",
                    "Value": BACKUP_FILE.name
                }
            ])

            summary_df.to_excel(
                writer,
                sheet_name="Summary",
                index=False,
                startrow=0
            )

            totals_df.to_excel(
                writer,
                sheet_name="Summary",
                index=False,
                startrow=len(summary_df) + 3
            )

    # =====================================================
    # SAFE REPLACEMENT
    # =====================================================

    shutil.move(TEMP_FILE, EXCEL_FILE)

    print("\n" + "=" * 80)
    print("SPLECTOR INFRASTRUCTURE UPDATE COMPLETE")
    print("=" * 80)

    print(f"""
Main Workbook Updated:
{EXCEL_FILE.name}

Snapshot Backup Created:
{BACKUP_FILE.name}
""")

# =========================================================
# ENTRY
# =========================================================

if __name__ == "__main__":

    if hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):

        asyncio.set_event_loop_policy(
            asyncio.WindowsSelectorEventLoopPolicy()
        )

    asyncio.run(main())