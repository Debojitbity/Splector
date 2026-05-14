import asyncio
import aiohttp
import pandas as pd
import socket
import time
import shutil

from datetime import datetime

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
# CONFIGURATION
# =========================================================

EXCEL_FILE = "data/links.xlsx"

SHEETS = [
    "production",
    "temporary",
    "unstable"
]

CONCURRENCY_LIMIT = 50
TIMEOUT_SECONDS = 15

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    )
}

# =========================================================
# SNAPSHOT BACKUP
# =========================================================

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

BACKUP_FILE = f"data/links_snapshot_{timestamp}.xlsx"

print(f"\n[BACKUP] Creating snapshot: {BACKUP_FILE}")

shutil.copy2(EXCEL_FILE, BACKUP_FILE)

# =========================================================
# HEALTH CLASSIFIER
# =========================================================

def classify_health(row):

    reason = str(row["reason"])

    if row["reachable"] is True:
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
        "Too Many Redirects"
    ]:
        return "UNSTABLE"

    return "UNKNOWN"

# =========================================================
# DOMAIN CHECKER
# =========================================================

async def check_domain(session, domain, semaphore):

    async with semaphore:

        result = {
            "domain": domain,
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

                    elapsed = round(
                        (time.perf_counter() - start) * 1000,
                        2
                    )

                    # Read tiny chunk only
                    await response.content.read(256)

                    result.update({
                        "reachable": True,
                        "status_code": response.status,
                        "reason": "OK",
                        "response_time_ms": elapsed,
                        "final_url": str(response.url)
                    })

                    print(
                        f"[OK] "
                        f"{domain} "
                        f"{response.status} "
                        f"{elapsed}ms"
                    )

                    return result

            # =================================================
            # ERROR HANDLING
            # =================================================

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

        print(
            f"[FAIL] "
            f"{domain} "
            f"-> {result['reason']}"
        )

        return result

# =========================================================
# PROCESS SHEET
# =========================================================

async def process_sheet(sheet_name, old_df):

    print(f"\n{'='*70}")
    print(f"PROCESSING SHEET: {sheet_name.upper()}")
    print(f"{'='*70}")

    domains = (
        old_df["domain"]
        .dropna()
        .astype(str)
        .str.strip()
        .unique()
        .tolist()
    )

    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

    timeout = aiohttp.ClientTimeout(
        total=TIMEOUT_SECONDS
    )

    connector = aiohttp.TCPConnector(
        ssl=False,
        limit=CONCURRENCY_LIMIT,
        ttl_dns_cache=300
    )

    async with aiohttp.ClientSession(
        headers=HEADERS,
        timeout=timeout,
        connector=connector
    ) as session:

        tasks = [
            check_domain(session, domain, semaphore)
            for domain in domains
        ]

        results = []

        for completed_task in asyncio.as_completed(tasks):

            result = await completed_task
            results.append(result)

    new_df = pd.DataFrame(results)

    # =====================================================
    # COMPARISON ENGINE
    # =====================================================

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

    # =====================================================
    # HEALTH CLASSIFICATION
    # =====================================================

    new_df["health"] = new_df.apply(
        classify_health,
        axis=1
    )

    # =====================================================
    # SUMMARY
    # =====================================================

    live_count = len(
        new_df[new_df["health"] == "LIVE"]
    )

    dead_count = len(
        new_df[new_df["health"] == "DEAD"]
    )

    unstable_count = len(
        new_df[new_df["health"] == "UNSTABLE"]
    )

    avg_response = round(
        pd.to_numeric(
            new_df["response_time_ms"],
            errors="coerce"
        ).mean(),
        2
    )

    summary = {
        "sheet": sheet_name,
        "total": len(new_df),
        "live": live_count,
        "dead": dead_count,
        "unstable": unstable_count,
        "avg_ms": avg_response,
        "recovered": recovered,
        "degraded": degraded,
        "reason_changed": reason_changed
    }

    return new_df, summary

# =========================================================
# MAIN
# =========================================================

async def main():

    print("\n[LOADING EXCEL FILE]")

    excel = pd.ExcelFile(EXCEL_FILE)

    # =====================================================
    # OVERWRITE ORIGINAL FILE ONLY
    # =====================================================

    writer = pd.ExcelWriter(
        EXCEL_FILE,
        engine="openpyxl",
        mode="w"
    )

    global_summary = []

    for sheet in SHEETS:

        if sheet not in excel.sheet_names:

            print(f"[SKIP] Missing sheet: {sheet}")
            continue

        # =================================================
        # LOAD OLD DATA
        # =================================================

        old_df = pd.read_excel(
            EXCEL_FILE,
            sheet_name=sheet
        )

        # =================================================
        # PROCESS
        # =================================================

        new_df, summary = await process_sheet(
            sheet,
            old_df
        )

        global_summary.append(summary)

        # =================================================
        # REMOVE INTERNAL HEALTH COLUMN
        # =================================================

        export_df = new_df[[
            "domain",
            "reachable",
            "status_code",
            "reason",
            "response_time_ms",
            "final_url"
        ]]

        # =================================================
        # OVERWRITE SHEET
        # =================================================

        export_df.to_excel(
            writer,
            sheet_name=sheet,
            index=False
        )

    # =====================================================
    # SAVE UPDATED WORKBOOK
    # =====================================================

    writer.close()

    # =====================================================
    # BEAUTIFUL TERMINAL SUMMARY
    # =====================================================

    print("\n")
    print("=" * 80)
    print("SPLECTOR INFRASTRUCTURE SUMMARY")
    print("=" * 80)

    total_live = 0
    total_dead = 0
    total_unstable = 0
    total_domains = 0

    for s in global_summary:

        total_live += s["live"]
        total_dead += s["dead"]
        total_unstable += s["unstable"]
        total_domains += s["total"]

        print(f"""
[{s['sheet'].upper()}]

Total Domains   : {s['total']}
Live            : {s['live']}
Dead            : {s['dead']}
Unstable        : {s['unstable']}
Recovered       : {s['recovered']}
Degraded        : {s['degraded']}
Reason Changed  : {s['reason_changed']}
Avg Response    : {s['avg_ms']} ms
""")

    print("=" * 80)

    live_percentage = round(
        (total_live / total_domains) * 100,
        2
    )

    print(f"""
GLOBAL TOTALS

Total Domains      : {total_domains}
Live Domains       : {total_live}
Dead Domains       : {total_dead}
Unstable Domains   : {total_unstable}
Infrastructure Health Score : {live_percentage}%
""")

    print("=" * 80)

    print(f"""
FILES

Snapshot Backup:
{BACKUP_FILE}

Updated Main Workbook:
{EXCEL_FILE}
""")

    print("=" * 80)

# =========================================================
# ENTRY POINT
# =========================================================

if __name__ == "__main__":

    if hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):

        asyncio.set_event_loop_policy(
            asyncio.WindowsSelectorEventLoopPolicy()
        )

    asyncio.run(main())