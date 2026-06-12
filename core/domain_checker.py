import asyncio
import aiohttp
import pandas as pd
import socket
import time
import shutil
import random
import os
import re

from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse
from core.db import get_db_connection

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

def sanitize_excel(value):
    if not isinstance(value, str):
        return value
    value = value.strip()
    if value.startswith(("=", "+", "-", "@")):
        value = "'" + value
    return value

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

async def check_domain(session, domain, semaphore, timeout_seconds, pause_event, cancel_event):
    if not pause_event.is_set():
        await pause_event.wait()

    if cancel_event.is_set():
        return None

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
                    timeout=timeout_seconds,
                    allow_redirects=True,
                    ssl=False
                ) as response:

                    final_url = str(response.url)

                    if not is_allowed_domain(final_url):
                        result["reason"] = "External Redirect Blocked"
                        result["final_url"] = sanitize_excel(final_url)
                        return result

                    await response.content.read(256)
                    elapsed = round((time.perf_counter() - start) * 1000, 2)

                    result.update({
                        "reachable": True,
                        "status_code": response.status,
                        "reason": "OK",
                        "response_time_ms": elapsed,
                        "final_url": sanitize_excel(final_url)
                    })

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

        return result

async def process_sheet(sheet_name, old_df, session, config, emitter, pause_event, cancel_event):
    emitter.log("INFO", f"PROCESSING: {sheet_name.upper()}")

    domains = (
        old_df["domain"]
        .dropna()
        .astype(str)
        .str.strip()
        .unique()
        .tolist()
    )

    total_domains = len(domains)
    emitter.stage_start(1, total_domains)

    semaphore = asyncio.Semaphore(config.concurrency_limit)
    tasks = [
        check_domain(session, domain, semaphore, config.timeout_seconds, pause_event, cancel_event)
        for domain in domains
    ]

    results = []
    completed = 0

    for task in asyncio.as_completed(tasks):
        result = await task
        if result is None:
            break
        results.append(result)
        completed += 1
        
        # We only have one stage conceptually in this runner, or maybe we don't map perfectly.
        # Let's map process_sheet loop to stage 1 progress.
        emitter.stage_progress(1, completed, total_domains)
        
        if result["reason"] == "OK":
            emitter.log("INFO", f"[OK] {result['domain']} {result['status_code']} {result['response_time_ms']}ms")
        else:
            emitter.log("WARNING", f"[FAIL] {result['domain']} -> {result['reason']}")

    if not results:
        return pd.DataFrame(), {}

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

async def run_domain_checker(config, emitter, pause_event, cancel_event):
    emitter.pipeline_status("running")
    emitter.log("INFO", "══════════════════════════════════════════════════════")
    emitter.log("INFO", "SPLECTOR INFRASTRUCTURE HEALTH CHECK")
    emitter.log("INFO", "══════════════════════════════════════════════════════")

    db_file = config.abs_db_path
    sheets = ["production", "temporary", "unstable"]

    emitter.log("INFO", "[LOADING DOMAINS FROM SQLITE]")

    timeout = aiohttp.ClientTimeout(total=config.timeout_seconds)
    connector = aiohttp.TCPConnector(
        ssl=False,
        limit=config.concurrency_limit,
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

        try:
            total_domains = 0
            total_live = 0
            total_dead = 0
            total_unstable = 0

            for sheet in sheets:
                if cancel_event.is_set():
                    break

                conn = get_db_connection(db_file)
                old_df = pd.read_sql_query("SELECT * FROM domains WHERE source_sheet = ?", conn, params=[sheet])
                conn.close()

                if old_df.empty:
                    continue

                new_df, summary = await process_sheet(
                    sheet, old_df, session, config, emitter, pause_event, cancel_event
                )

                if new_df.empty:
                    continue

                global_summary.append(summary)

                total_domains += summary["Total Domains"]
                total_live += summary["Live"]
                total_dead += summary["Dead"]
                total_unstable += summary["Unstable"]

                # UPDATE DATABASE
                with get_db_connection(db_file) as conn_w:
                    conn_w.execute("BEGIN TRANSACTION")
                    for _, row in new_df.iterrows():
                        reachable_int = 1 if row["reachable"] else 0
                        
                        rt = row["response_time_ms"]
                        rt_val = float(rt) if pd.notna(rt) and rt != "" else None

                        conn_w.execute("""
                            UPDATE domains
                            SET reachable = ?, status_code = ?, reason = ?, response_time_ms = ?, final_url = ?
                            WHERE domain = ?
                        """, (
                            reachable_int,
                            str(row["status_code"]),
                            str(row["reason"]),
                            rt_val,
                            str(row["final_url"]),
                            str(row["domain"])
                        ))
                    conn_w.commit()

            if cancel_event.is_set():
                emitter.log("WARNING", "Health check cancelled by user.")
                emitter.pipeline_status("cancelled")
                return

            health_score = round((total_live / total_domains) * 100, 2) if total_domains else 0
            emitter.log("INFO", f"Infrastructure Health Score: {health_score}%")

        except Exception as e:
            emitter.log("ERROR", f"Failed during domain checking: {e}")
            emitter.pipeline_status("error")
            return

    emitter.log("INFO", "══════════════════════════════════════════════════════")
    emitter.log("INFO", "SPLECTOR INFRASTRUCTURE UPDATE COMPLETE")
    emitter.log("INFO", "══════════════════════════════════════════════════════")
    emitter.log("INFO", f"Database Updated: {db_file}")

    emitter.stage_complete(1)
    emitter.pipeline_status("completed")
