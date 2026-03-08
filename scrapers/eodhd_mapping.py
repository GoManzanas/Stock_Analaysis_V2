"""CUSIP to ticker resolver using EODHD Exchange Symbol List + name search fallback.

Phase 1: Download exchange symbol list (active + delisted) — 2 API calls
Phase 2: Bulk CUSIP matching via SQL JOIN on ISIN-derived CUSIPs
Phase 3: Name-search fallback for remaining unmatched CUSIPs
"""

import logging
import time
from datetime import datetime, timezone

import requests
from rich.console import Console

from config.settings import (
    EODHD_API_KEY,
    EODHD_BASE_URL,
    EODHD_CALLS_PER_SECOND,
    EODHD_SYMBOL_EXCHANGES,
)
from db.database import insert_or_ignore, query_all, query_one
from scrapers.base import BaseScraper

log = logging.getLogger(__name__)
console = Console()

_DELAY = 1.0 / EODHD_CALLS_PER_SECOND


def extract_cusip_from_isin(isin: str) -> str | None:
    """Extract 9-digit CUSIP from a US ISIN.

    ISIN format: 2-char country code + 9-digit CUSIP + 1 check digit = 12 chars.
    Example: US0378331005 -> 037833100
    """
    if not isin or len(isin) != 12:
        return None
    if not isin[:2].isalpha():
        return None
    # Extract 9-digit CUSIP (positions 2-10)
    cusip9 = isin[2:11]
    # Basic validation: should be alphanumeric
    if not cusip9.isalnum():
        return None
    return cusip9


def download_exchange_symbols(exchange: str, delisted: bool = False) -> list[dict]:
    """Download all tickers from an exchange via EODHD Exchange Symbol List API.

    Args:
        exchange: Exchange code (e.g., "US")
        delisted: If True, fetch delisted tickers instead of active ones

    Returns:
        List of symbol dicts with fields: Code, Name, Country, Exchange, Currency, Isin, Type
    """
    params = {
        "api_token": EODHD_API_KEY,
        "fmt": "json",
    }
    if delisted:
        params["delisted"] = "1"

    try:
        resp = requests.get(
            f"{EODHD_BASE_URL}/exchange-symbol-list/{exchange}",
            params=params,
            timeout=120,
        )
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list):
                return data
            log.warning("Unexpected response format for exchange %s: %s", exchange, type(data))
        else:
            log.error("Exchange symbol list API returned %d for %s", resp.status_code, exchange)
    except (requests.RequestException, ValueError) as e:
        log.error("Failed to download exchange symbols for %s: %s", exchange, e)

    return []


def resolve_cusip_via_mapping(cusip: str) -> dict | None:
    """Try EODHD ID Mapping API with both 9-digit and 6-digit CUSIP.

    Returns dict with symbol info or None.

    Note: This is the old per-CUSIP approach, kept for backward compatibility.
    The bulk symbol list approach (Phase 1+2) is preferred.
    """
    for cusip_variant in [cusip, cusip[:6]]:
        try:
            resp = requests.get(
                f"{EODHD_BASE_URL}/id-mapping",
                params={
                    "filter[cusip]": cusip_variant,
                    "api_token": EODHD_API_KEY,
                    "fmt": "json",
                },
                timeout=30,
            )
            if resp.status_code == 200:
                data = resp.json()
                if data and isinstance(data, list):
                    us_match = next((d for d in data if d.get("Exchange", "").upper() == "US"), None)
                    result = us_match or data[0]
                    return result
                elif data and isinstance(data, dict) and data.get("Code"):
                    return data
        except (requests.RequestException, ValueError) as e:
            log.warning("ID mapping error for CUSIP %s: %s", cusip_variant, e)

    return None


def resolve_cusip_via_search(issuer_name: str) -> dict | None:
    """Fallback: search EODHD by issuer name.

    Returns dict with symbol info or None.
    """
    if not issuer_name or len(issuer_name.strip()) < 2:
        return None

    # Clean up issuer name for search
    query = issuer_name.strip().split("/")[0].strip()  # Remove class suffixes
    query = query.replace(".", " ").replace(",", " ")

    try:
        resp = requests.get(
            f"{EODHD_BASE_URL}/search/{query}",
            params={
                "api_token": EODHD_API_KEY,
                "fmt": "json",
            },
            timeout=30,
        )
        if resp.status_code == 200:
            data = resp.json()
            if data and isinstance(data, list) and len(data) > 0:
                # Prefer US exchange matches
                us_matches = [d for d in data if d.get("Exchange", "").upper() == "US"]
                return us_matches[0] if us_matches else data[0]
    except (requests.RequestException, ValueError) as e:
        log.warning("Search error for '%s': %s", issuer_name, e)

    return None


def _extract_ticker_info(result: dict) -> dict:
    """Extract ticker, symbol, exchange from EODHD API result."""
    code = result.get("Code") or result.get("code") or result.get("Symbol") or ""
    exchange = result.get("Exchange") or result.get("exchange") or "US"
    name = result.get("Name") or result.get("name") or ""
    sec_type = result.get("Type") or result.get("type") or ""

    eodhd_symbol = f"{code}.{exchange}" if code else ""

    return {
        "ticker": code,
        "eodhd_symbol": eodhd_symbol,
        "name": name,
        "exchange": exchange,
        "security_type": sec_type,
    }


class CusipResolver(BaseScraper):
    """Resolves CUSIPs to tickers using EODHD Exchange Symbol List + name search fallback."""

    job_type = "cusip_resolve"

    def run(self, batch_size: int = 100, **kwargs):
        """Resolve all unresolved CUSIPs in 3 phases."""
        self._phase1_download_symbols()
        if self.is_interrupted:
            return

        self._phase2_bulk_match()
        if self.is_interrupted:
            return

        self._phase3_name_search_fallback(batch_size)

    def _phase1_download_symbols(self):
        """Phase 1: Download exchange symbol list (active + delisted)."""
        for exchange in EODHD_SYMBOL_EXCHANGES:
            target = f"symbol_download:{exchange}"
            job_id, progress = self.get_or_create_job(target)
            job = self.get_job(target)
            if job and job["status"] == "completed":
                count = query_one(
                    self.conn,
                    "SELECT COUNT(*) as cnt FROM exchange_symbols WHERE exchange = ?",
                    (exchange,),
                )
                console.print(
                    f"[dim]Phase 1: Symbol list for {exchange} already downloaded "
                    f"({count['cnt']} symbols).[/dim]"
                )
                continue

            if not EODHD_API_KEY:
                console.print(
                    "[bold red]Error: EODHD_API_KEY not set. "
                    "Create a .env file with EODHD_API_KEY=your_key[/bold red]"
                )
                return

            console.print(f"[bold]Phase 1: Downloading exchange symbols for {exchange}...[/bold]")

            total_inserted = 0

            # Download active tickers
            console.print(f"  Fetching active tickers for {exchange}...")
            active_symbols = download_exchange_symbols(exchange, delisted=False)
            if active_symbols:
                rows = self._prepare_symbol_rows(active_symbols, is_delisted=False)
                inserted = insert_or_ignore(
                    self.conn, "exchange_symbols",
                    ["code", "name", "country", "exchange", "currency", "isin", "cusip9", "type", "is_delisted"],
                    rows,
                )
                total_inserted += inserted
                console.print(f"  Active tickers: {len(active_symbols)} fetched, {inserted} new")

            if self.is_interrupted:
                self.interrupt_job(job_id, {"active_count": len(active_symbols)})
                return

            # Download delisted tickers
            console.print(f"  Fetching delisted tickers for {exchange}...")
            time.sleep(_DELAY)
            delisted_symbols = download_exchange_symbols(exchange, delisted=True)
            if delisted_symbols:
                rows = self._prepare_symbol_rows(delisted_symbols, is_delisted=True)
                inserted = insert_or_ignore(
                    self.conn, "exchange_symbols",
                    ["code", "name", "country", "exchange", "currency", "isin", "cusip9", "type", "is_delisted"],
                    rows,
                )
                total_inserted += inserted
                console.print(f"  Delisted tickers: {len(delisted_symbols)} fetched, {inserted} new")

            self.conn.commit()

            if self.is_interrupted:
                self.interrupt_job(job_id, {
                    "active_count": len(active_symbols),
                    "delisted_count": len(delisted_symbols),
                })
                return

            # Safeguard: don't mark as completed if nothing was downloaded
            if total_inserted == 0 and not active_symbols and not delisted_symbols:
                console.print(
                    f"[bold red]Phase 1 failed for {exchange}: API returned 0 symbols. "
                    f"Check your EODHD_API_KEY and network connection.[/bold red]"
                )
                self.fail_job(job_id, "API returned 0 symbols")
                return

            self.complete_job(job_id)
            console.print(
                f"[green]Phase 1 complete for {exchange}: "
                f"{total_inserted} symbols stored "
                f"({len(active_symbols)} active + {len(delisted_symbols)} delisted)[/green]"
            )

    def _prepare_symbol_rows(self, symbols: list[dict], is_delisted: bool) -> list[tuple]:
        """Convert API response dicts to insert tuples, extracting CUSIP from ISIN."""
        rows = []
        for s in symbols:
            isin = s.get("Isin") or s.get("ISIN") or ""
            cusip9 = extract_cusip_from_isin(isin) if isin else None
            rows.append((
                s.get("Code", ""),
                s.get("Name", ""),
                s.get("Country", ""),
                s.get("Exchange", ""),
                s.get("Currency", ""),
                isin,
                cusip9,
                s.get("Type", ""),
                1 if is_delisted else 0,
            ))
        return rows

    def _phase2_bulk_match(self):
        """Phase 2: Bulk CUSIP matching via SQL JOIN."""
        target = "bulk_match"
        job_id, progress = self.get_or_create_job(target)
        job = self.get_job(target)
        if job and job["status"] == "completed":
            console.print("[dim]Phase 2: Bulk matching already completed.[/dim]")
            return

        # Check that exchange_symbols has data
        sym_count = query_one(self.conn, "SELECT COUNT(*) as cnt FROM exchange_symbols")
        if not sym_count or sym_count["cnt"] == 0:
            console.print(
                "[bold red]Phase 2: exchange_symbols table is empty. "
                "Run Phase 1 first (check API key).[/bold red]"
            )
            return

        # Count unresolved before matching
        unresolved_before = query_one(
            self.conn,
            "SELECT COUNT(*) as cnt FROM securities WHERE ticker IS NULL",
        )
        unresolved_count = unresolved_before["cnt"] if unresolved_before else 0

        if unresolved_count == 0:
            console.print("[green]Phase 2: All CUSIPs already resolved.[/green]")
            self.complete_job(job_id)
            return

        console.print(
            f"[bold]Phase 2: Bulk matching {unresolved_count} unresolved CUSIPs "
            f"against exchange symbol list...[/bold]"
        )

        now = datetime.now(timezone.utc).isoformat()

        # Use CTE with ROW_NUMBER to pick best match per CUSIP when multiple symbols share it.
        # Prefer: active over delisted, Common Stock over other types, alphabetically by code.
        self.conn.execute(
            """
            WITH best_match AS (
                SELECT cusip9, code, name, exchange, type, is_delisted,
                       ROW_NUMBER() OVER (
                           PARTITION BY cusip9
                           ORDER BY
                               is_delisted ASC,
                               CASE WHEN type = 'Common Stock' THEN 0 ELSE 1 END,
                               code
                       ) AS rn
                FROM exchange_symbols
                WHERE cusip9 IS NOT NULL
            )
            UPDATE securities
            SET
                ticker = bm.code,
                eodhd_symbol = bm.code || '.' || bm.exchange,
                name = COALESCE(bm.name, securities.name),
                security_type = bm.type,
                exchange = bm.exchange,
                is_active = CASE WHEN bm.is_delisted = 0 THEN 1 ELSE 0 END,
                resolved_at = ?,
                resolution_source = 'bulk_symbol_list',
                resolution_confidence = 0.95
            FROM best_match bm
            WHERE securities.cusip = bm.cusip9
              AND bm.rn = 1
              AND securities.ticker IS NULL
            """,
            (now,),
        )
        self.conn.commit()

        # Count how many were matched
        unresolved_after = query_one(
            self.conn,
            "SELECT COUNT(*) as cnt FROM securities WHERE ticker IS NULL",
        )
        remaining = unresolved_after["cnt"] if unresolved_after else 0
        matched = unresolved_count - remaining

        self.complete_job(job_id)
        console.print(
            f"[green]Phase 2 complete: {matched} CUSIPs matched, "
            f"{remaining} still unresolved[/green]"
        )

    def _phase3_name_search_fallback(self, batch_size: int = 100):
        """Phase 3: Name-search fallback for remaining unmatched CUSIPs."""
        target = "name_search_fallback"
        job_id, progress = self.get_or_create_job(target)
        job = self.get_job(target)
        if job and job["status"] == "completed":
            console.print("[dim]Phase 3: Name search fallback already completed.[/dim]")
            return

        # Only search CUSIPs that haven't been resolved or attempted
        unresolved = query_all(
            self.conn,
            """SELECT cusip, name FROM securities
               WHERE ticker IS NULL AND resolution_source IS NULL
               ORDER BY cusip""",
        )

        if not unresolved:
            console.print("[green]Phase 3: No remaining CUSIPs to search.[/green]")
            self.complete_job(job_id)
            return

        total = len(unresolved)
        console.print(
            f"[bold]Phase 3: Name-search fallback for {total} unmatched CUSIPs...[/bold]"
        )

        # Restore progress from previous run
        resolved_set = set()
        if progress and "resolved" in progress:
            resolved_set = set(progress["resolved"])

        resolved_count = 0
        failed_count = 0
        now = datetime.now(timezone.utc).isoformat()

        for i, row in enumerate(unresolved):
            if self.is_interrupted:
                self.interrupt_job(job_id, {
                    "resolved": list(resolved_set),
                    "resolved_count": resolved_count,
                    "failed_count": failed_count,
                })
                break

            cusip = row["cusip"]
            issuer_name = row["name"] or ""

            if cusip in resolved_set:
                continue

            result = resolve_cusip_via_search(issuer_name)

            if result is not None:
                info = _extract_ticker_info(result)
                self.conn.execute(
                    """UPDATE securities SET
                        ticker = ?, eodhd_symbol = ?, name = COALESCE(?, name),
                        security_type = ?, exchange = ?, is_active = 1,
                        resolved_at = ?, resolution_source = 'name_search',
                        resolution_confidence = 0.7
                    WHERE cusip = ?""",
                    (
                        info["ticker"], info["eodhd_symbol"], info["name"],
                        info["security_type"], info["exchange"],
                        now, cusip,
                    ),
                )
                resolved_count += 1
            else:
                self.conn.execute(
                    """UPDATE securities SET
                        resolution_source = 'unresolved',
                        resolution_confidence = 0.0,
                        resolved_at = ?
                    WHERE cusip = ?""",
                    (now, cusip),
                )
                failed_count += 1

            resolved_set.add(cusip)

            # Commit and report progress every batch_size
            if (i + 1) % batch_size == 0:
                self.conn.commit()
                self.update_progress(job_id, {
                    "resolved": list(resolved_set),
                    "resolved_count": resolved_count,
                    "failed_count": failed_count,
                })
                self.conn.commit()
                console.print(
                    f"  [{resolved_count + failed_count}/{total}] "
                    f"resolved: {resolved_count}, failed: {failed_count}"
                )

            time.sleep(_DELAY)

        # Final commit
        self.conn.commit()

        if not self.is_interrupted:
            self.complete_job(job_id)
            console.print(
                f"[green]Phase 3 complete: "
                f"Resolved: {resolved_count}, Unresolved: {failed_count}, Total: {total}[/green]"
            )
