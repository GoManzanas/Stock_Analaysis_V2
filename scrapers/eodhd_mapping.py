"""CUSIP to ticker resolver using EODHD ID Mapping API.

Primary: EODHD /api/id-mapping?filter[cusip]={CUSIP}
Fallback: EODHD /api/search/{ISSUER_NAME}
"""

import logging
import time
from datetime import datetime, timezone

import requests
from rich.console import Console

from config.settings import EODHD_API_KEY, EODHD_BASE_URL, EODHD_CALLS_PER_SECOND
from db.database import query_all
from scrapers.base import BaseScraper

log = logging.getLogger(__name__)
console = Console()

_DELAY = 1.0 / EODHD_CALLS_PER_SECOND


def resolve_cusip_via_mapping(cusip: str) -> dict | None:
    """Try EODHD ID Mapping API with both 9-digit and 6-digit CUSIP.

    Returns dict with symbol info or None.
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
                    # Return first US-exchange match, or first result
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
    """Resolves CUSIPs to tickers using EODHD APIs."""

    job_type = "cusip_resolve"

    def run(self, batch_size: int = 100, **kwargs):
        """Resolve all unresolved CUSIPs."""
        unresolved = query_all(
            self.conn,
            "SELECT cusip, name FROM securities WHERE ticker IS NULL ORDER BY cusip",
        )

        if not unresolved:
            console.print("[green]All CUSIPs already resolved.[/green]")
            return

        total = len(unresolved)
        console.print(f"[bold]CUSIP Resolution[/bold]: {total} unresolved CUSIPs")

        job_id, progress = self.get_or_create_job("all")
        job = self.get_job("all")
        if job and job["status"] == "completed":
            console.print("[dim]Already completed.[/dim]")
            return

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

            # Try ID mapping first
            result = resolve_cusip_via_mapping(cusip)
            source = "eodhd_mapping"
            confidence = 1.0

            if result is None:
                # Fallback to name search
                time.sleep(_DELAY)
                result = resolve_cusip_via_search(issuer_name)
                source = "name_search"
                confidence = 0.7

            if result is not None:
                info = _extract_ticker_info(result)
                self.conn.execute(
                    """UPDATE securities SET
                        ticker = ?, eodhd_symbol = ?, name = COALESCE(?, name),
                        security_type = ?, exchange = ?, is_active = 1,
                        resolved_at = ?, resolution_source = ?,
                        resolution_confidence = ?
                    WHERE cusip = ?""",
                    (
                        info["ticker"], info["eodhd_symbol"], info["name"],
                        info["security_type"], info["exchange"],
                        now, source, confidence, cusip,
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
                f"[bold green]CUSIP resolution complete.[/bold green] "
                f"Resolved: {resolved_count}, Failed: {failed_count}, Total: {total}"
            )
