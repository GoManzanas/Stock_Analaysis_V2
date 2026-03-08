"""Corporate actions (splits, dividends, symbol changes) from EODHD."""

import json
import logging
import time

import requests
from rich.console import Console

from config.settings import EODHD_API_KEY, EODHD_BASE_URL, EODHD_CALLS_PER_SECOND
from db.database import query_all
from scrapers.base import BaseScraper

log = logging.getLogger(__name__)
console = Console()

_DELAY = 1.0 / EODHD_CALLS_PER_SECOND


def fetch_splits(ticker: str, exchange: str = "US") -> list[dict]:
    """Fetch split history for a ticker from EODHD."""
    try:
        resp = requests.get(
            f"{EODHD_BASE_URL}/splits/{ticker}.{exchange}",
            params={"api_token": EODHD_API_KEY, "fmt": "json", "from": "2010-01-01"},
            timeout=30,
        )
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list):
                return data
    except (requests.RequestException, ValueError) as e:
        log.warning("Splits fetch error for %s: %s", ticker, e)
    return []


def _parse_split_ratio(split_str: str) -> float | None:
    """Parse split ratio string like '4/1' into float (4.0)."""
    try:
        parts = split_str.strip().split("/")
        if len(parts) == 2:
            return float(parts[0]) / float(parts[1])
    except (ValueError, ZeroDivisionError):
        pass
    return None


class CorporateActionsScraper(BaseScraper):
    """Downloads splits and symbol changes from EODHD."""

    job_type = "corporate_actions"

    def run(self, batch_size: int = 100, **kwargs):
        """Download corporate actions for all resolved tickers."""
        tickers = query_all(
            self.conn,
            "SELECT DISTINCT ticker, exchange FROM securities WHERE ticker IS NOT NULL",
        )

        if not tickers:
            console.print("[yellow]No resolved tickers to fetch corporate actions for.[/yellow]")
            return

        total = len(tickers)
        console.print(f"[bold]Corporate Actions Download[/bold]: {total} tickers")

        job_id, progress = self.get_or_create_job("all")
        job = self.get_job("all")
        if job and job["status"] == "completed":
            console.print("[dim]Already completed.[/dim]")
            return

        completed_set = set()
        if progress and "completed" in progress:
            completed_set = set(progress["completed"])

        fetched = 0
        actions_count = 0

        for i, row in enumerate(tickers):
            if self.is_interrupted:
                self.interrupt_job(job_id, {
                    "completed": list(completed_set),
                    "fetched": fetched,
                    "actions_count": actions_count,
                })
                break

            ticker = row["ticker"]
            # EODHD API uses broad exchange code "US", not specific exchange (NYSE, NASDAQ, etc.)
            exchange = "US"

            if ticker in completed_set:
                continue

            # Fetch splits
            splits = fetch_splits(ticker, exchange)
            for split in splits:
                date = split.get("date", "")
                split_str = split.get("split", "")
                ratio = _parse_split_ratio(split_str)

                details = json.dumps({
                    "ratio_str": split_str,
                    "ratio": ratio,
                })
                action_type = "split" if ratio and ratio > 1 else "reverse_split"

                self.conn.execute(
                    """INSERT OR IGNORE INTO corporate_actions
                    (ticker, action_type, effective_date, details, source)
                    VALUES (?, ?, ?, ?, ?)""",
                    (ticker, action_type, date, details, "eodhd"),
                )
                actions_count += 1

            completed_set.add(ticker)
            fetched += 1

            if (i + 1) % batch_size == 0:
                self.conn.commit()
                self.update_progress(job_id, {
                    "completed": list(completed_set),
                    "fetched": fetched,
                    "actions_count": actions_count,
                })
                self.conn.commit()
                console.print(f"  [{fetched}/{total}] {actions_count} actions found")

            time.sleep(_DELAY)

        self.conn.commit()

        if not self.is_interrupted:
            self.complete_job(job_id)
            console.print(
                f"[bold green]Corporate actions complete.[/bold green] "
                f"Tickers: {fetched}, Actions: {actions_count}"
            )
