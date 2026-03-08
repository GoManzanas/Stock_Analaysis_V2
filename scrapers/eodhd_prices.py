"""EODHD historical price downloader."""

import logging
import time

import requests
from rich.console import Console

from config.settings import EODHD_API_KEY, EODHD_BASE_URL, EODHD_CALLS_PER_SECOND
from db.database import query_all, query_one
from scrapers.base import BaseScraper

log = logging.getLogger(__name__)
console = Console()

_DELAY = 1.0 / EODHD_CALLS_PER_SECOND

BENCHMARKS = [
    ("SPY", "US"),
    ("GSPC", "INDX"),
]


def fetch_eod_prices(ticker: str, exchange: str = "US", from_date: str = "2010-01-01") -> list[dict]:
    """Fetch EOD price history for a ticker from EODHD."""
    try:
        resp = requests.get(
            f"{EODHD_BASE_URL}/eod/{ticker}.{exchange}",
            params={
                "api_token": EODHD_API_KEY,
                "fmt": "json",
                "from": from_date,
            },
            timeout=60,
        )
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list):
                return data
    except Exception as e:
        log.warning("Price fetch error for %s.%s: %s", ticker, exchange, e)
    return []


class PriceScraper(BaseScraper):
    """Downloads historical EOD prices from EODHD."""

    job_type = "price_download"

    def run(self, batch_size: int = 100, **kwargs):
        """Download prices for all resolved tickers + benchmarks."""
        tickers = query_all(
            self.conn,
            "SELECT DISTINCT ticker, exchange FROM securities WHERE ticker IS NOT NULL",
        )

        if not tickers:
            console.print("[yellow]No resolved tickers to fetch prices for.[/yellow]")
            return

        total = len(tickers) + len(BENCHMARKS)
        console.print(f"[bold]Price Download[/bold]: {len(tickers)} tickers + {len(BENCHMARKS)} benchmarks")

        job_id, progress = self.get_or_create_job("all")
        job = self.get_job("all")
        if job and job["status"] == "completed":
            console.print("[dim]Already completed.[/dim]")
            return

        completed_set = set()
        if progress and "completed" in progress:
            completed_set = set(progress["completed"])

        fetched = 0
        rows_inserted = 0

        # Download ticker prices
        for i, row in enumerate(tickers):
            if self.is_interrupted:
                break

            ticker = row["ticker"]
            exchange = row["exchange"] or "US"
            ticker_key = f"{ticker}.{exchange}"

            if ticker_key in completed_set:
                continue

            # Check for existing data (incremental)
            existing = query_one(
                self.conn,
                "SELECT MAX(date) as max_date FROM prices WHERE ticker = ?",
                (ticker,),
            )
            from_date = "2010-01-01"
            if existing and existing["max_date"]:
                from_date = existing["max_date"]

            prices = fetch_eod_prices(ticker, exchange, from_date)
            if prices:
                price_rows = []
                for p in prices:
                    price_rows.append((
                        ticker,
                        p.get("date", ""),
                        p.get("open"),
                        p.get("high"),
                        p.get("low"),
                        p.get("close"),
                        p.get("adjusted_close"),
                        p.get("volume"),
                    ))

                self.conn.executemany(
                    """INSERT OR IGNORE INTO prices
                    (ticker, date, open, high, low, close, adj_close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    price_rows,
                )
                rows_inserted += len(price_rows)

            completed_set.add(ticker_key)
            fetched += 1

            if (i + 1) % batch_size == 0:
                self.conn.commit()
                self.update_progress(job_id, {
                    "completed": list(completed_set),
                    "fetched": fetched,
                    "rows_inserted": rows_inserted,
                })
                self.conn.commit()
                console.print(f"  [{fetched}/{total}] {rows_inserted} price rows")

            time.sleep(_DELAY)

        # Download benchmark prices
        if not self.is_interrupted:
            for ticker, exchange in BENCHMARKS:
                ticker_key = f"{ticker}.{exchange}"
                if ticker_key in completed_set:
                    continue

                console.print(f"  Downloading benchmark: {ticker_key}...")
                prices = fetch_eod_prices(ticker, exchange)
                if prices:
                    price_rows = []
                    for p in prices:
                        price_rows.append((
                            ticker,
                            p.get("date", ""),
                            p.get("open"),
                            p.get("high"),
                            p.get("low"),
                            p.get("close"),
                            p.get("adjusted_close"),
                            p.get("volume"),
                        ))

                    self.conn.executemany(
                        """INSERT OR IGNORE INTO benchmark_prices
                        (ticker, date, open, high, low, close, adj_close, volume)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                        price_rows,
                    )
                    rows_inserted += len(price_rows)

                completed_set.add(ticker_key)
                fetched += 1
                time.sleep(_DELAY)

        self.conn.commit()

        if self.is_interrupted:
            self.interrupt_job(job_id, {
                "completed": list(completed_set),
                "fetched": fetched,
                "rows_inserted": rows_inserted,
            })
        else:
            self.complete_job(job_id)
            console.print(
                f"[bold green]Price download complete.[/bold green] "
                f"Tickers: {fetched}, Rows: {rows_inserted}"
            )
