"""Fund metrics cache: pre-compute and store screening metrics for fast queries."""

import logging
import sqlite3
from typing import Callable

from analytics.screening import compute_fund_metrics
from db.database import query_all, query_one, upsert

logger = logging.getLogger(__name__)

# Columns in fund_metrics_cache (excluding computed_at, handled by DEFAULT)
_CACHE_COLUMNS = [
    "cik", "name", "annualized_return", "sharpe_ratio", "sp500_correlation",
    "max_drawdown", "hhi", "top5_concentration", "avg_turnover",
    "quarters_active", "latest_aum", "avg_confidence", "computed_at",
]

_METRIC_KEYS = [
    "annualized_return", "sharpe_ratio", "sp500_correlation", "max_drawdown",
    "hhi", "top5_concentration", "avg_turnover", "quarters_active",
    "latest_aum", "avg_confidence",
]


def get_stale_ciks(conn: sqlite3.Connection) -> list[str]:
    """Find CIKs that need cache refresh.

    Returns CIKs that either:
    - Have no cache entry
    - Have filings newer than their cache computed_at
    - Must have >= 2 filings (needed for return computation)
    """
    rows = query_all(
        conn,
        """
        SELECT f.cik
        FROM filers f
        WHERE (
            SELECT COUNT(*) FROM filings fi WHERE fi.cik = f.cik
        ) >= 2
        AND (
            f.cik NOT IN (SELECT cik FROM fund_metrics_cache)
            OR (SELECT MAX(fi.scraped_at) FROM filings fi WHERE fi.cik = f.cik)
               > (SELECT computed_at FROM fund_metrics_cache WHERE cik = f.cik)
        )
        """,
    )
    return [r["cik"] for r in rows]


def refresh_cache(
    conn: sqlite3.Connection,
    ciks: list[str] | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
) -> int:
    """Recompute metrics for given CIKs (or all stale CIKs) and upsert into cache.

    Args:
        conn: Database connection.
        ciks: Specific CIKs to refresh. If None, refreshes all stale CIKs.
        progress_callback: Called with (current, total) after each CIK.

    Returns:
        Number of CIKs refreshed.
    """
    if ciks is None:
        ciks = get_stale_ciks(conn)

    if not ciks:
        return 0

    total = len(ciks)
    batch_rows = []

    for i, cik in enumerate(ciks):
        # Get filer name
        filer = query_one(conn, "SELECT name FROM filers WHERE cik = ?", (cik,))
        name = filer["name"] if filer else None

        metrics = compute_fund_metrics(conn, cik)

        row = (
            cik,
            name,
            metrics.get("annualized_return"),
            metrics.get("sharpe_ratio"),
            metrics.get("sp500_correlation"),
            metrics.get("max_drawdown"),
            metrics.get("hhi"),
            metrics.get("top5_concentration"),
            metrics.get("avg_turnover"),
            metrics.get("quarters_active"),
            metrics.get("latest_aum"),
            metrics.get("avg_confidence"),
            # computed_at: use SQL datetime('now') via explicit value
            None,  # placeholder, will be set in SQL
        )
        batch_rows.append(row)

        # Commit every 50 CIKs
        if len(batch_rows) >= 50:
            _upsert_batch(conn, batch_rows)
            batch_rows = []

        if progress_callback:
            progress_callback(i + 1, total)

    # Flush remaining
    if batch_rows:
        _upsert_batch(conn, batch_rows)

    return total


def _upsert_batch(conn: sqlite3.Connection, rows: list[tuple]) -> None:
    """Upsert a batch of cache rows."""
    sql = """
        INSERT INTO fund_metrics_cache
            (cik, name, annualized_return, sharpe_ratio, sp500_correlation,
             max_drawdown, hhi, top5_concentration, avg_turnover,
             quarters_active, latest_aum, avg_confidence, computed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(cik) DO UPDATE SET
            name = excluded.name,
            annualized_return = excluded.annualized_return,
            sharpe_ratio = excluded.sharpe_ratio,
            sp500_correlation = excluded.sp500_correlation,
            max_drawdown = excluded.max_drawdown,
            hhi = excluded.hhi,
            top5_concentration = excluded.top5_concentration,
            avg_turnover = excluded.avg_turnover,
            quarters_active = excluded.quarters_active,
            latest_aum = excluded.latest_aum,
            avg_confidence = excluded.avg_confidence,
            computed_at = datetime('now')
    """
    # Strip the placeholder computed_at from each row (SQL uses datetime('now'))
    trimmed = [row[:-1] for row in rows]
    conn.executemany(sql, trimmed)
    conn.commit()


def is_cache_fresh(conn: sqlite3.Connection) -> bool:
    """Return True if no stale CIKs exist."""
    return len(get_stale_ciks(conn)) == 0


def get_cache_stats(conn: sqlite3.Connection) -> dict:
    """Return cache status summary."""
    cached = query_one(
        conn, "SELECT COUNT(*) AS cnt FROM fund_metrics_cache"
    )
    total_filers = query_one(
        conn, "SELECT COUNT(*) AS cnt FROM filers"
    )
    stale = len(get_stale_ciks(conn))
    last_refresh = query_one(
        conn, "SELECT MAX(computed_at) AS ts FROM fund_metrics_cache"
    )

    return {
        "total_cached": cached["cnt"] if cached else 0,
        "total_filers": total_filers["cnt"] if total_filers else 0,
        "stale_count": stale,
        "last_refresh": last_refresh["ts"] if last_refresh else None,
    }
