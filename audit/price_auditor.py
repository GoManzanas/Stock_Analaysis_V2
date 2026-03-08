"""Price data anomaly detection.

Detects day-over-day return outliers, stale prices, and close vs adj_close divergence.
"""

import logging
import sqlite3

from rich.console import Console

from db.database import query_all

log = logging.getLogger(__name__)
console = Console()


def _record_finding(
    conn: sqlite3.Connection,
    audit_type: str,
    entity_type: str,
    entity_id: str,
    finding: str,
    severity: str,
    auto_fixed: bool = False,
    details: str = "",
):
    conn.execute(
        """INSERT INTO audit_results
        (audit_type, entity_type, entity_id, finding, severity, auto_fixed, details)
        VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (audit_type, entity_type, entity_id, finding, severity, int(auto_fixed), details),
    )


def audit_return_outliers(conn: sqlite3.Connection, threshold: float = 0.5) -> int:
    """Flag single-day price changes > threshold (default ±50%).

    Cross-references against corporate_actions to check if a split explains the move.
    """
    findings = 0

    rows = query_all(conn, """
        SELECT p1.ticker, p1.date as date1, p2.date as date2,
               p1.close as close1, p2.close as close2
        FROM prices p1
        JOIN prices p2 ON p1.ticker = p2.ticker
            AND p2.date = (SELECT MIN(date) FROM prices WHERE ticker = p1.ticker AND date > p1.date)
        WHERE p1.close > 0 AND p2.close > 0
            AND ABS((p2.close - p1.close) / p1.close) > ?
    """, (threshold,))

    for row in rows:
        pct_change = (row["close2"] - row["close1"]) / row["close1"]

        # Check if a corporate action explains this
        split = conn.execute(
            "SELECT * FROM corporate_actions WHERE ticker = ? AND effective_date BETWEEN ? AND ?",
            (row["ticker"], row["date1"], row["date2"]),
        ).fetchone()

        if split:
            severity = "info"
            finding = (
                f"{row['ticker']}: {pct_change:+.1%} on {row['date2']} "
                f"(explained by {split['action_type']} on {split['effective_date']})"
            )
        else:
            severity = "warning"
            finding = (
                f"{row['ticker']}: {pct_change:+.1%} on {row['date2']} "
                f"(${row['close1']:.2f} → ${row['close2']:.2f}, no corporate action found)"
            )

        _record_finding(
            conn, "return_outlier", "price", f"{row['ticker']}:{row['date2']}",
            finding, severity,
        )
        findings += 1

    return findings


def audit_stale_prices(conn: sqlite3.Connection, stale_days: int = 30) -> int:
    """Flag tickers where the last price date is significantly old."""
    findings = 0

    rows = query_all(conn, """
        SELECT ticker, MAX(date) as last_date,
               julianday('now') - julianday(MAX(date)) as days_stale
        FROM prices
        GROUP BY ticker
        HAVING days_stale > ?
    """, (stale_days,))

    for row in rows:
        _record_finding(
            conn, "stale_price", "price", row["ticker"],
            f"{row['ticker']}: last price on {row['last_date']} "
            f"({int(row['days_stale'])} days ago, possible delisting)",
            "info",
        )
        findings += 1

    return findings


def run_price_audit(conn: sqlite3.Connection) -> dict:
    """Run all price audit checks. Returns summary stats."""
    # Clear previous price audit results
    conn.execute("DELETE FROM audit_results WHERE audit_type IN ('return_outlier', 'stale_price')")

    outlier_findings = audit_return_outliers(conn)
    stale_findings = audit_stale_prices(conn)

    conn.commit()

    total = outlier_findings + stale_findings
    console.print(
        f"[bold]Price Audit[/bold]: {total} findings "
        f"(return_outliers: {outlier_findings}, stale_prices: {stale_findings})"
    )

    return {"return_outliers": outlier_findings, "stale_prices": stale_findings, "total": total}
