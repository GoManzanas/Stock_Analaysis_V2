"""Cross-validation of holdings vs prices.

Computes sum(shares * adj_close) per filing and compares against reported total_value.
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


def reconcile_filings(conn: sqlite3.Connection, threshold: float = 0.1) -> int:
    """Cross-validate holdings value against prices for each filing.

    For each filing, computes sum(shares * adj_close_on_report_date) and
    compares against reported total_value. Flags discrepancies > threshold (default 10%).

    Returns number of findings.
    """
    findings = 0

    # Get filings with reported total_value
    filings = query_all(conn, """
        SELECT f.id, f.accession_number, f.cik, f.report_date, f.total_value,
               fi.name as filer_name
        FROM filings f
        JOIN filers fi ON f.cik = fi.cik
        WHERE f.total_value > 0 AND f.report_date IS NOT NULL
    """)

    for filing in filings:
        # Compute portfolio value from holdings × prices
        result = conn.execute("""
            SELECT
                SUM(CASE WHEN p.adj_close IS NOT NULL THEN h.shares * p.adj_close ELSE 0 END) as computed_value,
                COUNT(*) as total_holdings,
                SUM(CASE WHEN p.adj_close IS NOT NULL THEN 1 ELSE 0 END) as priced_holdings,
                SUM(CASE WHEN h.put_call IS NOT NULL THEN 1 ELSE 0 END) as option_holdings
            FROM holdings h
            LEFT JOIN securities s ON h.cusip = s.cusip
            LEFT JOIN prices p ON s.ticker = p.ticker AND p.date = ?
            WHERE h.filing_id = ?
        """, (filing["report_date"], filing["id"])).fetchone()

        if result is None or result["total_holdings"] == 0:
            continue

        computed = result["computed_value"] or 0
        reported = filing["total_value"]
        priced = result["priced_holdings"]
        total = result["total_holdings"]
        options = result["option_holdings"]
        coverage = priced / total if total > 0 else 0

        if computed == 0 or reported == 0:
            continue

        discrepancy = abs(computed - reported) / reported

        if discrepancy > threshold:
            severity = "warning" if discrepancy > 0.5 else "info"
            _record_finding(
                conn, "reconciliation", "filing", filing["accession_number"],
                f"{filing['filer_name']}: computed ${computed:,.0f} vs reported ${reported:,.0f} "
                f"({discrepancy:.1%} discrepancy). "
                f"Coverage: {priced}/{total} holdings priced, {options} options.",
                severity,
                details=f"cik={filing['cik']}, report_date={filing['report_date']}, "
                        f"coverage={coverage:.2f}",
            )
            findings += 1

    return findings


def run_reconciliation(conn: sqlite3.Connection) -> dict:
    """Run reconciliation audit. Returns summary stats."""
    # Clear previous reconciliation results
    conn.execute("DELETE FROM audit_results WHERE audit_type = 'reconciliation'")

    findings = reconcile_filings(conn)
    conn.commit()

    console.print(f"[bold]Reconciliation[/bold]: {findings} filings with >10% discrepancy")

    return {"discrepancies": findings}
