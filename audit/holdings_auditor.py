"""Holdings data validation.

Detects value-in-thousands misdetection, filing errors, and amendment issues.
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


def audit_value_scale(conn: sqlite3.Connection) -> int:
    """Detect holdings where value/shares implies wrong scale (thousands vs actual).

    Compares implied_price (value/shares) against adj_close on report date.
    Returns number of findings.
    """
    findings = 0

    rows = query_all(conn, """
        SELECT h.id, h.cusip, h.value, h.shares, h.issuer_name,
               f.report_date, f.accession_number,
               s.ticker,
               p.adj_close
        FROM holdings h
        JOIN filings f ON h.filing_id = f.id
        LEFT JOIN securities s ON h.cusip = s.cusip
        LEFT JOIN prices p ON s.ticker = p.ticker AND f.report_date = p.date
        WHERE h.shares > 0 AND h.value > 0 AND p.adj_close IS NOT NULL AND p.adj_close > 0
    """)

    for row in rows:
        implied_price = row["value"] / row["shares"]
        actual_price = row["adj_close"]
        ratio = implied_price / actual_price

        if ratio > 500:
            # Value likely overstated by ~1000x
            _record_finding(
                conn, "value_scale", "holding", str(row["id"]),
                f"Implied price ${implied_price:.2f} is ~{ratio:.0f}x actual ${actual_price:.2f}. "
                f"Value may be overstated by 1000x.",
                "warning",
                details=f"cusip={row['cusip']}, ticker={row['ticker']}, "
                        f"value={row['value']}, shares={row['shares']}",
            )
            findings += 1
        elif ratio < 0.002:
            # Value likely understated by ~1000x
            _record_finding(
                conn, "value_scale", "holding", str(row["id"]),
                f"Implied price ${implied_price:.4f} is ~{1/ratio:.0f}x below actual ${actual_price:.2f}. "
                f"Value may need 1000x multiplier.",
                "warning",
                details=f"cusip={row['cusip']}, ticker={row['ticker']}, "
                        f"value={row['value']}, shares={row['shares']}",
            )
            findings += 1

    return findings


def audit_filing_errors(conn: sqlite3.Connection) -> int:
    """Detect obvious data errors in holdings.

    - Zero shares with non-zero value
    - Negative values or shares
    - Duplicate CUSIPs within same filing (legitimate if different PUT/CALL)
    """
    findings = 0

    # Zero shares with non-zero value
    rows = query_all(conn, """
        SELECT h.id, h.cusip, h.issuer_name, h.value, h.shares, f.accession_number
        FROM holdings h
        JOIN filings f ON h.filing_id = f.id
        WHERE h.shares = 0 AND h.value > 0
    """)
    for row in rows:
        _record_finding(
            conn, "filing_error", "holding", str(row["id"]),
            f"Zero shares with non-zero value (${row['value']:.0f})",
            "info",
            details=f"cusip={row['cusip']}, accession={row['accession_number']}",
        )
        findings += 1

    # Negative values
    rows = query_all(conn, """
        SELECT h.id, h.cusip, h.value, h.shares, f.accession_number
        FROM holdings h
        JOIN filings f ON h.filing_id = f.id
        WHERE h.value < 0 OR h.shares < 0
    """)
    for row in rows:
        _record_finding(
            conn, "filing_error", "holding", str(row["id"]),
            f"Negative value (${row['value']:.0f}) or shares ({row['shares']:.0f})",
            "warning",
            details=f"cusip={row['cusip']}, accession={row['accession_number']}",
        )
        findings += 1

    # Duplicate CUSIPs within filing (excluding different PUT/CALL)
    rows = query_all(conn, """
        SELECT h.filing_id, h.cusip, COUNT(*) as cnt, f.accession_number
        FROM holdings h
        JOIN filings f ON h.filing_id = f.id
        WHERE h.put_call IS NULL
        GROUP BY h.filing_id, h.cusip
        HAVING cnt > 1
    """)
    for row in rows:
        _record_finding(
            conn, "filing_error", "filing", row["accession_number"],
            f"Duplicate CUSIP {row['cusip']} appears {row['cnt']} times (no PUT/CALL distinction)",
            "info",
            details=f"filing_id={row['filing_id']}",
        )
        findings += 1

    return findings


def run_holdings_audit(conn: sqlite3.Connection) -> dict:
    """Run all holdings audit checks. Returns summary stats."""
    # Clear previous holdings audit results
    conn.execute("DELETE FROM audit_results WHERE audit_type IN ('value_scale', 'filing_error')")

    value_findings = audit_value_scale(conn)
    error_findings = audit_filing_errors(conn)

    conn.commit()

    total = value_findings + error_findings
    console.print(
        f"[bold]Holdings Audit[/bold]: {total} findings "
        f"(value_scale: {value_findings}, filing_errors: {error_findings})"
    )

    return {"value_scale": value_findings, "filing_errors": error_findings, "total": total}
