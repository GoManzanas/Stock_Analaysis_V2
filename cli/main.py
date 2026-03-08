"""Click CLI entry point for 13F Fund Analyst."""

import click
from rich.console import Console
from rich.table import Table

from config.settings import DB_PATH
from db.database import get_connection, get_table_count, init_db, query_all

console = Console()


@click.group()
def cli():
    """13F Fund Analyst — SEC filing data pipeline."""
    pass


# --- Scrape commands ---

@cli.group()
def scrape():
    """Download and parse SEC filings."""
    pass


@scrape.command("bulk")
@click.option("--from-year", default=2014, help="Start year (default: 2014)")
@click.option("--to-year", default=2025, help="End year (default: 2025)")
def scrape_bulk(from_year: int, to_year: int):
    """Download and parse SEC bulk 13F data sets."""
    from scrapers.sec_bulk import SecBulkScraper

    with SecBulkScraper() as scraper:
        scraper.run(from_year=from_year, to_year=to_year)


# --- Resolve commands ---

@cli.command("resolve")
def resolve_cusips():
    """Map CUSIPs to tickers via EODHD ID Mapping API."""
    from scrapers.eodhd_mapping import CusipResolver

    with CusipResolver() as resolver:
        resolver.run()


# --- Download commands ---

@cli.group()
def download():
    """Download price and corporate action data from EODHD."""
    pass


@download.command("prices")
def download_prices():
    """Pull EOD prices from EODHD for all resolved tickers."""
    from scrapers.eodhd_prices import PriceScraper

    with PriceScraper() as scraper:
        scraper.run()


@download.command("corporate")
def download_corporate():
    """Pull splits, dividends, and symbol changes from EODHD."""
    from scrapers.eodhd_corporate import CorporateActionsScraper

    with CorporateActionsScraper() as scraper:
        scraper.run()


# --- Audit commands ---

@cli.group()
def audit():
    """Run data quality audits."""
    pass


@audit.command("holdings")
def audit_holdings():
    """Validate 13F holdings data (value scale, filing errors)."""
    from audit.holdings_auditor import run_holdings_audit

    init_db()
    conn = get_connection()
    try:
        run_holdings_audit(conn)
    finally:
        conn.close()


@audit.command("prices")
def audit_prices():
    """Detect price data anomalies (outliers, stale prices)."""
    from audit.price_auditor import run_price_audit

    init_db()
    conn = get_connection()
    try:
        run_price_audit(conn)
    finally:
        conn.close()


@audit.command("reconcile")
def audit_reconcile():
    """Cross-validate holdings values against prices."""
    from audit.reconciler import run_reconciliation

    init_db()
    conn = get_connection()
    try:
        run_reconciliation(conn)
    finally:
        conn.close()


# --- Pipeline command ---

@cli.command()
@click.option("--from-year", default=2014, help="Start year (default: 2014)")
@click.option("--to-year", default=2025, help="End year (default: 2025)")
def pipeline(from_year: int, to_year: int):
    """Run the full data pipeline in order."""
    from scrapers.sec_bulk import SecBulkScraper
    from scrapers.eodhd_mapping import CusipResolver
    from scrapers.eodhd_corporate import CorporateActionsScraper
    from scrapers.eodhd_prices import PriceScraper
    from audit.holdings_auditor import run_holdings_audit
    from audit.price_auditor import run_price_audit
    from audit.reconciler import run_reconciliation

    console.print("[bold]Step 1/7: SEC Bulk Download[/bold]")
    with SecBulkScraper() as scraper:
        scraper.run(from_year=from_year, to_year=to_year)

    console.print("\n[bold]Step 2/7: CUSIP Resolution[/bold]")
    with CusipResolver() as resolver:
        resolver.run()

    console.print("\n[bold]Step 3/7: Corporate Actions[/bold]")
    with CorporateActionsScraper() as scraper:
        scraper.run()

    console.print("\n[bold]Step 4/7: Price Download[/bold]")
    with PriceScraper() as scraper:
        scraper.run()

    console.print("\n[bold]Step 5/7: Holdings Audit[/bold]")
    init_db()
    conn = get_connection()
    try:
        run_holdings_audit(conn)

        console.print("\n[bold]Step 6/7: Price Audit[/bold]")
        run_price_audit(conn)

        console.print("\n[bold]Step 7/7: Reconciliation[/bold]")
        run_reconciliation(conn)
    finally:
        conn.close()

    console.print("\n[bold green]Pipeline complete![/bold green]")


# --- Status command ---

@cli.command()
@click.option("--detail", type=click.Choice(["filings", "cusips", "prices", "jobs"]), help="Show detailed breakdown")
def status(detail: str | None):
    """Show pipeline status dashboard."""
    init_db()
    conn = get_connection()

    try:
        table = Table(title="Pipeline Status")
        table.add_column("Metric", style="cyan")
        table.add_column("Count", justify="right", style="green")

        table.add_row("Filers", str(get_table_count(conn, "filers")))
        table.add_row("Filings", str(get_table_count(conn, "filings")))
        table.add_row("Holdings", str(get_table_count(conn, "holdings")))
        table.add_row("Securities (total)", str(get_table_count(conn, "securities")))

        resolved = conn.execute(
            "SELECT COUNT(*) FROM securities WHERE ticker IS NOT NULL"
        ).fetchone()[0]
        unresolved = conn.execute(
            "SELECT COUNT(*) FROM securities WHERE ticker IS NULL"
        ).fetchone()[0]
        table.add_row("Securities (resolved)", str(resolved))
        table.add_row("Securities (unresolved)", str(unresolved))
        table.add_row("Prices (rows)", str(get_table_count(conn, "prices")))
        table.add_row("Corporate Actions", str(get_table_count(conn, "corporate_actions")))
        table.add_row("Audit Results", str(get_table_count(conn, "audit_results")))

        console.print(table)

        if detail == "filings":
            _show_filing_detail(conn)
        elif detail == "cusips":
            _show_cusip_detail(conn)
        elif detail == "prices":
            _show_price_detail(conn)
        elif detail == "jobs":
            _show_job_detail(conn)
    finally:
        conn.close()


def _show_filing_detail(conn):
    rows = query_all(
        conn,
        """SELECT report_year, report_quarter, COUNT(*) as cnt,
                  SUM(holding_count) as holdings
           FROM filings
           WHERE report_year IS NOT NULL
           GROUP BY report_year, report_quarter
           ORDER BY report_year, report_quarter""",
    )
    table = Table(title="Filings by Quarter")
    table.add_column("Quarter", style="cyan")
    table.add_column("Filings", justify="right")
    table.add_column("Holdings", justify="right")
    for row in rows:
        table.add_row(
            f"{row['report_year']}Q{row['report_quarter']}",
            str(row["cnt"]),
            str(row["holdings"] or 0),
        )
    console.print(table)


def _show_cusip_detail(conn):
    rows = query_all(
        conn,
        """SELECT resolution_source, COUNT(*) as cnt
           FROM securities
           GROUP BY resolution_source
           ORDER BY cnt DESC""",
    )
    table = Table(title="CUSIP Resolution Sources")
    table.add_column("Source", style="cyan")
    table.add_column("Count", justify="right")
    for row in rows:
        table.add_row(row["resolution_source"] or "unresolved", str(row["cnt"]))
    console.print(table)


def _show_price_detail(conn):
    row = conn.execute(
        "SELECT MIN(date) as min_date, MAX(date) as max_date, COUNT(DISTINCT ticker) as tickers FROM prices"
    ).fetchone()
    if row and row["min_date"]:
        console.print(f"  Date range: {row['min_date']} to {row['max_date']}")
        console.print(f"  Tickers with prices: {row['tickers']}")
    else:
        console.print("  [dim]No price data yet.[/dim]")


def _show_job_detail(conn):
    rows = query_all(
        conn,
        "SELECT job_type, target, status, started_at, completed_at FROM scrape_jobs ORDER BY id DESC LIMIT 20",
    )
    table = Table(title="Recent Jobs")
    table.add_column("Type", style="cyan")
    table.add_column("Target")
    table.add_column("Status")
    table.add_column("Started")
    table.add_column("Completed")
    for row in rows:
        status_style = {"completed": "green", "failed": "red", "running": "yellow", "interrupted": "yellow"}.get(row["status"], "")
        table.add_row(
            row["job_type"],
            row["target"],
            f"[{status_style}]{row['status']}[/{status_style}]" if status_style else row["status"],
            row["started_at"] or "",
            row["completed_at"] or "",
        )
    console.print(table)


# --- Reset command ---

@cli.command()
@click.option("--confirm", is_flag=True, help="Confirm database reset")
def reset(confirm: bool):
    """Wipe database and rebuild from scratch."""
    if not confirm:
        console.print("[yellow]Pass --confirm to actually reset the database.[/yellow]")
        return

    import os
    if DB_PATH.exists():
        os.remove(DB_PATH)
        console.print(f"[red]Deleted {DB_PATH}[/red]")

    init_db()
    console.print("[green]Database recreated.[/green]")


if __name__ == "__main__":
    cli()
