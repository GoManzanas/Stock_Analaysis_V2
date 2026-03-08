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
    """Map CUSIPs to tickers via EODHD Exchange Symbol List + name search fallback."""
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
    from audit.reconciler import run_reconciliation

    console.print("[bold]Step 1/6: SEC Bulk Download[/bold]")
    with SecBulkScraper() as scraper:
        scraper.run(from_year=from_year, to_year=to_year)

    console.print("\n[bold]Step 2/6: CUSIP Resolution (Bulk + Fallback)[/bold]")
    with CusipResolver() as resolver:
        resolver.run()

    console.print("\n[bold]Step 3/6: Price Download + Price Audit[/bold]")
    with PriceScraper() as scraper:
        scraper.run()

    console.print("\n[bold]Step 4/6: Corporate Actions[/bold]")
    with CorporateActionsScraper() as scraper:
        scraper.run()

    console.print("\n[bold]Step 5/6: Holdings Audit[/bold]")
    init_db()
    conn = get_connection()
    try:
        run_holdings_audit(conn)

        console.print("\n[bold]Step 6/6: Reconciliation[/bold]")
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
        table.add_row("Exchange Symbols", str(get_table_count(conn, "exchange_symbols")))
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


# --- Analytics commands ---

@cli.group()
def analytics():
    """Analyze fund returns, metrics, and screening."""
    pass


@analytics.command("returns")
@click.argument("cik")
@click.option("--cumulative", is_flag=True, help="Show cumulative growth-of-$1 instead of quarterly returns")
def analytics_returns(cik: str, cumulative: bool):
    """Display quarterly returns for a fund."""
    from analytics.returns import compute_quarterly_returns, compute_cumulative_returns

    init_db()
    conn = get_connection()
    try:
        quarterly = compute_quarterly_returns(conn, cik)
        if not quarterly:
            console.print(f"[yellow]No return data found for CIK {cik}.[/yellow]")
            return

        if cumulative:
            data = compute_cumulative_returns(quarterly)
            table = Table(title=f"Cumulative Returns — CIK {cik}")
            table.add_column("Quarter", style="cyan")
            table.add_column("Cumulative Value ($)", justify="right")
            table.add_column("Return (%)", justify="right")
            table.add_column("Confidence (%)", justify="right")

            for row in data:
                ret_pct = f"{row['quarterly_return'] * 100:+.2f}"
                table.add_row(
                    row["report_date"],
                    f"${row['cumulative_value']:.4f}",
                    ret_pct,
                    f"{row['confidence'] * 100:.1f}",
                )
        else:
            data = quarterly
            table = Table(title=f"Quarterly Returns — CIK {cik}")
            table.add_column("Quarter", style="cyan")
            table.add_column("Return (%)", justify="right")
            table.add_column("Confidence (%)", justify="right")
            table.add_column("Position Count", justify="right")
            table.add_column("Total Value", justify="right")

            for row in data:
                ret_pct = f"{row['quarterly_return'] * 100:+.2f}"
                value_str = f"${row['total_value']:,.0f}" if row['total_value'] else "N/A"
                table.add_row(
                    row["report_date"],
                    ret_pct,
                    f"{row['confidence'] * 100:.1f}",
                    str(row["position_count"]),
                    value_str,
                )

        console.print(table)
    finally:
        conn.close()


@analytics.command("metrics")
@click.argument("cik")
def analytics_metrics(cik: str):
    """Display all screening metrics for a single fund."""
    from analytics.screening import compute_fund_metrics

    init_db()
    conn = get_connection()
    try:
        metrics = compute_fund_metrics(conn, cik)

        # Look up fund name
        row = conn.execute(
            "SELECT name FROM filers WHERE cik = ?", (cik,)
        ).fetchone()
        fund_name = row["name"] if row else f"CIK {cik}"

        table = Table(title=f"Fund Metrics — {fund_name}")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", justify="right", style="green")

        formatters = {
            "annualized_return": ("Annualized Return", lambda v: f"{v * 100:.2f}%"),
            "sharpe_ratio": ("Sharpe Ratio", lambda v: f"{v:.3f}"),
            "sp500_correlation": ("S&P 500 Correlation", lambda v: f"{v:.3f}"),
            "max_drawdown": ("Max Drawdown", lambda v: f"{v * 100:.2f}%"),
            "hhi": ("HHI (Concentration)", lambda v: f"{v:.4f}"),
            "top5_concentration": ("Top-5 Weight", lambda v: f"{v * 100:.1f}%"),
            "avg_turnover": ("Avg Quarterly Turnover", lambda v: f"{v * 100:.1f}%"),
            "quarters_active": ("Quarters Active", lambda v: str(int(v))),
            "latest_aum": ("Latest AUM", lambda v: f"${v:,.0f}"),
            "avg_confidence": ("Avg Confidence", lambda v: f"{v * 100:.1f}%"),
        }

        for key, (label, fmt) in formatters.items():
            val = metrics.get(key)
            table.add_row(label, fmt(val) if val is not None else "N/A")

        console.print(table)
    finally:
        conn.close()


@analytics.command("screen")
@click.option("--min-return", type=float, default=None, help="Minimum annualized return (decimal, e.g. 0.10 for 10%)")
@click.option("--max-correlation", type=float, default=None, help="Maximum S&P 500 correlation")
@click.option("--min-quarters", type=int, default=4, help="Minimum quarters active (default: 4)")
@click.option("--min-aum", type=float, default=None, help="Minimum latest AUM in dollars")
@click.option("--min-sharpe", type=float, default=None, help="Minimum Sharpe ratio")
@click.option("--max-drawdown", type=float, default=None, help="Maximum drawdown (negative number, e.g. -0.20)")
@click.option("--min-confidence", type=float, default=0.8, help="Minimum average confidence (default: 0.8)")
@click.option("--sort-by", type=str, default="annualized_return", help="Metric to sort by (default: annualized_return)")
@click.option("--limit", type=int, default=25, help="Max results (default: 25)")
def analytics_screen(min_return, max_correlation, min_quarters, min_aum, min_sharpe, max_drawdown, min_confidence, sort_by, limit):
    """Screen funds by metric thresholds."""
    from analytics.ranking import screen_funds
    from rich.progress import Progress

    init_db()
    conn = get_connection()
    try:
        filters = {
            "min_annualized_return": min_return,
            "max_sp500_correlation": max_correlation,
            "min_quarters_active": min_quarters,
            "min_latest_aum": min_aum,
            "min_sharpe_ratio": min_sharpe,
            "max_max_drawdown": max_drawdown,
            "min_avg_confidence": min_confidence,
        }

        with Progress(console=console) as progress:
            task = progress.add_task("Screening funds...", total=None)
            results = screen_funds(conn, filters=filters, sort_by=sort_by, limit=limit)
            progress.update(task, completed=True)

        if not results:
            console.print("[yellow]No funds matched the screening criteria.[/yellow]")
            return

        table = Table(title="Fund Screening Results")
        table.add_column("Rank", justify="right", style="dim")
        table.add_column("Fund Name", style="cyan", max_width=40)
        table.add_column("CIK", style="dim")
        table.add_column("CAGR (%)", justify="right")
        table.add_column("Sharpe", justify="right")
        table.add_column("Correlation", justify="right")
        table.add_column("Max DD (%)", justify="right")
        table.add_column("Quarters", justify="right")
        table.add_column("AUM", justify="right")

        for i, fund in enumerate(results, 1):
            table.add_row(
                str(i),
                fund.get("name", "Unknown"),
                fund["cik"],
                f"{fund['annualized_return'] * 100:.1f}" if fund.get("annualized_return") is not None else "N/A",
                f"{fund['sharpe_ratio']:.2f}" if fund.get("sharpe_ratio") is not None else "N/A",
                f"{fund['sp500_correlation']:.2f}" if fund.get("sp500_correlation") is not None else "N/A",
                f"{fund['max_drawdown'] * 100:.1f}" if fund.get("max_drawdown") is not None else "N/A",
                str(fund.get("quarters_active", "N/A")),
                f"${fund['latest_aum']:,.0f}" if fund.get("latest_aum") is not None else "N/A",
            )

        console.print(table)
    finally:
        conn.close()


@analytics.command("top")
@click.option(
    "--view",
    type=click.Choice(["top_performers", "contrarian", "concentrated", "long_track_record"]),
    default="top_performers",
    help="Prebuilt screen view (default: top_performers)",
)
@click.option("--limit", type=int, default=25, help="Max results (default: 25)")
def analytics_top(view: str, limit: int):
    """Run prebuilt fund screens."""
    from analytics.ranking import prebuilt_screen
    from rich.progress import Progress

    init_db()
    conn = get_connection()
    try:
        with Progress(console=console) as progress:
            task = progress.add_task(f"Running '{view}' screen...", total=None)
            results = prebuilt_screen(conn, name=view, limit=limit)
            progress.update(task, completed=True)

        if not results:
            console.print(f"[yellow]No funds matched the '{view}' screen.[/yellow]")
            return

        table = Table(title=f"Top Funds — {view.replace('_', ' ').title()}")
        table.add_column("Rank", justify="right", style="dim")
        table.add_column("Fund Name", style="cyan", max_width=40)
        table.add_column("CIK", style="dim")
        table.add_column("CAGR (%)", justify="right")
        table.add_column("Sharpe", justify="right")
        table.add_column("Correlation", justify="right")
        table.add_column("Max DD (%)", justify="right")
        table.add_column("Quarters", justify="right")
        table.add_column("AUM", justify="right")

        for i, fund in enumerate(results, 1):
            table.add_row(
                str(i),
                fund.get("name", "Unknown"),
                fund["cik"],
                f"{fund['annualized_return'] * 100:.1f}" if fund.get("annualized_return") is not None else "N/A",
                f"{fund['sharpe_ratio']:.2f}" if fund.get("sharpe_ratio") is not None else "N/A",
                f"{fund['sp500_correlation']:.2f}" if fund.get("sp500_correlation") is not None else "N/A",
                f"{fund['max_drawdown'] * 100:.1f}" if fund.get("max_drawdown") is not None else "N/A",
                str(fund.get("quarters_active", "N/A")),
                f"${fund['latest_aum']:,.0f}" if fund.get("latest_aum") is not None else "N/A",
            )

        console.print(table)
    finally:
        conn.close()


if __name__ == "__main__":
    cli()
