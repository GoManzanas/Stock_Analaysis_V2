"""SEC bulk 13F data set downloader and parser.

Downloads quarterly ZIP files from SEC, parses TSV files, and bulk-inserts
into SQLite. Handles the value cutover (thousands vs actual dollars) and
13F-HR/A amendments.
"""

import csv
import io
import logging
import zipfile
from datetime import datetime
from pathlib import Path

import requests
from rich.console import Console

from config.settings import SEC_BULK_BASE_URL, SEC_BULK_DIR, SEC_USER_AGENT, VALUE_CUTOVER_QUARTER
from db.database import get_table_count, insert_or_ignore, transaction, upsert
from scrapers.base import BaseScraper

log = logging.getLogger(__name__)
console = Console()

# --- URL pattern changed in 2024 ---
# 2013-2023: {year}q{quarter}_form13f.zip
# 2024+: date-range format like 01mar2024-31may2024_form13f.zip

# Hardcoded 2024+ ZIP filenames (irregular date ranges, not quarterly)
_NEW_FORMAT_ZIPS = {
    (2024, 1): "01jan2024-29feb2024_form13f.zip",
    (2024, 2): "01mar2024-31may2024_form13f.zip",
    (2024, 3): "01jun2024-31aug2024_form13f.zip",
    (2024, 4): "01sep2024-30nov2024_form13f.zip",
    (2025, 1): "01dec2024-28feb2025_form13f.zip",
    (2025, 2): "01mar2025-31may2025_form13f.zip",
    (2025, 3): "01jun2025-31aug2025_form13f.zip",
    (2025, 4): "01sep2025-30nov2025_form13f.zip",
    (2026, 1): "01dec2025-28feb2026_form13f.zip",
}


def build_quarter_list(from_year: int = 2014, to_year: int = 2025) -> list[dict]:
    """Build list of quarters with their ZIP URLs and metadata.

    Returns list of dicts with keys: year, quarter, quarter_key, url, filename.
    """
    quarters = []
    for year in range(from_year, to_year + 1):
        for q in range(1, 5):
            key = (year, q)
            quarter_key = f"{year}Q{q}"

            if key in _NEW_FORMAT_ZIPS:
                filename = _NEW_FORMAT_ZIPS[key]
            elif year <= 2023:
                filename = f"{year}q{q}_form13f.zip"
            else:
                # Future quarters not yet in _NEW_FORMAT_ZIPS
                continue

            url = SEC_BULK_BASE_URL + filename
            quarters.append({
                "year": year,
                "quarter": q,
                "quarter_key": quarter_key,
                "url": url,
                "filename": filename,
            })
    return quarters


def download_zip(url: str, dest: Path) -> bool:
    """Download a ZIP file from SEC.

    Returns True if downloaded, False if already cached.
    """
    if dest.exists() and dest.stat().st_size > 0:
        log.info("Already cached: %s", dest.name)
        return False

    response = requests.get(
        url,
        headers={"User-Agent": SEC_USER_AGENT},
        stream=True,
        timeout=120,
    )
    response.raise_for_status()

    dest.parent.mkdir(parents=True, exist_ok=True)
    with open(dest, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    return True


def _parse_sec_date(date_str: str) -> str | None:
    """Parse SEC date format (e.g., '30-SEP-2023') to ISO format (2023-09-30)."""
    if not date_str or not date_str.strip():
        return None
    try:
        dt = datetime.strptime(date_str.strip(), "%d-%b-%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        log.warning("Unparseable date: %s", date_str)
        return None


def _quarter_from_date(date_str: str) -> tuple[int, int] | None:
    """Extract (year, quarter) from an ISO date string."""
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
        q = (dt.month - 1) // 3 + 1
        return dt.year, q
    except ValueError:
        return None


def _is_value_in_thousands(quarter_key: str) -> bool:
    """Check if values for this quarter's ZIP are in thousands.

    ZIPs before 2023Q1 have values in thousands of dollars.
    """
    cutover_year = int(VALUE_CUTOVER_QUARTER[:4])
    cutover_q = int(VALUE_CUTOVER_QUARTER[-1])
    q_year = int(quarter_key[:4])
    q_num = int(quarter_key[-1])
    return (q_year, q_num) < (cutover_year, cutover_q)


def _find_in_zip(zip_file: zipfile.ZipFile, filename: str) -> str | None:
    """Find a file in a ZIP, checking root first then subdirectories."""
    if filename in zip_file.namelist():
        return filename
    # Some SEC ZIPs nest files in a subdirectory
    for name in zip_file.namelist():
        if name.endswith("/" + filename):
            return name
    return None


def _read_tsv(zip_file: zipfile.ZipFile, filename: str) -> list[dict]:
    """Read a TSV file from a ZIP and return list of dicts."""
    path = _find_in_zip(zip_file, filename)
    if path is None:
        log.warning("File not found in ZIP: %s", filename)
        return []
    with zip_file.open(path) as f:
        text = io.TextIOWrapper(f, encoding="utf-8", errors="replace")
        reader = csv.DictReader(text, delimiter="\t")
        return list(reader)


def parse_quarter_zip(zip_path: Path, quarter_key: str, conn) -> dict:
    """Parse a quarterly ZIP file and insert data into the database.

    Returns a stats dict with counts of inserted rows.
    """
    value_in_thousands = _is_value_in_thousands(quarter_key)
    stats = {"filers": 0, "filings": 0, "holdings": 0, "securities": 0}

    with zipfile.ZipFile(zip_path) as zf:
        submissions = _read_tsv(zf, "SUBMISSION.tsv")
        coverpages = _read_tsv(zf, "COVERPAGE.tsv")
        infotable = _read_tsv(zf, "INFOTABLE.tsv")
        summarypage = _read_tsv(zf, "SUMMARYPAGE.tsv")

    # Build lookup maps
    coverpage_by_accession = {row["ACCESSION_NUMBER"]: row for row in coverpages}
    summary_by_accession = {row["ACCESSION_NUMBER"]: row for row in summarypage}

    with transaction(conn):
        # --- Insert filers and filings ---
        for sub in submissions:
            accession = sub["ACCESSION_NUMBER"]
            cik = sub["CIK"].lstrip("0") or "0"
            filing_date = _parse_sec_date(sub.get("FILING_DATE", ""))
            report_date = _parse_sec_date(sub.get("PERIODOFREPORT", ""))
            form_type = sub.get("SUBMISSIONTYPE", "")

            cover = coverpage_by_accession.get(accession, {})
            summary = summary_by_accession.get(accession, {})

            manager_name = cover.get("FILINGMANAGER_NAME", "")
            address_parts = [
                cover.get("FILINGMANAGER_STREET1", ""),
                cover.get("FILINGMANAGER_STREET2", ""),
                cover.get("FILINGMANAGER_CITY", ""),
                cover.get("FILINGMANAGER_STATEORCOUNTRY", ""),
                cover.get("FILINGMANAGER_ZIPCODE", ""),
            ]
            address = ", ".join(p for p in address_parts if p and p.strip())

            # Amendment info
            is_amendment = cover.get("ISAMENDMENT", "").upper() in ("Y", "YES", "TRUE", "1")
            amendment_type = cover.get("AMENDMENTTYPE", "").strip() if is_amendment else None

            # Total value from summary
            total_value_raw = summary.get("TABLEVALUETOTAL", "0")
            try:
                total_value = float(total_value_raw or 0)
            except (ValueError, TypeError):
                total_value = 0.0
            if value_in_thousands:
                total_value *= 1000

            holding_count_raw = summary.get("TABLEENTRYTOTAL", "0")
            try:
                holding_count = int(holding_count_raw or 0)
            except (ValueError, TypeError):
                holding_count = 0

            report_yq = _quarter_from_date(report_date)
            report_year = report_yq[0] if report_yq else None
            report_quarter = report_yq[1] if report_yq else None

            # Upsert filer
            upsert(
                conn,
                "filers",
                ["cik", "name", "address"],
                [(cik, manager_name, address)],
                conflict_columns=["cik"],
                update_columns=["name", "address"],
            )

            # Insert filing (skip if accession already exists)
            try:
                conn.execute(
                    """INSERT OR IGNORE INTO filings
                    (cik, accession_number, filing_date, report_date,
                     report_year, report_quarter, form_type, amendment_type,
                     total_value, holding_count, source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (cik, accession, filing_date, report_date,
                     report_year, report_quarter, form_type, amendment_type,
                     total_value, holding_count, "bulk"),
                )
                if conn.execute("SELECT changes()").fetchone()[0] > 0:
                    stats["filings"] += 1
            except Exception as e:
                log.warning("Failed to insert filing %s: %s", accession, e)
                continue

        # Build filing_id lookup
        filing_id_map = {}
        rows = conn.execute(
            "SELECT id, accession_number FROM filings"
        ).fetchall()
        for row in rows:
            filing_id_map[row["accession_number"]] = row["id"]

        # --- Handle amendments (RESTATEMENT) ---
        for sub in submissions:
            accession = sub["ACCESSION_NUMBER"]
            cover = coverpage_by_accession.get(accession, {})
            is_amendment = cover.get("ISAMENDMENT", "").upper() in ("Y", "YES", "TRUE", "1")
            amendment_type_str = cover.get("AMENDMENTTYPE", "").strip().upper() if is_amendment else ""

            if amendment_type_str == "RESTATEMENT":
                cik = sub["CIK"].lstrip("0") or "0"
                report_date = _parse_sec_date(sub.get("PERIODOFREPORT", ""))
                # Delete holdings from original filing(s) for same CIK+report_date
                conn.execute(
                    """DELETE FROM holdings WHERE filing_id IN (
                        SELECT id FROM filings
                        WHERE cik = ? AND report_date = ? AND accession_number != ?
                    )""",
                    (cik, report_date, accession),
                )

        # --- Insert holdings ---
        holdings_batch = []
        cusip_set = set()
        for row in infotable:
            accession = row.get("ACCESSION_NUMBER", "")
            filing_id = filing_id_map.get(accession)
            if filing_id is None:
                continue

            cusip = row.get("CUSIP", "").strip()
            if not cusip:
                continue

            try:
                value = float(row.get("VALUE", 0) or 0)
            except (ValueError, TypeError):
                value = 0.0
            if value_in_thousands:
                value *= 1000

            try:
                shares = float(row.get("SSHPRNAMT", 0) or 0)
            except (ValueError, TypeError):
                shares = 0.0

            put_call = row.get("PUTCALL", "").strip() or None

            holdings_batch.append((
                filing_id,
                cusip,
                row.get("NAMEOFISSUER", ""),
                row.get("TITLEOFCLASS", ""),
                value,
                shares,
                row.get("SSHPRNAMTTYPE", ""),
                put_call,
                row.get("INVESTMENTDISCRETION", ""),
                int(row.get("VOTING_AUTH_SOLE", 0) or 0),
                int(row.get("VOTING_AUTH_SHARED", 0) or 0),
                int(row.get("VOTING_AUTH_NONE", 0) or 0),
            ))
            cusip_set.add((cusip, row.get("NAMEOFISSUER", "")))

        # Bulk insert holdings
        conn.executemany(
            """INSERT INTO holdings
            (filing_id, cusip, issuer_name, class_title, value, shares,
             sh_prn_type, put_call, investment_discretion,
             voting_sole, voting_shared, voting_none)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            holdings_batch,
        )
        stats["holdings"] = len(holdings_batch)

        # --- Populate securities with unresolved CUSIPs ---
        cusip_rows = [(cusip, name) for cusip, name in cusip_set]
        insert_or_ignore(conn, "securities", ["cusip", "name"], cusip_rows)
        stats["securities"] = len(cusip_set)

    # Update filer stats
    conn.execute("""
        UPDATE filers SET
            filing_count = (SELECT COUNT(*) FROM filings WHERE filings.cik = filers.cik),
            first_report_date = (SELECT MIN(report_date) FROM filings WHERE filings.cik = filers.cik),
            last_report_date = (SELECT MAX(report_date) FROM filings WHERE filings.cik = filers.cik),
            total_value_latest = (
                SELECT total_value FROM filings
                WHERE filings.cik = filers.cik
                ORDER BY report_date DESC LIMIT 1
            )
    """)
    conn.commit()
    stats["filers"] = get_table_count(conn, "filers")

    return stats


class SecBulkScraper(BaseScraper):
    """Downloads and parses SEC bulk 13F data sets."""

    job_type = "bulk_13f"

    def run(self, from_year: int = 2014, to_year: int = 2025, **kwargs):
        """Download and parse all quarterly ZIPs in the given year range."""
        quarters = build_quarter_list(from_year, to_year)
        if not quarters:
            console.print("[yellow]No quarters to process.[/yellow]")
            return

        console.print(f"[bold]SEC Bulk 13F Download[/bold]: {len(quarters)} quarters ({from_year}-{to_year})")

        for qi, q in enumerate(quarters):
            if self.is_interrupted:
                break

            quarter_key = q["quarter_key"]
            job_id, progress = self.get_or_create_job(quarter_key)
            job = self.get_job(quarter_key)

            if job and job["status"] == "completed":
                console.print(f"  [dim]{quarter_key}: already done[/dim]")
                continue

            zip_path = SEC_BULK_DIR / q["filename"]

            try:
                # Download
                console.print(f"  [cyan]{quarter_key}[/cyan]: downloading {q['filename']}...")
                downloaded = download_zip(q["url"], zip_path)
                if downloaded:
                    console.print(f"  [cyan]{quarter_key}[/cyan]: downloaded ({zip_path.stat().st_size / 1e6:.1f} MB)")

                # Parse
                console.print(f"  [cyan]{quarter_key}[/cyan]: parsing...")
                stats = parse_quarter_zip(zip_path, quarter_key, self.conn)
                console.print(
                    f"  [green]{quarter_key}[/green]: "
                    f"{stats['filings']} filings, "
                    f"{stats['holdings']} holdings, "
                    f"{stats['securities']} CUSIPs"
                )

                self.complete_job(job_id)

            except requests.HTTPError as e:
                if e.response.status_code == 404:
                    console.print(f"  [yellow]{quarter_key}: ZIP not available (404)[/yellow]")
                    self.fail_job(job_id, f"HTTP 404: {q['url']}")
                else:
                    console.print(f"  [red]{quarter_key}: HTTP error {e.response.status_code}[/red]")
                    self.fail_job(job_id, str(e))
            except Exception as e:
                console.print(f"  [red]{quarter_key}: error: {e}[/red]")
                self.fail_job(job_id, str(e))

        if self.is_interrupted:
            console.print("[yellow]Interrupted. Run again to resume.[/yellow]")
        else:
            console.print("[bold green]SEC bulk download complete.[/bold green]")
