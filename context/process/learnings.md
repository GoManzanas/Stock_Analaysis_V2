# Learnings

## SEC 13F Bulk Data

- SEC publishes pre-parsed bulk 13F data as quarterly ZIP files with TSV files
- **URL format changed in 2024**: Pre-2024 uses `{year}q{quarter}_form13f.zip`, 2024+ uses date ranges like `01mar2024-31may2024_form13f.zip`
- The 2024+ date ranges are NOT quarterly — they're irregular (Jan-Feb, Mar-May, Jun-Aug, Sep-Nov, Dec-Feb)
- TSV column names differ from what you'd expect: `SSHPRNAMT` (not SHARES), `SSHPRNAMTTYPE`, `PUTCALL`, `NAMEOFISSUER`, `VOTING_AUTH_SOLE/SHARED/NONE`
- The summary page file is `SUMMARYPAGE.tsv` (no underscore)
- Value cutover: ZIPs from 2023Q1 onward have values in actual dollars. 2022Q4 and earlier in thousands. The boundary is by filing date (Jan 3, 2023), but in practice all filings in 2023Q1 ZIP are post-cutover
- Each ZIP also contains: `FORM13F_metadata.json` and `FORM13F_readme.htm`
- SEC dates use format `DD-MMM-YYYY` (e.g., `30-SEP-2023`)
- 13F-HR/A amendments can be RESTATEMENT (replace) or NEW HOLDINGS (append)
- SEC requires a `User-Agent` header with contact info for all requests
- CIK values have leading zeros in the raw data (e.g., `0001234567`) — strip them

## EODHD API

- EOD Historical Data plan ($19.99/mo) covers: EOD prices, splits, dividends, bulk API, ID Mapping, Search
- Daily limit: ~100K API calls
- ID Mapping API: `GET /api/id-mapping?filter[cusip]={CUSIP}` — maps CUSIP to ticker
- SEC uses 9-digit CUSIPs (6 issuer + 2 issue + 1 check). EODHD may expect 6-digit — try both
- The `adjusted_close` field in EOD API response (not `adj_close`) is pre-adjusted for splits + dividends
- Search API prefers US exchange matches when multiple results returned

## SQLite

- WAL mode enables concurrent reads during writes
- Use `INSERT OR IGNORE` for idempotent bulk inserts
- `executemany` with WAL mode handles millions of rows efficiently
- `AUTOINCREMENT` creates an internal `sqlite_sequence` table — exclude it when listing user tables
