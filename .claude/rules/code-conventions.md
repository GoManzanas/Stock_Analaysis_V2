---
alwaysApply: true
---

# Core Architecture & Code Conventions

## Project Structure

```
config/settings.py     — All configuration (paths, API keys, constants)
db/schema.sql          — SQLite DDL
db/database.py         — Connection helpers, init, transactions
scrapers/base.py       — BaseScraper with job tracking + SIGINT
scrapers/sec_bulk.py   — SEC bulk 13F download + parse
scrapers/eodhd_*.py    — EODHD API scrapers (mapping, prices, corporate)
audit/*.py             — Data quality auditors
cli/main.py            — Click CLI entry point
```

## Code Style

- Use type hints in function signatures
- Use `pathlib.Path` for file paths
- Use parameterized SQL queries (never f-strings for SQL)
- Use context managers for database transactions
- Prefer `logging` module over `print`

## Scraper Rules

- **All scrapers must be idempotent** — running twice produces the same result
- **All scrapers must be resumable** — track progress in `scrape_jobs` table
- **SIGINT handling** — check `is_interrupted` flag between batch boundaries
- **Batch commits** — commit at natural boundaries (per-quarter, per-100-tickers)
- **Rate limiting** — respect API limits, configurable delays

## Database Rules

- SQLite WAL mode, foreign keys enabled
- All `holdings.value` stored in actual dollars (normalize at parse time)
- ISO 8601 date strings
- Use `INSERT OR IGNORE` for idempotent inserts
- One transaction per logical batch (e.g., one SEC quarterly ZIP)

## SEC 13F Rules

- **Value cutover**: `report_date < 2023-01-01` → multiply value × 1000
- **Amendments**: RESTATEMENT replaces; NEW HOLDINGS appends
- **CUSIPs**: SEC uses 9-digit; try both 9 and 6 digit for EODHD lookups
