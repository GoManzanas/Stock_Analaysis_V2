# Project: 13F Fund Analyst

## Overview

A Python CLI tool that builds a comprehensive database of SEC 13F institutional holdings filings, resolves securities to tickers, downloads historical prices, and audits data quality. Covers ~6,000-7,000 quarterly filers from 2014 onward.

## Architecture

```
SEC Bulk ZIPs → Parser → SQLite DB ← EODHD APIs (prices, mappings, corporate actions)
                              ↓
                        Audit Pipeline → audit_results table
                              ↓
                         CLI (Click + Rich)
```

**Data sources**:
- SEC bulk 13F data sets (quarterly ZIPs with TSV files)
- EODHD API: ID mapping (CUSIP→ticker), EOD prices, splits, dividends, symbol changes

**Stack**: Python 3.12+, SQLite (WAL mode), Click, Rich, requests, python-dotenv

## Key Files

| Path | Purpose |
|------|---------|
| `config/settings.py` | Paths, API keys, constants |
| `db/schema.sql` | SQLite DDL for all tables |
| `db/database.py` | Connection pool, helpers, init |
| `scrapers/base.py` | BaseScraper with job tracking + SIGINT |
| `scrapers/sec_bulk.py` | SEC bulk 13F downloader + parser |
| `scrapers/eodhd_mapping.py` | CUSIP → ticker resolver |
| `scrapers/eodhd_prices.py` | Historical price downloader |
| `scrapers/eodhd_corporate.py` | Splits, dividends, symbol changes |
| `audit/holdings_auditor.py` | Validate 13F holdings data |
| `audit/price_auditor.py` | Detect price anomalies |
| `audit/reconciler.py` | Cross-check holdings vs prices |
| `cli/main.py` | Click CLI entry point |
| `scripts/seed.py` | One-command bootstrap |

## Development

```bash
pip install -r requirements.txt   # Install dependencies
python -m cli.main --help         # CLI usage
python -m cli.main pipeline       # Run full pipeline
python -m cli.main status         # Show pipeline status
pytest                            # Run tests
```

## Key Conventions

- **All scrapers are idempotent and resumable** — track progress in `scrape_jobs` table
- **Value cutover**: pre-2023 SEC values in thousands (multiply by 1000), post-2023 in actual dollars
- **SIGINT handling**: clean interruption at batch boundaries, resume from saved state
- **No ORM** — raw SQL with parameterized queries
- **SQLite WAL mode** for concurrent reads during writes
- Store all `holdings.value` in actual dollars (normalize at parse time)

## Conventions

### Before Making Changes

- Read the relevant file(s) first
- Check `context/product/decisions.md` for prior decisions
- Check `context/process/learnings.md` for known gotchas

### After Making Changes

- If the change involved a non-obvious decision, log it in `context/product/decisions.md`
- If we learned something useful, add it to `context/process/learnings.md`

@context/architecture/concepts.md
@context/architecture/conventions.md
@context/process/learnings.md
