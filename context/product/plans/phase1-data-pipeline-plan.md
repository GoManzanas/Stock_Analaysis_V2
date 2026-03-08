# Phase 1: 13F Fund Analysis Data Pipeline

## Context

Build a Python CLI tool that downloads all SEC 13F filings (2014-2025), resolves CUSIPs to tickers via EODHD, downloads historical prices, and runs data quality audits. This replaces the current Chrome extension scaffold entirely.

**Why**: To analyze ~6,000-7,000 institutional investment managers' quarterly holdings, compute portfolio returns, and screen for funds with interesting characteristics (low S&P correlation, high returns, concentrated conviction).

**Outcome**: A populated SQLite database with ~6M holding rows, ~10K resolved securities with price histories, and audit reports — all accessible via a CLI.

---

## Scope (Phase 1 only)

**In scope**: Bulk SEC pipeline, CUSIP resolution, price download, corporate actions, audit pipeline, CLI
**Deferred**: Incremental scraper (edgartools for 2026+), analytics engine, API layer, React frontend

---

## Implementation Chunks

### Chunk 0: Project Scaffold (30 min)
- Remove all Chrome extension files (src/, manifest.json, vite.config.ts, package.json, etc.)
- Create Python project structure:
  ```
  config/__init__.py, settings.py
  db/__init__.py, schema.sql, database.py
  scrapers/__init__.py, base.py, sec_bulk.py, eodhd_mapping.py, eodhd_prices.py, eodhd_corporate.py
  audit/__init__.py, price_auditor.py, holdings_auditor.py, reconciler.py
  cli/__init__.py, main.py
  scripts/seed.py
  tests/
  data/sec_bulk/  (gitignored)
  ```
- Write `requirements.txt`: click, rich, requests, python-dotenv, pytest
- Create `.env` with `EODHD_API_KEY=66187de7da4855.83824808`
- Create `.env.example` with `EODHD_API_KEY=your_key_here`
- Update `.gitignore` for Python (`__pycache__/`, `*.pyc`, `.venv/`, `data/`, `*.db`)
- Write `config/settings.py`: paths, API key from .env, SEC URL template, value cutover date
- Update `CLAUDE.md`, `context/architecture/` docs for Python project

**Verify**: `pip install -r requirements.txt && python -c "from config.settings import *"`

### Chunk 1: Database Schema + Connection Layer (1-2 hrs)
- `db/schema.sql`: all 8 tables (filers, filings, holdings, securities, corporate_actions, prices, benchmark_prices, scrape_jobs) + audit_results table + all indexes
- `db/database.py`: `get_connection()` (WAL mode, foreign keys), `init_db()`, transaction context manager, `execute_many()` helper
- Key: all `holdings.value` stored in actual dollars (cutover normalization at parse time)
- `tests/test_database.py`: init, CRUD on each table, WAL mode verification

**Verify**: `pytest tests/test_database.py`

### Chunk 2: Scrape Jobs + SIGINT Handling (1-2 hrs)
- `scrapers/base.py`: `BaseScraper` with job tracking (create, update progress, complete, fail, resume)
- SIGINT handler: sets `is_interrupted` flag, scraper checks between batch boundaries
- Rich progress display helper
- `tests/test_scrape_jobs.py`: job lifecycle, resume logic, interruption simulation

**Verify**: `pytest tests/test_scrape_jobs.py`

### Chunk 3: SEC Bulk Downloader — Download (1-2 hrs)
- `scrapers/sec_bulk.py` download portion:
  - `build_quarter_urls(from_year, to_year)` — URL pattern: `https://www.sec.gov/files/structureddata/data/form-13f-data-sets/{YYYYQN}_form13f.zip`
  - `download_quarter(url, dest)` with Rich progress bar
  - Cache in `data/sec_bulk/`, skip if already downloaded
  - SEC requires `User-Agent` header with contact info
- `tests/test_sec_bulk_download.py`: URL generation, caching logic

**Verify**: Download one real quarter's ZIP successfully

### Chunk 4: SEC Bulk Parser — Parse + Insert (2-3 hrs) **CRITICAL**
- `scrapers/sec_bulk.py` parse portion:
  - Parse SUBMISSION.tsv → `filings`, COVERPAGE.tsv → `filers`, INFOTABLE.tsv → `holdings`, SUMMARY_PAGE.tsv → `filings.total_value`
  - **Value cutover**: `report_date < 2023-01-01` → multiply value by 1000
  - **Amendments**: RESTATEMENT → delete+replace holdings for same CIK+report_period; NEW HOLDINGS → append
  - Populate `securities` table with distinct CUSIPs (ticker=NULL)
  - One transaction per quarter ZIP
- `tests/test_sec_bulk_parse.py`: synthetic ZIP with all TSV files, test cutover, amendments, idempotency

**Risk**: TSV encoding, exact column names, amendment matching logic
**Verify**: Parse a real ZIP, verify row counts and known fund holdings against SEC EDGAR

### Chunk 5: CLI Framework (1 hr)
- `cli/main.py`: Click group with `scrape bulk`, `status`, `reset` commands
- Rich live progress display during scrape
- Entry point: `python -m cli.main`

**Verify**: `python -m cli.main scrape bulk --from-year 2024 --to-year 2024` works end-to-end

### Chunk 6: CUSIP Resolution (1-2 hrs)
- `scrapers/eodhd_mapping.py`:
  - Primary: `GET /api/id-mapping?filter[cusip]={CUSIP}` — try 9-digit and 6-digit
  - Fallback: `GET /api/search/{ISSUER_NAME}` with fuzzy matching
  - Store in `securities` with `resolution_source`, `resolution_confidence` (1.0=exact, 0.8=high, 0.5=fuzzy, 0.0=unresolved)
  - Rate limiting, resumability (skip already-resolved)
- `tests/test_eodhd_mapping.py`: mock API, 6/9-digit handling, confidence scoring

**Verify**: Resolve 10 known CUSIPs (e.g., AAPL=037833100) against real API

### Chunk 7: Corporate Actions (1 hr)
- `scrapers/eodhd_corporate.py`:
  - Splits: `GET /api/splits/{TICKER}.US?from=2010-01-01&fmt=json`
  - Symbol changes: `GET /api/exchange-symbol-list/US` with symbol change history
  - Store in `corporate_actions` with parsed details JSON
- `tests/test_eodhd_corporate.py`: mock API, split ratio parsing

**Verify**: Download splits for AAPL, TSLA, NVDA — verify against known split history

### Chunk 8: Price Download (1-2 hrs) — *parallel with Chunk 7*
- `scrapers/eodhd_prices.py`:
  - `GET /api/eod/{TICKER}.US?from=2010-01-01&fmt=json`
  - `INSERT OR IGNORE` for idempotency
  - Benchmarks: SPY.US and GSPC.INDX → `benchmark_prices`
  - Batch commits per 100 tickers, resumable (check `max(date)` per ticker)
- `tests/test_eodhd_prices.py`: mock API, incremental download, benchmark

**Verify**: Download 10 tickers, verify date ranges and adj_close presence

### Chunk 9: Audit Pipeline (2-3 hrs)
- `audit/holdings_auditor.py`: value-in-thousands detection (implied_price vs adj_close), filing errors (zero shares, negatives, duplicates), amendment verification
- `audit/price_auditor.py`: day-over-day outliers (>50%), cross-ref against corporate_actions, stale price detection
- `audit/reconciler.py`: sum(shares * adj_close) vs reported total_value, flag >10% discrepancies
- `tests/test_auditors.py`: synthetic data with planted anomalies

**Verify**: Auditors catch all planted anomalies in test data

### Chunk 10: Full Integration + CLI Completion (1-2 hrs)
- Complete CLI: `pipeline`, `resolve cusips`, `download prices`, `download corporate`, `audit holdings/prices/reconcile`, `status --detail`, `resume`
- `scripts/seed.py`: one-command bootstrap
- Integration test with 1 real quarter
- Update README.md, CLAUDE.md, context/ docs

**Verify**: `python -m cli.main pipeline --from-year 2024 --to-year 2024` runs full pipeline end-to-end

---

## Dependency Graph

```
Chunk 0 (scaffold)
  └─> Chunk 1 (DB schema)
        └─> Chunk 2 (scrape jobs)
              ├─> Chunk 3 (SEC download)
              │     └─> Chunk 4 (SEC parse) ──┐
              │           └─> Chunk 5 (CLI)    │
              └─> Chunk 6 (CUSIP resolve) <────┘
                    ├─> Chunk 7 (corporate)  ──┐
                    └─> Chunk 8 (prices)  ─────┤
                                               └─> Chunk 9 (audit)
                                                     └─> Chunk 10 (integration)
```

**Critical path**: 0 → 1 → 2 → 3 → 4 → 6 → 8 → 9 → 10

---

## Risk Register

| Risk | Impact | Mitigation |
|------|--------|------------|
| SEC bulk ZIP URL format | Blocks download | Verify URL pattern manually before coding |
| TSV parsing edge cases | Data corruption | Use `csv.reader` with `dialect='excel-tab'`, handle encoding errors |
| EODHD API response format | Blocks resolution | Make test API calls before building parsers |
| Amendment handling | Double-counting | Comprehensive test cases, verify against SEC EDGAR |
| Value cutover boundary (Jan 2023) | 1000x error | Audit pipeline catches this, test around boundary |
| 6-digit vs 9-digit CUSIP | Failed resolution | Try both in sequence, log which worked |

---

## Verification (End-to-End)

1. `python -m cli.main scrape bulk --from-year 2024 --to-year 2024` — downloads and parses 4 quarters
2. `python -m cli.main resolve cusips` — resolves CUSIPs to tickers
3. `python -m cli.main download corporate` — fetches splits/symbol changes
4. `python -m cli.main download prices` — fetches EOD prices
5. `python -m cli.main audit holdings` — validates holdings data
6. `python -m cli.main audit prices` — validates price data
7. `python -m cli.main audit reconcile` — cross-validates holdings vs prices
8. `python -m cli.main status` — shows complete pipeline status dashboard

---

## Key Files Reference

| File | Purpose |
|------|---------|
| `.context/attachments/FUND_IMPLEMENTATION_PLAN.md` | Full spec with schemas, APIs, audit logic |
| `config/settings.py` | Paths, API key, constants |
| `db/schema.sql` | All table DDL + indexes |
| `db/database.py` | Connection, init, helpers |
| `scrapers/base.py` | BaseScraper with job tracking + SIGINT |
| `scrapers/sec_bulk.py` | SEC bulk download + parse |
| `scrapers/eodhd_mapping.py` | CUSIP → ticker resolution |
| `scrapers/eodhd_prices.py` | EOD price download |
| `scrapers/eodhd_corporate.py` | Splits, symbol changes |
| `audit/*.py` | Holdings, price, reconciliation auditors |
| `cli/main.py` | Click CLI with Rich progress |
