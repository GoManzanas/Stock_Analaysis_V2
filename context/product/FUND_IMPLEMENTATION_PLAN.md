# 13F Fund Analysis Tool — Implementation Plan

## Scope & Scale

**Universe**: All institutional investment managers who filed a 13F-HR for Q4 2025 (report period ending 12/31/2025, due by 2/14/2026). This is approximately **6,000–7,000 filers** per quarter, each with anywhere from 1 to 5,000+ holdings. Across all filers, the unique CUSIP universe is likely 8,000–12,000 distinct securities.

**Historical depth**: All quarterly 13F filings from 2014 onward (when SEC mandated XML format). Older filings (2010–2013) are plain text with inconsistent formatting — defer to Phase 3.

**Price data**: EODHD historical EOD prices for every security that appears in any 13F holding, going back to 2010 or the ticker's listing date.

---

## Key Discovery: SEC Bulk 13F Data Sets

The SEC publishes **pre-parsed, flat-file bulk downloads** of ALL 13F filings per quarter at:
`https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets`

Each quarterly ZIP contains 7 TSV files:

| File | Description | Key Fields |
|------|-------------|------------|
| SUBMISSION | One row per filing | ACCESSION_NUMBER, CIK, FILING_DATE, REPORT_PERIOD |
| COVERPAGE | Filing metadata | MANAGER_NAME, MANAGER_ADDRESS, REPORT_TYPE |
| OTHERMANAGER | Co-managers | OTHER_MANAGER_NAME, CIK |
| OTHERMANAGER2 | Additional managers | Name only |
| INFOTABLE | **The holdings** | CUSIP, ISSUER_NAME, CLASS_TITLE, VALUE, SHARES, SH_PRN_TYPE, PUT_CALL, INVESTMENT_DISCRETION, VOTING_SOLE/SHARED/NONE |
| SIGNATURE | Signatory info | NAME, TITLE, PHONE, CITY, STATE |
| SUMMARY_PAGE | Filing summary | TOTAL_VALUE, ENTRY_TOTAL |

**Critical note from SEC docs**: Starting January 3, 2023, market value in INFOTABLE is reported in **actual dollars** (rounded to nearest dollar). Pre-2023, it was in **thousands**. The code must handle this cutover.

**This changes our approach entirely**: Instead of scraping thousands of individual EDGAR filings, we download ~40 quarterly ZIP files (Q1 2014 through Q4 2025) and bulk-load them. This is faster by orders of magnitude, more reliable, and gives us the complete universe automatically.

**We still use `edgartools`** for:
- Looking up individual filings on demand (e.g. user drills into a specific fund)
- Fetching filings from 2026+ that aren't yet in the bulk data sets
- Future expansion to Form 4, 10-K, etc.

---

## Key Discovery: EODHD Has a CUSIP→Symbol Mapping API

EODHD offers an **ID Mapping API** (`CUSIP / ISIN / FIGI / LEI / CIK ↔ Symbol`). This solves the hardest problem in 13F analysis — mapping the CUSIPs in SEC filings to tradeable tickers for price lookups. The SEC's own 13F data only has CUSIPs and issuer names, not tickers.

EODHD also provides:
- **Splits API**: `/api/splits/{TICKER}.US` — full history of stock splits with ratios
- **Dividends API**: `/api/div/{TICKER}.US`
- **Bulk EOD API**: Download entire exchange prices for a single day in one call
- **Symbol Change History**: Via Exchanges API — tracks ticker renames
- **Insider Transactions API** (Form 4): Useful for Phase 3
- **Adjusted close prices**: `adj_close` field is pre-adjusted for splits + dividends

---

## Confirmed Decisions

| Decision | Choice | Implication |
|----------|--------|-------------|
| EODHD plan | EOD Historical Data ($19.99/mo) | Covers all needed endpoints: EOD prices, splits/dividends, bulk API, Search API, **and ID Mapping API** (CUSIP→Symbol). Does NOT include Fundamentals or Calendar — not needed for Phase 1. Daily limit is ~100K API calls. |
| PUT/CALL options | Track separately, exclude from returns | Store in holdings with `put_call` flag. Filter them out during return calculations. Still visible in fund profile views. |
| Git strategy | `.gitignore` DB + rebuild script | Ship `seed.py` that downloads SEC bulk ZIPs and rebuilds from scratch. DB will be 500MB–1GB at full scale. |

### EODHD Endpoints Confirmed Available on EOD Historical Data Plan

| Endpoint | URL Pattern | Cost | Use Case |
|----------|-------------|------|----------|
| EOD Prices | `/api/eod/{TICKER}.US` | 1 call/ticker | Historical OHLCV + adj_close |
| Bulk EOD | `/api/eod-bulk-last-day/US` | 100 calls/exchange | Daily incremental updates |
| Splits | `/api/splits/{TICKER}.US` | 1 call/ticker | Split history for audit |
| Dividends | `/api/div/{TICKER}.US` | 1 call/ticker | Dividend history for audit |
| ID Mapping | `/api/id-mapping?filter[cusip]=...` | 1 call/request | CUSIP → ticker resolution |
| Search | `/api/search/{QUERY}` | 1 call/request | Fallback name-based ticker lookup |
| Exchange Symbols | `/api/exchange-symbol-list/US` | 1 call | Full ticker list + symbol changes |

---

## Phase 1: Data Pipeline (Scrape, Store, Audit)

### 1.1 — Project Scaffolding

**Stack**: Python 3.12+, SQLite (WAL mode), Click (CLI framework), Rich (terminal UI)

```
fund-analyst/
├── config/
│   └── settings.py              # Paths, API keys, constants
├── db/
│   ├── schema.sql               # SQLite DDL
│   ├── database.py              # Connection pool, helpers, migrations
│   └── migrations/              # Schema versioning (simple numbered SQL files)
├── scrapers/
│   ├── sec_bulk.py              # SEC bulk 13F data set downloader + parser
│   ├── sec_incremental.py       # edgartools-based scraper for recent/individual filings
│   ├── eodhd_prices.py          # EODHD historical price downloader
│   ├── eodhd_mapping.py         # CUSIP → ticker resolver using EODHD ID Mapping API
│   └── eodhd_corporate.py       # Splits, dividends, symbol changes from EODHD
├── audit/
│   ├── price_auditor.py         # Detect and flag split/rename anomalies
│   ├── holdings_auditor.py      # Detect value-in-thousands vs dollars issues
│   └── reconciler.py            # Cross-check holdings values against prices × shares
├── cli/
│   └── main.py                  # Click CLI with Rich progress bars
├── scripts/
│   └── seed.py                  # One-command bootstrap
├── data/                        # SQLite DB + downloaded ZIPs (gitignored)
│   └── sec_bulk/                # Cached SEC quarterly ZIPs
├── .env.example
├── requirements.txt
└── README.md
```

**Key design decisions**:
- SQLite rather than Postgres (local tool, single user, git-friendly rebuild script)
- `.gitignore` the DB; include a `seed.py` that can rebuild from scratch
- All scrapers are **idempotent** and **resumable** — they track progress in a `scrape_jobs` table and skip already-completed work
- CLI uses Rich for live progress tables, spinners, and color

### 1.2 — Database Schema

**Tables** (beyond what was already drafted):

```
filers
  cik TEXT PK
  name TEXT
  address TEXT
  first_report_date TEXT
  last_report_date TEXT
  filing_count INT
  total_value_latest REAL        -- most recent filing's total value

filings
  id INTEGER PK
  cik TEXT FK → filers
  accession_number TEXT UNIQUE
  filing_date TEXT
  report_date TEXT               -- quarter end (e.g. 2025-12-31)
  report_year INT                -- derived, for fast queries
  report_quarter INT             -- derived (1-4)
  form_type TEXT                 -- 13F-HR, 13F-HR/A
  amendment_type TEXT            -- RESTATEMENT, NEW HOLDINGS, or NULL
  total_value REAL
  holding_count INT
  source TEXT                    -- 'bulk' or 'edgartools'
  scraped_at TEXT

holdings
  id INTEGER PK
  filing_id INT FK → filings
  cusip TEXT
  issuer_name TEXT
  class_title TEXT
  value REAL                     -- always stored in actual dollars
  shares REAL
  sh_prn_type TEXT               -- SH or PRN
  put_call TEXT                  -- PUT, CALL, or NULL
  investment_discretion TEXT
  voting_sole INT
  voting_shared INT
  voting_none INT

securities
  cusip TEXT PK
  ticker TEXT                    -- resolved via EODHD
  eodhd_symbol TEXT              -- e.g. AAPL.US
  name TEXT
  security_type TEXT             -- equity, etf, option, etc
  exchange TEXT
  is_active BOOLEAN
  resolved_at TEXT
  resolution_source TEXT         -- 'eodhd_mapping', 'name_match', 'manual'
  resolution_confidence REAL     -- 0-1, how sure we are of the mapping

corporate_actions
  id INTEGER PK
  ticker TEXT
  action_type TEXT               -- 'split', 'reverse_split', 'symbol_change',
                                 -- 'delisted', 'merger', 'spinoff'
  effective_date TEXT
  details TEXT                   -- JSON: split ratio, old/new symbol, etc
  source TEXT                    -- 'eodhd', 'manual'

prices
  ticker TEXT
  date TEXT
  open REAL
  high REAL
  low REAL
  close REAL
  adj_close REAL                 -- split+dividend adjusted (from EODHD)
  volume INT
  PK (ticker, date)

benchmark_prices
  date TEXT PK
  ticker TEXT                    -- 'SPY' or 'GSPC'
  adj_close REAL

scrape_jobs
  id INTEGER PK
  job_type TEXT                  -- 'bulk_13f', 'incremental_13f', 'cusip_resolve',
                                 -- 'price_download', 'audit'
  target TEXT                    -- quarter key (e.g. '2025Q4'), CIK, ticker
  status TEXT                    -- 'pending', 'running', 'completed', 'failed', 'interrupted'
  progress TEXT                  -- JSON: {current: 500, total: 7000, ...}
  error_message TEXT
  started_at TEXT
  completed_at TEXT
  resumed_count INT DEFAULT 0
```

**Indexes**: On `holdings(cusip)`, `holdings(filing_id)`, `filings(cik, report_date)`, `filings(report_year, report_quarter)`, `prices(date)`, `securities(ticker)`.

### 1.3 — SEC Bulk 13F Downloader (`sec_bulk.py`)

**What it does**: Downloads quarterly ZIP files from SEC, parses the TSV files, and bulk-inserts into SQLite.

**Steps**:

1. **Build URL list**: `https://www.sec.gov/files/structureddata/data/form-13f-data-sets/{date_range}_form13f.zip` for each quarter from Q1 2014 to latest available.

2. **Download with caching**: Save ZIPs to `data/sec_bulk/`. Skip if already downloaded (check file hash). Show Rich progress bar during download.

3. **Parse each ZIP**:
   - Read SUBMISSION.tsv → insert into `filings` table
   - Read COVERPAGE.tsv → update `filers` table with manager info
   - Read INFOTABLE.tsv → insert into `holdings` table
   - **Handle the value cutover**: If `report_date < 2023-01-01`, multiply value by 1000. If `>= 2023-01-01`, store as-is.

4. **Handle amendments**: 13F-HR/A filings are amendments. When we encounter one:
   - If `amendment_type = 'RESTATEMENT'`: replace the original filing's holdings entirely
   - If `amendment_type = 'NEW HOLDINGS'`: add to the original filing's holdings
   - Track this logic carefully — it's a common source of double-counting

5. **Resumability**: Track each quarterly ZIP in `scrape_jobs`. If interrupted mid-parse, on restart skip completed quarters and resume from the current one. Use SQLite transactions per-quarter (one commit per ZIP file, not per row).

**Estimated scale**: ~40 quarterly ZIPs × ~150K rows in INFOTABLE each = ~6M holding rows total. SQLite handles this fine. Bulk insert with executemany + WAL mode should take 5-10 minutes total.

### 1.4 — Incremental Scraper (`sec_incremental.py`)

**What it does**: Uses `edgartools` to fetch filings that are too recent for the bulk data sets (e.g., Q1 2026 filings that haven't been published in a bulk set yet).

**Steps**:

1. Determine which quarters we're missing from the bulk data (e.g., after the latest bulk ZIP).
2. Use `edgartools` `get_filings(form="13F-HR")` to find all 13F filings in the missing date range.
3. For each filing, call `.obj()` to get the parsed ThirteenF object.
4. Extract holdings from `.infotable.to_pandas()` and insert into the same schema.
5. Mark `source='edgartools'` to distinguish from bulk data.

**Rate limiting**: SEC allows 10 req/s. `edgartools` handles this internally but we should stay conservative (8 req/s).

**Resumability**: Track per-CIK in `scrape_jobs`. On restart, skip CIKs already completed for the target quarter.

### 1.5 — CUSIP → Ticker Resolution (`eodhd_mapping.py`)

**What it does**: Maps the ~8,000–12,000 unique CUSIPs in our holdings data to tradeable tickers using EODHD.

**Steps**:

1. **Extract distinct CUSIPs** from `securities` table where `ticker IS NULL`.

2. **EODHD ID Mapping API** (primary method — confirmed available on your plan):
   ```
   GET https://eodhd.com/api/id-mapping?filter[cusip]={CUSIP}&api_token={KEY}&fmt=json
   ```
   Returns: `{symbol: "AAPL.US", isin: "US0378331005", figi: "BBG000B9XRY4", ...}`
   Each call costs 1 API credit. For 10K CUSIPs, that's 10K calls — well within the ~100K daily limit.

3. **Fallback — Name matching**: For CUSIPs that EODHD can't resolve, fuzzy-match the `issuer_name` from SEC data against EODHD's symbol search API:
   ```
   GET https://eodhd.com/api/search/{ISSUER_NAME}
   ```

4. **Fallback — SEC's 13(f) securities list**: The SEC publishes a quarterly list of all Section 13(f) securities with CUSIPs and issuer names. Cross-reference this as an additional signal.

5. **Store results** in `securities` table with `resolution_confidence` score:
   - 1.0: Exact CUSIP match from EODHD
   - 0.8: High-confidence name match
   - 0.5: Fuzzy name match (needs manual review)
   - 0.0: Unresolved

6. **Handle 6-digit vs 9-digit CUSIPs**: SEC filings use 9-digit CUSIPs (6 issuer + 2 issue + 1 check digit). EODHD may expect 6-digit. Try both.

**Resumability**: Track resolved vs unresolved CUSIPs. On restart, only attempt unresolved ones.

**Output**: A mapping table that enables us to look up prices for any security in any 13F filing.

### 1.6 — Price Data Download (`eodhd_prices.py`)

**What it does**: Downloads historical EOD prices for every ticker in our securities table.

**Steps**:

1. **Get ticker list**: All tickers from `securities` where `ticker IS NOT NULL` and `is_active = TRUE`.

2. **Download per ticker**:
   ```
   GET https://eodhd.com/api/eod/{TICKER}.US?from=2010-01-01&fmt=json
   ```
   Returns OHLCV + adj_close for entire history. Each call = 1 API credit.

3. **Bulk insert** into `prices` table. Use `INSERT OR IGNORE` for idempotency.

4. **Download benchmark**: S&P 500 via `GSPC.INDX` (index) and `SPY.US` (ETF). Store in `benchmark_prices`.

5. **Batch optimization**: For daily incremental updates, use EODHD's **Bulk API**:
   ```
   GET https://eodhd.com/api/eod-bulk-last-day/US
   ```
   Returns entire exchange for one day in a single call. Use this for ongoing updates instead of per-ticker calls.

**API budget estimate**: ~10,000 tickers × 1 call each = 10,000 calls for initial load. Your EOD Historical Data plan allows ~100K calls/day, so this is feasible in one run. Incremental updates use the bulk endpoint (1 call per exchange per day).

**Options exclusion**: Tickers resolved from holdings with `put_call IS NOT NULL` are still downloaded for completeness, but flagged in the `securities` table as `security_type='option'`. The analytics engine (Phase 2) filters these out of return calculations.

**Resumability**: Track which tickers have been downloaded and their `max(date)`. On restart, only download tickers with missing or stale data, using `from` parameter to fetch only new dates.

### 1.7 — Corporate Actions Download (`eodhd_corporate.py`)

**What it does**: Downloads split history, dividend history, and symbol change history for all tickers.

**Steps**:

1. **Splits** per ticker:
   ```
   GET https://eodhd.com/api/splits/{TICKER}.US?from=2010-01-01&fmt=json
   ```
   Returns `[{date, split: "4/1"}, ...]`

2. **Symbol changes** (exchange-level):
   ```
   GET https://eodhd.com/api/exchange-symbol-list/US?type=SYMBOL_CHANGE_HISTORY
   ```
   or via the Exchanges API for ticker rename history.

3. **Store** in `corporate_actions` table with parsed details.

4. **Build a CUSIP→ticker timeline**: Some CUSIPs map to different tickers at different points in time (due to mergers, renames). The `corporate_actions` table lets us build a time-aware mapping.

### 1.8 — Price Data Auditing (`audit/price_auditor.py`)

**What it does**: Detects and flags anomalies in the price data that indicate unaccounted-for splits, reverse splits, ticker changes, or data quality issues.

**Audit checks**:

1. **Day-over-day return outlier detection**:
   - Flag any single-day price change > ±50% (likely a split or data error)
   - Cross-reference against `corporate_actions` table
   - If a split exists on that date, verify the ratio matches the price move
   - If no split record exists, flag for investigation

2. **Close vs adj_close divergence**:
   - EODHD provides both raw `close` and split+dividend-adjusted `adj_close`
   - Compare cumulative adjustment factor: `close / adj_close` over time
   - Discontinuities in this ratio indicate an adjustment event
   - Verify each discontinuity against a known split or dividend

3. **Volume spike validation**:
   - Splits typically cause a proportional volume change (e.g., 4:1 split → ~4× volume)
   - Flag cases where price halves but volume doesn't double (or vice versa)

4. **Share count consistency check**:
   - For a given fund, if shares held doubles between quarters but value stays flat, likely a stock split occurred
   - Cross-reference with `corporate_actions`
   - This catches splits that EODHD might have missed

5. **Stale price detection**:
   - Flag tickers where the last price date is significantly before the current date (possible delisting)
   - Cross-reference with EODHD's delisted companies endpoint

6. **CUSIP change detection**:
   - Some securities change CUSIPs (e.g., after a reverse split, the CUSIP changes)
   - Detect when a CUSIP disappears from all 13F filings and a new CUSIP appears with the same issuer name
   - Link them in the `securities` table

**Output**: An `audit_results` table with findings categorized as `auto_fixed`, `needs_review`, and `confirmed_ok`. The CLI shows a summary and lets you drill in.

### 1.9 — Holdings Auditor (`audit/holdings_auditor.py`)

**What it does**: Validates the 13F holdings data itself.

**Audit checks**:

1. **Value-in-thousands misdetection**:
   - For each holding, compute `implied_price = value / shares`
   - Compare `implied_price` against the actual `adj_close` on the report date
   - If `implied_price` is ~1000× the actual price, the value was reported in thousands but we didn't multiply
   - If `implied_price` is ~0.001× the actual price, we over-multiplied
   - Auto-fix where confidence is high

2. **Obvious data errors** (as noted in Todd's 13f.info caveats):
   - Values overstated by exactly 1000× (common filing error)
   - Zero-share holdings with non-zero values
   - Negative values or shares
   - Duplicate CUSIPs within the same filing (legitimate if different PUT/CALL or class titles)

3. **Amendment handling verification**:
   - Ensure restatements properly replace originals
   - Ensure "new holdings" add without duplicating
   - Flag filings that have both an original and a restatement but different total values (expected, but worth logging)

### 1.10 — Reconciler (`audit/reconciler.py`)

**What it does**: Cross-validates holdings × prices to ensure our computed portfolio values make sense.

- For each filing, compute `sum(shares × adj_close_on_report_date)` across all holdings
- Compare against the filing's reported `total_value`
- Flag significant discrepancies (>10%) for review
- Common causes: options positions (reported at notional value), bonds (reported at principal), unresolved CUSIPs

### 1.11 — CLI Interface (`cli/main.py`)

**Framework**: Click + Rich

**Commands**:

```bash
# Full pipeline — does everything in order
fund-analyst pipeline --from-year 2014

# Individual steps
fund-analyst scrape bulk              # Download + parse SEC bulk data sets
fund-analyst scrape incremental       # Fetch recent filings via edgartools
fund-analyst resolve cusips           # Map CUSIPs to tickers via EODHD
fund-analyst download prices          # Pull EOD prices from EODHD
fund-analyst download corporate       # Pull splits, dividends, symbol changes
fund-analyst audit prices             # Run price data quality checks
fund-analyst audit holdings           # Run holdings data quality checks
fund-analyst audit reconcile          # Cross-validate holdings vs prices

# Status and monitoring
fund-analyst status                   # Dashboard of what's been scraped, resolved, etc.
fund-analyst status --detail filings  # Show per-quarter filing counts
fund-analyst status --detail cusips   # Show resolution rates

# Maintenance
fund-analyst reset --confirm          # Wipe DB and rebuild from scratch
fund-analyst resume                   # Resume any interrupted job
```

**Progress display (Rich)**:

```
┌─────────────────────────────────────────────────────────┐
│ 13F Bulk Download                                       │
├─────────────────────────────────────────────────────────┤
│ Quarter    Status      Filings    Holdings    Duration  │
│ 2014-Q1   ✓ done      4,832      1,204,332   0:42      │
│ 2014-Q2   ✓ done      5,021      1,298,441   0:45      │
│ ...                                                     │
│ 2025-Q3   ⟳ running   3,201/6,800 ...        0:22      │
│ 2025-Q4   ◌ pending                                     │
├─────────────────────────────────────────────────────────┤
│ Total: 38/44 quarters │ 5.2M holdings │ ETA: 12 min    │
└─────────────────────────────────────────────────────────┘
```

**Interrupt/resume mechanics**:
- Every scraper checks `scrape_jobs` on start for any `status='running'` or `status='interrupted'` jobs
- On SIGINT (Ctrl-C), a signal handler sets `status='interrupted'` and records current progress
- On resume, the scraper reads the progress JSON and continues from where it left off
- SQLite transactions commit at natural batch boundaries (per-quarter, per-100-tickers, etc.) so no data corruption on interrupt

---

## Phase 2: Analytics Engine + React Frontend

Phase 2 has two parallel tracks: the analytics computations (Python) and the UI (React + FastAPI). The analytics engine computes metrics and stores them in the DB; the frontend reads them.

### 2.1 — Portfolio Return Estimation (`analytics/returns.py`)

- **Quarterly snapshot diffing**: Compare `shares × adj_close` at quarter end vs previous quarter
- **Handle entries/exits**: New positions = assumed bought at quarter-end price (conservative); exited positions = assumed sold at quarter-end price
- **Weight by position size**: Portfolio-weighted return per quarter
- **Options exclusion**: Filter out holdings with `put_call IS NOT NULL` before computing
- **Caveat tracking**: Store confidence/completeness score per quarter (% of holdings with resolved prices)
- **Output**: `fund_quarterly_returns` table with `(cik, report_date, quarterly_return, annualized_return_trailing, confidence_score)`

### 2.2 — Fund Screening Metrics (`analytics/screening.py`)

Precomputed into a `fund_metrics` materialized table, refreshed after each scrape:

- **Annualized return** (CAGR from quarterly returns)
- **Correlation with S&P 500** (Pearson on quarterly returns vs SPY)
- **Track record length** (quarters with filings)
- **Concentration metrics**: HHI, top-5 weight, position count
- **Turnover**: % of portfolio changed per quarter
- **Conviction**: Average position size, new position sizing
- **Max drawdown**: Worst peak-to-trough based on quarterly returns
- **Sharpe ratio**: Using 3-month T-bill as risk-free rate

### 2.3 — Fund Ranking & Filtering Engine (`analytics/ranking.py`)

- SQL-based screener: `WHERE annualized_return > 0.15 AND sp500_correlation < 0.5 AND quarters_active >= 40`
- Support composite scoring (weighted rank across multiple criteria)
- Prebuilt "views": Top performers, Contrarian funds, Concentrated conviction, Longest track records

### 2.4 — API Layer (`api/`)

**Framework**: FastAPI, serving the SQLite DB read-only.

```
api/
├── main.py                  # FastAPI app, CORS config
├── routers/
│   ├── funds.py             # /funds — list, search, filter, rank
│   ├── fund_detail.py       # /funds/{cik} — profile, metrics, filings
│   ├── holdings.py          # /funds/{cik}/holdings/{quarter} — single filing
│   ├── securities.py        # /securities/{cusip} — who owns this stock?
│   ├── prices.py            # /prices/{ticker} — OHLCV time series
│   └── screener.py          # /screener — advanced filtering endpoint
├── models.py                # Pydantic response schemas
└── deps.py                  # DB connection dependency
```

**Key endpoints**:

| Method | Path | Description |
|--------|------|-------------|
| GET | `/funds` | Paginated fund list with sort/filter (returns, correlation, AUM, etc.) |
| GET | `/funds/{cik}` | Fund profile: name, address, filing history, computed metrics |
| GET | `/funds/{cik}/returns` | Quarterly return time series for charting |
| GET | `/funds/{cik}/holdings?quarter=2025Q4` | Holdings table for a specific filing |
| GET | `/funds/{cik}/holdings/history?cusip=037833100` | How a fund's position in one stock evolved over time |
| GET | `/funds/{cik}/compare?vs={cik2}` | Side-by-side metrics comparison |
| GET | `/securities/{cusip}/holders?quarter=2025Q4` | All funds holding this security in a given quarter |
| GET | `/securities/{cusip}/holders/history` | Holder count over time |
| GET | `/prices/{ticker}` | OHLCV time series with `?from=&to=` params |
| GET | `/screener` | Multi-filter fund search with ranking |
| GET | `/stats` | DB summary: total filers, filings, holdings, coverage |

### 2.5 — React Frontend (`frontend/`)

**Stack**: React 18, Vite, TailwindCSS, Recharts (charts), TanStack Table (data grids), React Router.

```
frontend/
├── src/
│   ├── pages/
│   │   ├── Dashboard.tsx          # Landing: summary stats, top movers, recent filings
│   │   ├── FundExplorer.tsx       # The main screener/table page
│   │   ├── FundDetail.tsx         # Single fund deep-dive
│   │   ├── FilingView.tsx         # Single 13F filing — the holdings table
│   │   ├── SecurityView.tsx       # Single stock: who owns it, price chart
│   │   └── Comparison.tsx         # Side-by-side fund comparison
│   ├── components/
│   │   ├── FundTable.tsx          # Sortable, filterable fund data grid
│   │   ├── FilterPanel.tsx        # Sidebar filters for screener
│   │   ├── ReturnChart.tsx        # Quarterly return line/bar chart
│   │   ├── HoldingsTable.tsx      # Holdings grid with value, shares, weight
│   │   ├── PriceChart.tsx         # OHLCV candlestick or line chart
│   │   ├── MetricCard.tsx         # Single stat display (CAGR, Sharpe, etc.)
│   │   ├── CorrelationBadge.tsx   # Visual indicator for S&P correlation
│   │   └── QuarterPicker.tsx      # Quarter selector dropdown
│   ├── hooks/
│   │   ├── useFunds.ts           # React Query hook for fund list
│   │   ├── useFundDetail.ts      # Hook for single fund data
│   │   └── usePrices.ts          # Hook for price time series
│   └── lib/
│       ├── api.ts                # Axios client pointing to FastAPI
│       └── formatters.ts         # Currency, percentage, date formatting
```

#### Page: Fund Explorer (the main screen)

This is the core of the app — a screener that lets you find interesting funds.

**Layout**: Filter sidebar (left) + results table (center) + optional detail panel (right on click).

**Filter panel** — sliders and dropdowns for:
- Annualized return range (e.g. >15%)
- S&P 500 correlation range (e.g. <0.3)
- Track record length (e.g. >40 quarters / 10 years)
- AUM range (total portfolio value)
- Concentration (top-5 weight, HHI)
- Turnover range
- Max drawdown ceiling
- Position count range
- Sector focus (if we can classify from top holdings)

**Results table** — sortable columns:
- Fund name, CIK
- Latest AUM
- Annualized return (1yr, 3yr, 5yr, 10yr, all-time)
- S&P correlation
- Sharpe ratio
- Max drawdown
- Quarters active
- Top 5 holdings (abbreviated)
- Turnover
- Last filing date

Click a row → navigates to Fund Detail page (or opens a slide-over panel).

#### Page: Fund Detail

**Header**: Fund name, location, CIK, link to SEC EDGAR, AUM, filing count.

**Metrics row**: Cards showing CAGR, Sharpe, correlation, max drawdown, HHI, turnover — each with sparkline showing trend over time.

**Tab: Returns**
- Line chart: fund quarterly returns vs S&P 500, overlaid
- Cumulative return chart (growth of $1)
- Drawdown chart (rolling peak-to-trough)
- Toggle: 1yr / 3yr / 5yr / 10yr / All

**Tab: Holdings**
- Quarter picker dropdown at the top
- Holdings table for the selected quarter: ticker, name, value, shares, % weight, put/call flag
- Quarter-over-quarter comparison mode: select two quarters, see adds/drops/changes
- Holdings evolution: stacked area chart showing top positions over time

**Tab: Filings**
- Table of all 13F filings: quarter, filing date, form type (original/amendment), holdings count, total value
- Click a filing → opens Filing View page

**Tab: Top Positions History**
- For each of the fund's historically significant positions, show a row with:
  - Ticker, entry quarter, exit quarter (if applicable), peak weight, current weight
  - Mini price chart inline

#### Page: Filing View (single 13F report)

- Filing metadata: CIK, accession number, filing date, report date, form type
- Link to original SEC filing
- Holdings table: CUSIP, ticker, issuer name, class, value, shares, % weight, put/call, discretion
- Sort by any column
- Compare button → select another quarter for side-by-side diff
- Diff view: highlights new positions (green), exited positions (red), size changes (up/down arrows)

#### Page: Security View

- Header: ticker, company name, CUSIP, exchange
- **Price chart**: Candlestick or line chart with volume, full history, zoomable
  - Overlay markers for quarter-end dates (when 13F snapshots are taken)
- **Holders table**: All funds that held this security as of a selected quarter
  - Columns: fund name, shares, value, % of their portfolio
  - Sort by value or portfolio weight
- **Holder count over time**: Bar chart showing how many 13F filers held this stock each quarter
- **Top holders history**: Line chart showing the top 5 holders' share counts over time

#### Page: Comparison

- Select 2–4 funds by search/typeahead
- Side-by-side metric cards
- Overlaid return charts (cumulative growth of $1)
- Holdings overlap analysis: Venn-style or table showing shared positions
- Correlation matrix between selected funds

#### Page: Dashboard (landing)

- Summary stats: total filers in DB, total filings, unique securities, date range
- "Top 10 by returns this quarter" quick table
- "Biggest new positions this quarter" (aggregate across all filers)
- "Most widely held stocks" leaderboard
- Data freshness indicator: last scrape date, last price update

### 2.6 — Development & Deployment

**Dev workflow**:
```bash
# Terminal 1: FastAPI backend
cd api && uvicorn main:app --reload --port 8000

# Terminal 2: React frontend
cd frontend && npm run dev     # Vite dev server on :5173, proxies /api to :8000
```

**Production (local)**:
```bash
# Build frontend → static files
cd frontend && npm run build

# FastAPI serves both API and static frontend
uvicorn api.main:app --port 8000
# Frontend served from api/static/ via StaticFiles mount
```

No Docker needed for local use. Just `pip install`, `npm install`, and go.

---

## Phase 3: Extensions

### 3.1 — Insider Trading (Form 4)
- EODHD has an **Insider Transactions API** (`/api/insider-transactions/{TICKER}`)
- `edgartools` can parse Form 4 filings directly
- Cross-reference: "Is the CEO buying while the 13F funds are selling?"
- New frontend tab on Security View: insider buy/sell timeline overlaid on price chart

### 3.2 — Congressional Trading
- Source: Senate/House financial disclosures (structured data available from several open-source projects)
- Map disclosed trades to the same securities universe
- New frontend page: Congressional Trades explorer

### 3.3 — Pre-2014 13F Filings
- Plain text parsing with heuristic column detection (the academic paper from elsaifym.github.io has an R-based approach we can port to Python)
- Extends track record to 1999 for the deepest historical analysis

### 3.4 — Technical Patterns & Macro
- Integrate EODHD's Technical Analysis API for moving averages, RSI, etc.
- Macro indicators from EODHD's Macroeconomic Data API
- Overlay on price charts: "this fund bought when RSI was below 30"

---

## Phase 1 Execution Order

This is the order you'd actually run things:

| Step | Command | Duration Estimate | Dependencies |
|------|---------|-------------------|--------------|
| 1 | `fund-analyst scrape bulk` | 15–30 min (download + parse) | None |
| 2 | `fund-analyst scrape incremental` | 30–60 min (2026 filings) | Step 1 |
| 3 | `fund-analyst resolve cusips` | 30–60 min (~10K API calls) | Step 1 |
| 4 | `fund-analyst download corporate` | 15–30 min | Step 3 |
| 5 | `fund-analyst download prices` | 2–4 hours (~10K tickers) | Step 3 |
| 6 | `fund-analyst audit holdings` | 5 min (local compute) | Steps 1–2 |
| 7 | `fund-analyst audit prices` | 5 min (local compute) | Steps 4–5 |
| 8 | `fund-analyst audit reconcile` | 10 min (local compute) | Steps 1–7 |

**Total initial load**: ~4–6 hours, mostly waiting on EODHD API calls.
**Incremental updates**: ~15 min/quarter (bulk download + resolve new CUSIPs + fetch new prices).

### API Call Budget (Initial Full Load)

| Step | Calls | Notes |
|------|-------|-------|
| CUSIP resolution | ~10,000 | 1 call per unique CUSIP via ID Mapping |
| Search fallback | ~1,000 | For CUSIPs that ID Mapping can't resolve |
| Price download | ~10,000 | 1 call per resolved ticker (full history) |
| Splits download | ~10,000 | 1 call per ticker |
| Dividends download | ~10,000 | 1 call per ticker |
| Benchmark (SPY) | 1 | Single call |
| Exchange symbol list | 1 | Single call for symbol change history |
| **Total** | **~41,000** | **Well within 100K/day limit** |

If you want to be conservative, spread across 2 days: CUSIP resolution + prices on day 1, splits + dividends on day 2.

---

## Open Questions to Resolve Before Coding

1. ~~**EODHD plan level**~~ → **RESOLVED**: EOD Historical Data plan. Covers all needed endpoints including ID Mapping.

2. ~~**Handling options in 13F**~~ → **RESOLVED**: Track separately, exclude from return calculations. Filter on `put_call IS NOT NULL`.

3. **Amendment strategy**: When a fund files a restatement (13F-HR/A), should we always prefer the latest amendment, or keep both and let the user choose? *(Recommendation: prefer latest, store original as audit trail)*

4. **Delisted securities**: Some CUSIPs in historical filings map to companies that were subsequently acquired or delisted. Do you want to track these through to their terminal event (merger price, delisting price), or just mark them as unresolvable?

5. ~~**DB in git vs rebuild script**~~ → **RESOLVED**: `.gitignore` DB + ship rebuild script.
