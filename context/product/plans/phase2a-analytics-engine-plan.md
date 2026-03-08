# Phase 2A: Analytics Engine — Implementation Plan

## Context

Phase 1 (data pipeline) is complete: SEC 13F bulk scraper, CUSIP resolver, price downloader, corporate actions, and audit pipeline are all implemented and tested. The database contains filers, filings, holdings, securities, prices, benchmark_prices, corporate_actions, and audit_results tables.

**Goal**: Build an analytics engine that computes portfolio returns, screening metrics, and fund ranking/filtering — all on top of the existing SQLite database. This enables answering questions like "which funds beat the S&P 500 with low correlation over 10+ years?"

**User decisions**:
- SQL views for data freshness (no materialized tables)
- Quarter-end snapshot diffing for return estimation (simple, conservative)
- Hardcoded risk-free rate (4% annualized / ~1% quarterly) for Sharpe ratio
- Both CLI commands and Python module API

---

## Files to Create

```
analytics/
├── __init__.py           # Package init
├── returns.py            # Quarterly return computation
├── screening.py          # Fund-level metric computations
├── ranking.py            # Multi-filter screener + composite scoring
db/
└── views.sql             # SQL view definitions (applied during init_db)
tests/
├── test_returns.py       # Return computation tests
├── test_screening.py     # Screening metric tests
└── test_ranking.py       # Ranking/filtering tests
```

## Files to Modify

| File | Change |
|------|--------|
| `db/database.py` | Load `views.sql` alongside `schema.sql` in `init_db()` |
| `db/schema.sql` | Add index on `holdings(filing_id, cusip)` for join performance |
| `cli/main.py` | Add `analytics` command group with subcommands |
| `context/product/FUND_IMPLEMENTATION_PLAN.md` | Check in the master implementation plan |

## Existing Utilities to Reuse

- `db/database.py`: `get_connection()`, `init_db()`, `query_all()`, `query_one()`, `rows_to_dicts()`
- `config/settings.py`: `DB_PATH`
- `cli/main.py`: `console` (Rich Console), Click group pattern

---

## Chunk 1: SQL Views + DB Changes

### `db/views.sql`

**View: `v_holding_values`** — Joins holdings to their resolved prices on the filing's report_date.

```sql
CREATE VIEW IF NOT EXISTS v_holding_values AS
SELECT
    f.cik,
    f.report_date,
    f.report_year,
    f.report_quarter,
    f.id AS filing_id,
    h.cusip,
    h.issuer_name,
    h.value AS reported_value,
    h.shares,
    h.put_call,
    s.ticker,
    p.adj_close AS price_on_date,
    CASE WHEN s.ticker IS NOT NULL AND p.adj_close IS NOT NULL
         THEN h.shares * p.adj_close
         ELSE NULL
    END AS computed_value,
    CASE WHEN h.put_call IS NOT NULL THEN 1 ELSE 0 END AS is_option
FROM holdings h
JOIN filings f ON h.filing_id = f.id
LEFT JOIN securities s ON h.cusip = s.cusip
LEFT JOIN prices p ON s.ticker = p.ticker AND f.report_date = p.date
WHERE f.amendment_type IS NULL OR f.amendment_type = 'RESTATEMENT';
```

Note: The LEFT JOIN on prices uses `report_date = p.date` — if no exact match (weekend/holiday), the Python layer will find the nearest trading day.

**View: `v_portfolio_quarterly`** — Aggregates per fund per quarter (excluding options).

```sql
CREATE VIEW IF NOT EXISTS v_portfolio_quarterly AS
SELECT
    cik,
    report_date,
    report_year,
    report_quarter,
    filing_id,
    COUNT(*) AS position_count,
    SUM(reported_value) AS total_reported_value,
    SUM(CASE WHEN is_option = 0 AND computed_value IS NOT NULL THEN computed_value ELSE 0 END) AS total_computed_value,
    SUM(CASE WHEN is_option = 0 THEN reported_value ELSE 0 END) AS total_equity_reported,
    SUM(CASE WHEN is_option = 0 AND computed_value IS NOT NULL THEN 1 ELSE 0 END) AS priced_positions,
    SUM(CASE WHEN is_option = 0 THEN 1 ELSE 0 END) AS equity_positions,
    CASE WHEN SUM(CASE WHEN is_option = 0 THEN 1 ELSE 0 END) > 0
         THEN CAST(SUM(CASE WHEN is_option = 0 AND computed_value IS NOT NULL THEN 1 ELSE 0 END) AS REAL)
              / SUM(CASE WHEN is_option = 0 THEN 1 ELSE 0 END)
         ELSE 0
    END AS price_coverage
FROM v_holding_values
GROUP BY cik, report_date;
```

**View: `v_benchmark_quarterly`** — SPY quarterly returns for correlation.

```sql
CREATE VIEW IF NOT EXISTS v_benchmark_quarterly AS
SELECT
    date AS report_date,
    adj_close,
    LAG(adj_close) OVER (ORDER BY date) AS prev_adj_close,
    CASE WHEN LAG(adj_close) OVER (ORDER BY date) IS NOT NULL
         THEN (adj_close - LAG(adj_close) OVER (ORDER BY date))
              / LAG(adj_close) OVER (ORDER BY date)
         ELSE NULL
    END AS quarterly_return
FROM benchmark_prices
WHERE ticker = 'SPY'
  AND date IN (SELECT DISTINCT report_date FROM filings)
ORDER BY date;
```

### DB changes

- `db/database.py`: In `init_db()`, after executing `schema.sql`, also execute `views.sql`
- `db/schema.sql`: Add composite index `idx_holdings_filing_cusip ON holdings(filing_id, cusip)` for join performance

---

## Chunk 2: Returns Module (`analytics/returns.py`)

### Core function: `compute_quarterly_returns(conn, cik) -> list[dict]`

**Algorithm** (quarter-end snapshot diffing):

1. Query `v_holding_values` for the given CIK, all quarters, non-option holdings only
2. Group by quarter → build position snapshots: `{cusip: {shares, computed_value}}`
3. For each consecutive quarter pair (Q_prev, Q_curr):
   - **Continuing positions**: CUSIPs in both quarters → return = `(value_curr - value_prev) / value_prev` per position
   - **New positions** (in Q_curr but not Q_prev): contribute 0% return (assumed bought at quarter-end)
   - **Exited positions** (in Q_prev but not Q_curr): contribute 0% return (assumed sold at prev quarter-end)
   - **Portfolio return** = weighted average of position returns, weighted by prev-quarter value (or curr-quarter value for new positions)
4. Compute **confidence score** = `priced_positions / equity_positions` from `v_portfolio_quarterly`

**Return format**:
```python
[{
    "cik": "1234567",
    "report_date": "2024-12-31",
    "quarterly_return": 0.042,        # 4.2%
    "confidence": 0.95,               # 95% of holdings had prices
    "position_count": 150,
    "total_value": 5_000_000_000,
}, ...]
```

### Helper: `get_nearest_price(conn, ticker, date) -> float | None`

For when report_date falls on a weekend/holiday — find the nearest prior trading day's adj_close (look back up to 5 business days).

### Helper: `compute_cumulative_returns(quarterly_returns) -> list[dict]`

Compound quarterly returns into cumulative growth-of-$1 series.

---

## Chunk 3: Screening Module (`analytics/screening.py`)

### Function: `compute_fund_metrics(conn, cik) -> dict`

Calls `compute_quarterly_returns()` internally, then computes:

| Metric | Computation | Key |
|--------|-------------|-----|
| CAGR | `(product(1 + r_q))^(4/n) - 1` where n = number of quarters | `annualized_return` |
| Sharpe | `(mean(r_q) - 0.01) / stdev(r_q) * sqrt(4)` | `sharpe_ratio` |
| S&P Correlation | Pearson r between fund quarterly returns and SPY quarterly returns | `sp500_correlation` |
| Max Drawdown | Worst peak-to-trough from cumulative return series | `max_drawdown` |
| HHI | `sum(weight_i^2)` where weight = position value / total value, latest quarter | `hhi` |
| Top-5 Weight | Sum of 5 largest position weights, latest quarter | `top5_concentration` |
| Turnover | Average quarterly `|new + exited value| / total value` | `avg_turnover` |
| Track Record | Count of quarters with filings | `quarters_active` |
| Latest AUM | Total reported value from most recent filing | `latest_aum` |
| Avg Confidence | Mean price coverage across quarters | `avg_confidence` |

**Implementation notes**:
- Use `statistics.stdev()` and `statistics.mean()` from stdlib
- Pearson correlation: `statistics.correlation()` (Python 3.10+, we require 3.12+)
- Min 4 quarters required for CAGR/Sharpe/correlation; return `None` if insufficient data
- HHI and concentration computed from latest quarter's holdings only

### Function: `compute_concentration_metrics(conn, cik, report_date=None) -> dict`

Computes HHI, top-5 weight, and position count for a specific quarter (defaults to latest).

### Function: `compute_turnover(conn, cik) -> float`

Average quarterly turnover across all quarter pairs.

---

## Chunk 4: Ranking Module (`analytics/ranking.py`)

### Function: `screen_funds(conn, filters: dict, sort_by: str, limit: int) -> list[dict]`

**Algorithm**:
1. Get all CIKs with sufficient data (configurable min quarters, default 4)
2. For each CIK, compute metrics via `compute_fund_metrics()`
3. Apply filters (min/max thresholds for each metric)
4. Sort by requested metric
5. Return top N results

**Supported filters** (all optional):
```python
filters = {
    "min_annualized_return": 0.15,
    "max_annualized_return": None,
    "min_sp500_correlation": None,
    "max_sp500_correlation": 0.5,
    "min_quarters_active": 20,
    "min_latest_aum": 1_000_000,
    "max_max_drawdown": -0.3,
    "min_sharpe_ratio": 0.5,
    "min_avg_confidence": 0.8,
}
```

**Performance note**: Computing metrics for all ~6,000 filers will be slow (minutes). Optimization strategies:
- Pre-filter by `quarters_active` via a quick SQL query before computing full metrics
- Only compute metrics for filers that pass cheap filters first (AUM, quarters active)
- Consider caching results in a future phase if this becomes a bottleneck

### Function: `prebuilt_screen(conn, name: str) -> list[dict]`

Named screens with preset filters:
- `"top_performers"`: CAGR > 15%, min 20 quarters, min 80% confidence
- `"contrarian"`: S&P correlation < 0.3, min 20 quarters
- `"concentrated"`: HHI > 0.1 (top positions dominate), min 10 quarters
- `"long_track_record"`: min 40 quarters (10+ years), any return

---

## Chunk 5: CLI Commands

Add to `cli/main.py`:

```python
@cli.group()
def analytics():
    """Analyze fund performance and screen for interesting funds."""
    pass

@analytics.command("returns")
@click.argument("cik")
@click.option("--cumulative", is_flag=True, help="Show cumulative growth-of-$1")
def analytics_returns(cik, cumulative):
    """Show quarterly returns for a fund."""

@analytics.command("metrics")
@click.argument("cik")
def analytics_metrics(cik):
    """Show all screening metrics for a fund."""

@analytics.command("screen")
@click.option("--min-return", type=float, help="Minimum annualized return")
@click.option("--max-correlation", type=float, help="Maximum S&P 500 correlation")
@click.option("--min-quarters", type=int, default=4, help="Minimum quarters active")
@click.option("--min-aum", type=float, help="Minimum latest AUM in dollars")
@click.option("--min-sharpe", type=float, help="Minimum Sharpe ratio")
@click.option("--max-drawdown", type=float, help="Maximum drawdown (e.g., -0.3)")
@click.option("--min-confidence", type=float, default=0.8, help="Minimum avg price confidence")
@click.option("--sort-by", default="annualized_return", help="Sort metric")
@click.option("--limit", default=25, help="Number of results")
def analytics_screen(**kwargs):
    """Screen funds by metrics. Shows a ranked table."""

@analytics.command("top")
@click.option("--view", type=click.Choice(["top_performers", "contrarian", "concentrated", "long_track_record"]),
              default="top_performers")
@click.option("--limit", default=25)
def analytics_top(view, limit):
    """Show prebuilt fund screens."""
```

Rich table output for all commands, similar to existing `status` command pattern.

---

## Chunk 6: Tests

### `tests/test_returns.py`
- Test with synthetic data: 2 funds, 4 quarters each, known prices
- Verify quarterly return calculation matches hand-computed values
- Test new position handling (entry quarter = 0% return)
- Test exited position handling
- Test confidence score computation
- Test nearest-price fallback for weekend report dates
- Test cumulative return computation

### `tests/test_screening.py`
- Test CAGR computation with known quarterly returns
- Test Sharpe ratio with known returns and risk-free rate
- Test correlation with perfectly correlated / uncorrelated returns
- Test max drawdown with known drawdown scenario
- Test HHI with equal-weight and concentrated portfolios
- Test turnover calculation
- Test insufficient data handling (< 4 quarters)

### `tests/test_ranking.py`
- Test filter application (min/max thresholds)
- Test sorting by different metrics
- Test prebuilt screens return expected format
- Test limit parameter

---

## Execution Order

| Step | What | Dependencies |
|------|------|--------------|
| 1 | Create `db/views.sql` + update `db/database.py` to load views | None |
| 2 | Add index to `db/schema.sql` | None |
| 3 | Create `analytics/__init__.py` + `analytics/returns.py` | Step 1 |
| 4 | Write `tests/test_returns.py` + verify | Step 3 |
| 5 | Create `analytics/screening.py` | Step 3 |
| 6 | Write `tests/test_screening.py` + verify | Step 5 |
| 7 | Create `analytics/ranking.py` | Step 5 |
| 8 | Write `tests/test_ranking.py` + verify | Step 7 |
| 9 | Add CLI commands to `cli/main.py` | Steps 3, 5, 7 |
| 10 | Copy `FUND_IMPLEMENTATION_PLAN.md` into repo | None |

Steps 1-2 can be done in parallel. Steps 3-4, 5-6, 7-8 are sequential pairs. Step 9 depends on all analytics modules. Step 10 is independent.

---

## Verification

1. **Run all existing tests**: `pytest` — ensure no regressions
2. **Run new tests**: `pytest tests/test_returns.py tests/test_screening.py tests/test_ranking.py -v`
3. **Manual CLI test** (requires populated DB):
   ```bash
   python -m cli.main analytics returns 1067983       # Berkshire Hathaway
   python -m cli.main analytics metrics 1067983
   python -m cli.main analytics screen --min-quarters 20 --limit 10
   python -m cli.main analytics top --view top_performers
   ```
4. **Edge cases to verify**:
   - Fund with only 1 quarter of data (should return metrics as None)
   - Fund with all unresolved CUSIPs (confidence = 0, returns = None)
   - Report date on a weekend (nearest-price fallback works)

---

## Constants

Add to `config/settings.py`:
```python
# --- Analytics ---
RISK_FREE_RATE_QUARTERLY = 0.01  # ~4% annualized
MIN_QUARTERS_FOR_METRICS = 4     # Minimum quarters to compute CAGR, Sharpe, etc.
```
