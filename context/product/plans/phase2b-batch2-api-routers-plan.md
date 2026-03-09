# Phase 2B Batch 2: Remaining API Routers

## Context

Phase 2B Batch 1 completed the FastAPI backend skeleton with funds endpoints and metrics cache (commit `7b83f9f`). This batch adds the remaining 4 API routers: securities, prices, holdings history, and screener.

## Files to Create

| File | Purpose |
|------|---------|
| `api/routers/securities.py` | Security lookup, holder tracking, search |
| `api/routers/prices.py` | Price time series + benchmark comparison |
| `api/routers/holdings.py` | Position history tracking |
| `api/routers/screener.py` | Advanced screening + presets |
| `tests/test_api_securities.py` | Securities endpoint tests |
| `tests/test_api_prices.py` | Prices endpoint tests |
| `tests/test_api_holdings.py` | Holdings history tests |
| `tests/test_api_screener.py` | Screener tests |
| `tests/conftest.py` | Shared test fixtures (extract from test_api_funds.py) |

## Files to Modify

| File | Change |
|------|--------|
| `api/models.py` | Add ~5 new Pydantic models |
| `api/main.py` | Register 4 new routers |
| `api/routers/funds.py` | Extract `_quarter_to_date` to shared location |

## Reuse

- **Patterns**: Follow `api/routers/funds.py` for endpoint structure, error handling, pagination, dependency injection
- **Models**: `SecurityInfo`, `SecurityHolder`, `PricePoint` already exist in `api/models.py`
- **DB helpers**: `query_all`, `query_one`, `rows_to_dicts` from `db/database.py`
- **Views**: `v_holding_values`, `v_portfolio_quarterly` for holdings/securities queries
- **Analytics**: `_PREBUILT_SCREENS` from `analytics/ranking.py` for screener presets
- **Test helpers**: `_insert_filer`, `_insert_filing`, `_insert_security`, `_insert_holding`, `_insert_price` from `test_api_funds.py`

---

## Endpoints

### Securities Router (`/api/securities`)

1. **`GET /api/securities/search?q=AAPL&limit=20`** — Search by ticker or name (define BEFORE `{cusip}` routes)
2. **`GET /api/securities/{cusip}`** — Security info (uses existing `SecurityInfo` model)
3. **`GET /api/securities/{cusip}/holders?quarter=2024Q4`** — All funds holding this CUSIP (uses existing `SecurityHolder` model)
4. **`GET /api/securities/{cusip}/holders/history`** — Holder count + total shares per quarter (new `SecurityHolderHistoryPoint` model)

### Prices Router (`/api/prices`)

5. **`GET /api/prices/{ticker}?start_date=&end_date=`** — OHLCV time series (uses existing `PricePoint` model)
6. **`GET /api/prices/{ticker}/benchmark?start_date=&end_date=&benchmark=SPY`** — Ticker vs benchmark comparison (new `BenchmarkComparison` model)

### Holdings Router (`/api/holdings`)

7. **`GET /api/holdings/position-history?cik=&cusip=`** — Track a fund's position in a security over time (new `PositionHistoryPoint` model)

### Screener Router (`/api/screener`)

8. **`GET /api/screener`** — Advanced multi-filter screening from cache (superset of `/api/funds` filters, adds turnover + top5 filters). Returns `PaginatedResponse[FundSummary]`
9. **`GET /api/screener/presets`** — List preset screen configs (new `ScreenerPreset` model)
10. **`GET /api/screener/presets/{name}`** — Run a preset and return results

---

## New Pydantic Models (add to `api/models.py`)

```python
class SecurityHolderHistoryPoint(BaseModel):
    report_date: str
    quarter: str
    holder_count: int
    total_shares: float
    total_value: float

class BenchmarkComparison(BaseModel):
    date: str
    ticker_close: float | None = None
    ticker_adj_close: float | None = None
    benchmark_close: float | None = None
    benchmark_adj_close: float | None = None

class PositionHistoryPoint(BaseModel):
    report_date: str
    quarter: str
    shares: float | None = None
    value: float | None = None
    weight: float | None = None
    price: float | None = None

class ScreenerPreset(BaseModel):
    name: str
    description: str
    filters: dict
    sort_by: str
```

---

## Key SQL Queries

**Security holders:**
```sql
SELECT f.cik, fl.name, h.shares, h.value
FROM holdings h
JOIN filings f ON h.filing_id = f.id
JOIN filers fl ON f.cik = fl.cik
WHERE h.cusip = ? AND f.report_date = ?
  AND (f.amendment_type IS NULL OR f.amendment_type = 'RESTATEMENT')
ORDER BY h.value DESC
```

**Holder history:**
```sql
SELECT f.report_date,
       f.report_year || 'Q' || f.report_quarter AS quarter,
       COUNT(DISTINCT f.cik) AS holder_count,
       SUM(h.shares) AS total_shares,
       SUM(h.value) AS total_value
FROM holdings h JOIN filings f ON h.filing_id = f.id
WHERE h.cusip = ?
  AND (f.amendment_type IS NULL OR f.amendment_type = 'RESTATEMENT')
GROUP BY f.report_date ORDER BY f.report_date
```

**Position history (via views):**
```sql
SELECT v.report_date,
       v.report_year || 'Q' || v.report_quarter AS quarter,
       v.shares, v.reported_value AS value, v.price_on_date AS price,
       v.reported_value * 1.0 / pq.total_reported_value AS weight
FROM v_holding_values v
JOIN v_portfolio_quarterly pq ON v.cik = pq.cik AND v.report_date = pq.report_date
WHERE v.cik = ? AND v.cusip = ?
ORDER BY v.report_date
```

**Price + benchmark comparison:**
```sql
SELECT p.date, p.close AS ticker_close, p.adj_close AS ticker_adj_close,
       b.close AS benchmark_close, b.adj_close AS benchmark_adj_close
FROM prices p
LEFT JOIN benchmark_prices b ON p.date = b.date AND b.ticker = ?
WHERE p.ticker = ?
  AND (? IS NULL OR p.date >= ?) AND (? IS NULL OR p.date <= ?)
ORDER BY p.date
```

---

## Execution Order

| Step | What | Test |
|------|------|------|
| 1 | Extract shared test fixtures to `tests/conftest.py` | Run existing tests pass |
| 2 | Extract `_quarter_to_date` → top of `funds.py` or `api/utils.py` | Existing tests pass |
| 3 | Add new Pydantic models to `api/models.py` | — |
| 4 | `api/routers/prices.py` + `tests/test_api_prices.py` | `pytest tests/test_api_prices.py -v` |
| 5 | `api/routers/securities.py` + `tests/test_api_securities.py` | `pytest tests/test_api_securities.py -v` |
| 6 | `api/routers/holdings.py` + `tests/test_api_holdings.py` | `pytest tests/test_api_holdings.py -v` |
| 7 | `api/routers/screener.py` + `tests/test_api_screener.py` | `pytest tests/test_api_screener.py -v` |
| 8 | Register all routers in `api/main.py` | `pytest` (full suite) |

Steps 4-7 are independent and can be parallelized via subagents.

## Verification

1. `pytest` — all existing 209 tests + ~25-30 new tests pass
2. Manual curl against real DB:
   ```
   curl localhost:8000/api/securities/search?q=AAPL
   curl localhost:8000/api/securities/037833100
   curl localhost:8000/api/securities/037833100/holders?quarter=2024Q4
   curl localhost:8000/api/securities/037833100/holders/history
   curl "localhost:8000/api/prices/AAPL?start_date=2024-01-01"
   curl "localhost:8000/api/prices/AAPL/benchmark"
   curl "localhost:8000/api/holdings/position-history?cik=1067983&cusip=037833100"
   curl "localhost:8000/api/screener?min_return=0.1&min_quarters=20"
   curl localhost:8000/api/screener/presets
   ```
3. OpenAPI docs at `localhost:8000/docs` show all new endpoints
