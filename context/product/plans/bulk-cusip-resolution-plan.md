# Plan: Bulk CUSIP Resolution via Exchange Symbol List

## Context

The current CUSIP resolution (pipeline step 2) makes 152K+ individual API calls to EODHD's `/api/id-mapping` endpoint — one per unresolved CUSIP. At 5 calls/sec, this takes ~8.5 hours. The fix: download EODHD's full exchange symbol list (1 API call), extract CUSIPs from ISINs, and match locally in SQLite.

## New Pipeline Order

```
Step 1/6: SEC Bulk Download              (unchanged)
Step 2/6: CUSIP Resolution               (rewritten: bulk symbol list + local match)
Step 3/6: Price Download + Price Audit    (price audit runs as sub-step after download)
Step 4/6: Corporate Actions              (unchanged, renumbered)
Step 5/6: Holdings Audit                 (unchanged, renumbered)
Step 6/6: Reconciliation                 (unchanged, renumbered)
```

## Approach

### Phase 1: Download exchange symbol list
- Call `GET /api/exchange-symbol-list/US?api_token={KEY}&fmt=json` (active tickers)
- Call again with `&delisted=1` (delisted tickers — critical for historical 13F CUSIPs from 2014+)
- Store in new `exchange_symbols` table
- Extract 9-digit CUSIP from ISIN: `ISIN[2:11]` for US ISINs (format: `US` + 9-digit CUSIP + check digit)
- Pre-compute `cusip9` column at insert time for efficient JOIN

### Phase 2: Bulk CUSIP matching via SQL
- Single `UPDATE ... FROM` joining `securities` against `exchange_symbols` on `cusip = cusip9`
- Handle multiple tickers per CUSIP via `ROW_NUMBER()` tie-breaking (prefer US exchange, prefer Common Stock)
- Set `resolution_source = 'bulk_symbol_list'`, `resolution_confidence = 0.95`

### Price audit integration
- After `PriceScraper.run()` completes successfully, call `run_price_audit(conn)` inline
- Standalone `audit prices` CLI command still works independently

## Files to Change

### `db/schema.sql` — Add `exchange_symbols` table
```sql
CREATE TABLE IF NOT EXISTS exchange_symbols (
    code TEXT NOT NULL,
    name TEXT,
    country TEXT,
    exchange TEXT NOT NULL,
    currency TEXT,
    isin TEXT,
    cusip9 TEXT,
    type TEXT,
    is_delisted INTEGER DEFAULT 0,
    downloaded_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (code, exchange)
);
CREATE INDEX IF NOT EXISTS idx_exchange_symbols_cusip9 ON exchange_symbols(cusip9);
```

### `config/settings.py` — Add constant
```python
EODHD_SYMBOL_EXCHANGES = ["US"]  # Future: add "OTC"
```

### `scrapers/eodhd_mapping.py` — Major rewrite
- Add `download_exchange_symbols(exchange)` — single API call
- Add `extract_cusip_from_isin(isin)` — `ISIN[2:11]` for US ISINs
- Rewrite `CusipResolver.run()` into 2 phases with separate job targets:
  - `symbol_download:US` — Phase 1 (download)
  - `bulk_match` — Phase 2 (SQL JOIN)
- Remove `resolve_cusip_via_mapping()` from the main code path (keep function for backward compat)

### `scrapers/eodhd_prices.py` — Add price audit sub-step
- After successful completion, call `run_price_audit(self.conn)`

### `cli/main.py` — Reorder pipeline
- 7 steps → 6 steps (price audit folded into price download)
- Update step numbering and labels
- Add `exchange_symbols` count to status dashboard
- Update `resolve` command docstring

### `tests/test_eodhd_mapping.py` — Update tests
- Add tests for `extract_cusip_from_isin()`
- Add tests for bulk match phase (insert symbols, run resolver, verify match)
- Update integration test for 2-phase flow

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Store symbol list in DB vs memory | DB table | Avoids re-download, enables SQL JOIN, audit trail |
| Include delisted tickers | Yes, with `delisted=1` | 13F data from 2014+ references many delisted securities |
| Resolution source name | `bulk_symbol_list` | Distinct from old `eodhd_mapping` for clarity |
| Confidence for bulk match | 0.95 | High but below 1.0 (ISIN check digit not validated) |
| Job tracking per phase | Separate targets | Allows resume at any phase boundary |

## Verification

1. Run `python -m cli.main resolve` and confirm:
   - Phase 1 downloads ~70K+ symbols (active + delisted) in 2 API calls
   - Phase 2 bulk-matches thousands of CUSIPs in seconds via SQL
2. Run `python -m cli.main download prices` and confirm price audit runs after download
3. Run `python -m cli.main pipeline` and confirm 6-step flow works end-to-end
4. Run `python -m cli.main status --detail cusips` and confirm `bulk_symbol_list` appears as a resolution source
5. Run `pytest` and confirm all tests pass
