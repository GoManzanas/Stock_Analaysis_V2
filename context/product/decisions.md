# Decisions

Product and architectural decisions. Log non-obvious choices here.

<!-- Format:
## YYYY-MM-DD - [Decision Title]

**Context:** Why this came up
**Decision:** What we decided
**Alternatives considered:** What else we looked at
**Consequences:** What this means going forward
-->

## 2026-03-07 - Bulk CUSIP resolution via Exchange Symbol List

**Context:** CUSIP resolution made 152K+ individual API calls (1 per CUSIP) to EODHD's ID Mapping API, taking ~8.5 hours at 5 calls/sec.

**Decision:** Replace per-CUSIP API calls with EODHD Exchange Symbol List API (2 calls: active + delisted tickers), extract CUSIPs from ISINs, match locally via SQL JOIN. Unmatched CUSIPs remain unresolved (no name-search fallback).

**Alternatives considered:**
- Keep per-CUSIP approach with higher parallelism (still 152K calls, rate-limited)
- Pre-build a static CUSIP→ticker mapping file (stale, no delisted coverage)

**Consequences:**
- CUSIP resolution drops from ~8.5 hours to seconds (2 API calls + SQL JOIN)
- Pipeline reduced from 7 steps to 6 (price audit folded into price download)
- New `exchange_symbols` table stores the full symbol list for auditing/debugging
- `resolution_source = 'bulk_symbol_list'` at confidence 0.95 (vs 1.0 for old mapping)
- Future: can add OTC exchange for better coverage of OTC-traded securities
