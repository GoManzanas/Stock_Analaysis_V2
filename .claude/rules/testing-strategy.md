---
description: When writing tests or creating test files
---

# Testing Strategy

## Unit Tests (pytest)

### The Boundary Rule

- Mock at the **external API boundary** — EODHD HTTP calls, SEC downloads
- Mock at the **filesystem boundary** — ZIP file reads, file downloads
- Use real SQLite databases (in-memory or `tmp_path`) for DB tests

### Pattern

1. Use `tmp_path` fixture for test databases
2. Use `unittest.mock.patch` or `responses` library for HTTP mocking
3. Create synthetic test data (small TSV files, mock API responses)

### What to Test

- Database schema creation and CRUD operations
- Scrape job lifecycle (create, progress, complete, fail, resume)
- SEC TSV parsing logic (value cutover, amendments, idempotency)
- CUSIP resolution logic (6-digit vs 9-digit, confidence scoring)
- Audit detection logic (anomaly detection, threshold checks)
- CLI command invocation

### What NOT to Test

- SQLite internals
- Third-party library behavior (requests, click, rich)
- Static configuration/constants
