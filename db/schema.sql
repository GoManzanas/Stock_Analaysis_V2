-- 13F Fund Analyst Database Schema
-- SQLite with WAL mode

CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS filers (
    cik TEXT PRIMARY KEY,
    name TEXT,
    address TEXT,
    first_report_date TEXT,
    last_report_date TEXT,
    filing_count INTEGER DEFAULT 0,
    total_value_latest REAL
);

CREATE TABLE IF NOT EXISTS filings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cik TEXT NOT NULL REFERENCES filers(cik),
    accession_number TEXT UNIQUE NOT NULL,
    filing_date TEXT,
    report_date TEXT,
    report_year INTEGER,
    report_quarter INTEGER,
    form_type TEXT,
    amendment_type TEXT,
    total_value REAL,
    holding_count INTEGER,
    source TEXT,
    scraped_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS holdings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    filing_id INTEGER NOT NULL REFERENCES filings(id),
    cusip TEXT NOT NULL,
    issuer_name TEXT,
    class_title TEXT,
    value REAL,
    shares REAL,
    sh_prn_type TEXT,
    put_call TEXT,
    investment_discretion TEXT,
    voting_sole INTEGER,
    voting_shared INTEGER,
    voting_none INTEGER
);

CREATE TABLE IF NOT EXISTS securities (
    cusip TEXT PRIMARY KEY,
    ticker TEXT,
    eodhd_symbol TEXT,
    name TEXT,
    security_type TEXT,
    exchange TEXT,
    is_active INTEGER DEFAULT 1,
    resolved_at TEXT,
    resolution_source TEXT,
    resolution_confidence REAL DEFAULT 0.0
);

CREATE TABLE IF NOT EXISTS corporate_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    action_type TEXT NOT NULL,
    effective_date TEXT NOT NULL,
    details TEXT,
    source TEXT
);

CREATE TABLE IF NOT EXISTS prices (
    ticker TEXT NOT NULL,
    date TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    adj_close REAL,
    volume INTEGER,
    PRIMARY KEY (ticker, date)
);

CREATE TABLE IF NOT EXISTS benchmark_prices (
    ticker TEXT NOT NULL,
    date TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    adj_close REAL,
    volume INTEGER,
    PRIMARY KEY (ticker, date)
);

CREATE TABLE IF NOT EXISTS scrape_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_type TEXT NOT NULL,
    target TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    progress TEXT,
    error_message TEXT,
    started_at TEXT,
    completed_at TEXT,
    resumed_count INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS audit_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    audit_type TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT,
    finding TEXT NOT NULL,
    severity TEXT NOT NULL,
    auto_fixed INTEGER DEFAULT 0,
    details TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_holdings_cusip ON holdings(cusip);
CREATE INDEX IF NOT EXISTS idx_holdings_filing_id ON holdings(filing_id);
CREATE INDEX IF NOT EXISTS idx_filings_cik_report ON filings(cik, report_date);
CREATE INDEX IF NOT EXISTS idx_filings_quarter ON filings(report_year, report_quarter);
CREATE INDEX IF NOT EXISTS idx_prices_date ON prices(date);
CREATE INDEX IF NOT EXISTS idx_securities_ticker ON securities(ticker);
CREATE INDEX IF NOT EXISTS idx_corporate_actions_ticker ON corporate_actions(ticker);
CREATE INDEX IF NOT EXISTS idx_scrape_jobs_type_target ON scrape_jobs(job_type, target);
CREATE INDEX IF NOT EXISTS idx_audit_results_type ON audit_results(audit_type);
