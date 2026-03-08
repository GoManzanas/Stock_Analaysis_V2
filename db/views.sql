-- Analytics views for 13F Fund Analyst
-- These views join holdings, filings, securities, and prices
-- to support portfolio analytics and benchmarking.

-- v_holding_values: Joins holdings to resolved prices on filing report_date
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

-- v_portfolio_quarterly: Aggregates per fund per quarter (excluding options)
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

-- v_benchmark_quarterly: SPY quarterly returns for correlation
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
