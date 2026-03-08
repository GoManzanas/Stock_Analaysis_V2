"""Tests for fund ranking and screening with synthetic data."""

import pytest

from db.database import get_connection, init_db
from analytics.ranking import screen_funds, prebuilt_screen, _passes_filters


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / "test.db"


@pytest.fixture
def db(db_path):
    init_db(db_path)
    conn = get_connection(db_path)
    yield conn
    conn.close()


# --- Helpers ---

def _insert_filer(conn, cik, name="Test Fund"):
    conn.execute("INSERT INTO filers (cik, name) VALUES (?, ?)", (cik, name))


def _insert_filing(conn, cik, accession, report_date, year, quarter):
    conn.execute(
        """INSERT INTO filings (cik, accession_number, report_date, report_year,
           report_quarter, form_type, total_value, holding_count, source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (cik, accession, report_date, year, quarter, "13F-HR", 0, 0, "bulk"),
    )
    return conn.execute(
        "SELECT id FROM filings WHERE accession_number = ?", (accession,)
    ).fetchone()[0]


def _insert_security(conn, cusip, ticker, name="TEST"):
    conn.execute(
        "INSERT OR IGNORE INTO securities (cusip, ticker, name, exchange, resolution_confidence) "
        "VALUES (?, ?, ?, ?, ?)",
        (cusip, ticker, name, "US", 1.0),
    )


def _insert_holding(conn, filing_id, cusip, issuer, value, shares, put_call=None):
    conn.execute(
        """INSERT INTO holdings (filing_id, cusip, issuer_name, value, shares, sh_prn_type, put_call)
        VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (filing_id, cusip, issuer, value, shares, "SH", put_call),
    )


def _insert_price(conn, ticker, date, adj_close):
    conn.execute(
        "INSERT OR IGNORE INTO prices (ticker, date, close, adj_close, volume) VALUES (?, ?, ?, ?, ?)",
        (ticker, date, adj_close, adj_close, 1000000),
    )


def _insert_benchmark(conn, ticker, date, adj_close):
    conn.execute(
        "INSERT OR IGNORE INTO benchmark_prices (ticker, date, close, adj_close, volume) VALUES (?, ?, ?, ?, ?)",
        (ticker, date, adj_close, adj_close, 1000000),
    )


QUARTERS_5 = [
    ("2024-03-31", 2024, 1),
    ("2024-06-30", 2024, 2),
    ("2024-09-30", 2024, 3),
    ("2024-12-31", 2024, 4),
    ("2025-03-31", 2025, 1),
]

SPY_PRICES = {
    "2024-03-31": 500.0,
    "2024-06-30": 540.0,
    "2024-09-30": 520.0,
    "2024-12-31": 570.0,
    "2025-03-31": 590.0,
}


def _setup_benchmark(conn):
    """Insert SPY benchmark prices."""
    for date, price in SPY_PRICES.items():
        _insert_benchmark(conn, "SPY", date, price)


def _setup_fund(conn, cik, name, cusip, ticker, prices_by_date, shares=1000):
    """Create a fund with 5 quarters of data using given prices.

    Returns nothing; commits are left to the caller.
    """
    _insert_filer(conn, cik, name)
    _insert_security(conn, cusip, ticker, name)

    for date, price in prices_by_date.items():
        _insert_price(conn, ticker, date, price)

    for i, (date, year, q) in enumerate(QUARTERS_5):
        if date not in prices_by_date:
            continue
        fid = _insert_filing(conn, cik, f"{cik}-{i+1}", date, year, q)
        val = shares * prices_by_date[date]
        _insert_holding(conn, fid, cusip, name, val, shares)


def _setup_three_funds(conn):
    """Create 3 funds with different performance profiles.

    Fund A (CIK 100): Strong performer — steady growth
    Fund B (CIK 200): Weak performer — declining
    Fund C (CIK 300): Moderate performer — volatile
    """
    _setup_benchmark(conn)

    # Fund A: strong growth (100 → 110 → 120 → 135 → 150)
    _setup_fund(conn, "100", "Alpha Fund", "CUSIP_A", "ALPHA", {
        "2024-03-31": 100.0,
        "2024-06-30": 110.0,
        "2024-09-30": 120.0,
        "2024-12-31": 135.0,
        "2025-03-31": 150.0,
    })

    # Fund B: declining (100 → 95 → 90 → 85 → 80)
    _setup_fund(conn, "200", "Beta Fund", "CUSIP_B", "BETA", {
        "2024-03-31": 100.0,
        "2024-06-30": 95.0,
        "2024-09-30": 90.0,
        "2024-12-31": 85.0,
        "2025-03-31": 80.0,
    })

    # Fund C: volatile moderate (100 → 120 → 95 → 115 → 105)
    _setup_fund(conn, "300", "Gamma Fund", "CUSIP_C", "GAMMA", {
        "2024-03-31": 100.0,
        "2024-06-30": 120.0,
        "2024-09-30": 95.0,
        "2024-12-31": 115.0,
        "2025-03-31": 105.0,
    })

    conn.commit()


class TestPassesFilters:
    """Unit tests for the _passes_filters helper."""

    def test_no_filters(self):
        metrics = {"annualized_return": 0.10, "quarters_active": 4}
        assert _passes_filters(metrics, {}) is True

    def test_min_filter_passes(self):
        metrics = {"annualized_return": 0.20, "quarters_active": 10}
        assert _passes_filters(metrics, {"min_annualized_return": 0.15}) is True

    def test_min_filter_fails(self):
        metrics = {"annualized_return": 0.10, "quarters_active": 10}
        assert _passes_filters(metrics, {"min_annualized_return": 0.15}) is False

    def test_max_filter_passes(self):
        metrics = {"sp500_correlation": 0.2, "quarters_active": 10}
        assert _passes_filters(metrics, {"max_sp500_correlation": 0.5}) is True

    def test_max_filter_fails(self):
        metrics = {"sp500_correlation": 0.8, "quarters_active": 10}
        assert _passes_filters(metrics, {"max_sp500_correlation": 0.5}) is False

    def test_none_metric_with_filter_excludes(self):
        metrics = {"annualized_return": None, "quarters_active": 10}
        assert _passes_filters(metrics, {"min_annualized_return": 0.10}) is False

    def test_none_threshold_ignored(self):
        metrics = {"annualized_return": 0.10}
        assert _passes_filters(metrics, {"min_annualized_return": None}) is True

    def test_unknown_filter_ignored(self):
        metrics = {"annualized_return": 0.10}
        assert _passes_filters(metrics, {"unknown_filter": 0.5}) is True

    def test_max_drawdown_filter(self):
        """max_max_drawdown: -0.3 means drawdown must be <= -0.3 (i.e., -0.2 passes, -0.4 fails)."""
        metrics_mild = {"max_drawdown": -0.15}
        metrics_severe = {"max_drawdown": -0.45}
        # -0.15 <= -0.1 is False, but the filter is max_max_drawdown
        # max_max_drawdown = -0.3 means value must be <= -0.3
        # -0.15 <= -0.3 is False → excluded (less severe drawdown than threshold)
        # Wait — the semantics: "max_max_drawdown": -0.3 means we want funds
        # where max_drawdown <= -0.3 (deeper drawdown is more negative).
        # Actually re-reading the spec: max filter means value <= threshold.
        # -0.15 <= -0.3 → False (mild drawdown excluded by deep-drawdown filter)
        # -0.45 <= -0.3 → True (severe drawdown passes deep-drawdown filter)
        # This makes "max_max_drawdown" a filter for "only very negative drawdowns."
        # That's a bit odd, but let's just follow the spec as stated.
        assert _passes_filters(metrics_mild, {"max_max_drawdown": -0.3}) is False
        assert _passes_filters(metrics_severe, {"max_max_drawdown": -0.3}) is True


class TestScreenFunds:
    def test_basic_screening(self, db):
        """screen_funds returns funds matching filters, sorted correctly."""
        _setup_three_funds(db)

        results = screen_funds(db, filters={}, sort_by="annualized_return", limit=25)
        assert len(results) == 3
        # All should have cik and name
        for r in results:
            assert "cik" in r
            assert "name" in r
            assert "annualized_return" in r

    def test_filter_excludes_funds(self, db):
        """Filtering by min_annualized_return excludes negative-return funds."""
        _setup_three_funds(db)

        results = screen_funds(
            db,
            filters={"min_annualized_return": 0.0},
            sort_by="annualized_return",
        )
        # Fund B has negative returns, should be excluded
        ciks = [r["cik"] for r in results]
        assert "200" not in ciks
        # Fund A should be included (strong positive)
        assert "100" in ciks

    def test_sort_descending(self, db):
        """Default sort is descending (highest first)."""
        _setup_three_funds(db)

        results = screen_funds(
            db, filters={}, sort_by="annualized_return", sort_ascending=False
        )
        returns = [r["annualized_return"] for r in results]
        # All should be non-None since we have 4 quarters
        assert all(r is not None for r in returns)
        # Should be sorted descending
        for i in range(len(returns) - 1):
            assert returns[i] >= returns[i + 1]

    def test_sort_ascending(self, db):
        """Ascending sort for correlation-like metrics."""
        _setup_three_funds(db)

        results = screen_funds(
            db, filters={}, sort_by="quarters_active", sort_ascending=True
        )
        values = [r["quarters_active"] for r in results]
        for i in range(len(values) - 1):
            assert values[i] <= values[i + 1]

    def test_limit_parameter(self, db):
        """Limit caps the number of results."""
        _setup_three_funds(db)

        results = screen_funds(db, filters={}, sort_by="annualized_return", limit=2)
        assert len(results) == 2

    def test_limit_larger_than_results(self, db):
        """Limit larger than actual results returns all."""
        _setup_three_funds(db)

        results = screen_funds(db, filters={}, sort_by="annualized_return", limit=100)
        assert len(results) == 3

    def test_all_filtered_out(self, db):
        """If all funds fail filters, return empty list."""
        _setup_three_funds(db)

        results = screen_funds(
            db,
            filters={"min_annualized_return": 100.0},  # impossibly high
            sort_by="annualized_return",
        )
        assert results == []

    def test_min_quarters_active_prefilter(self, db):
        """min_quarters_active filters via SQL pre-query."""
        _setup_three_funds(db)

        # All funds have 4 quarters_active (5 filings - 1)
        results = screen_funds(
            db,
            filters={"min_quarters_active": 4},
            sort_by="annualized_return",
        )
        assert len(results) == 3

        # Require more quarters than available
        results = screen_funds(
            db,
            filters={"min_quarters_active": 10},
            sort_by="annualized_return",
        )
        assert len(results) == 0

    def test_min_latest_aum_prefilter(self, db):
        """min_latest_aum filters via SQL pre-query."""
        _setup_three_funds(db)

        # Fund A: 1000 * 150 = 150,000
        # Fund B: 1000 * 80 = 80,000
        # Fund C: 1000 * 105 = 105,000
        results = screen_funds(
            db,
            filters={"min_latest_aum": 120_000},
            sort_by="annualized_return",
        )
        ciks = [r["cik"] for r in results]
        assert "100" in ciks  # 150k passes
        assert "200" not in ciks  # 80k fails
        assert "300" not in ciks  # 105k fails

    def test_none_metric_excluded_by_filter(self, db):
        """Fund with None metric is excluded when that metric has a filter."""
        # Create a fund with only 3 quarters (not enough for CAGR)
        _setup_benchmark(db)
        _insert_filer(db, "400", "Short Fund")
        _insert_security(db, "CUSIP_D", "DELTA", "DELTA CORP")
        for i, (date, year, q) in enumerate(QUARTERS_5[:3]):
            price = 100.0 + i * 5
            _insert_price(db, "DELTA", date, price)
            fid = _insert_filing(db, "400", f"400-{i+1}", date, year, q)
            _insert_holding(db, fid, "CUSIP_D", "DELTA CORP", 1000 * price, 1000)
        conn = db
        conn.commit()

        # This fund has quarters_active=2 < MIN_QUARTERS_FOR_METRICS=4
        # so annualized_return will be None
        results = screen_funds(
            db,
            filters={"min_annualized_return": 0.0},
            sort_by="annualized_return",
        )
        ciks = [r["cik"] for r in results]
        assert "400" not in ciks

    def test_result_format(self, db):
        """Each result dict has all metric keys plus cik and name."""
        _setup_three_funds(db)

        results = screen_funds(db, filters={}, sort_by="annualized_return", limit=1)
        assert len(results) == 1

        result = results[0]
        expected_keys = {
            "annualized_return", "sharpe_ratio", "sp500_correlation",
            "max_drawdown", "hhi", "top5_concentration", "avg_turnover",
            "quarters_active", "latest_aum", "avg_confidence",
            "cik", "name",
        }
        assert set(result.keys()) == expected_keys

    def test_multiple_filters_combined(self, db):
        """Multiple filters are all applied (AND logic)."""
        _setup_three_funds(db)

        results = screen_funds(
            db,
            filters={
                "min_annualized_return": 0.0,
                "min_quarters_active": 4,
            },
            sort_by="annualized_return",
        )
        # Fund B has negative return, should be excluded
        ciks = [r["cik"] for r in results]
        assert "200" not in ciks

    def test_hhi_filter(self, db):
        """min_hhi and max_hhi filters work."""
        _setup_three_funds(db)

        # All funds have single position → HHI = 1.0
        results = screen_funds(
            db,
            filters={"min_hhi": 0.5},
            sort_by="hhi",
        )
        assert len(results) == 3

        results = screen_funds(
            db,
            filters={"max_hhi": 0.5},
            sort_by="hhi",
        )
        assert len(results) == 0  # All HHI = 1.0, none <= 0.5


class TestPrebuiltScreen:
    def test_top_performers(self, db):
        """top_performers screen returns correct format."""
        _setup_three_funds(db)

        results = prebuilt_screen(db, "top_performers")
        # Results should be a list of dicts
        assert isinstance(results, list)
        for r in results:
            assert "cik" in r
            assert "name" in r
            assert "annualized_return" in r

    def test_concentrated_screen(self, db):
        """concentrated screen filters by HHI."""
        _setup_three_funds(db)

        results = prebuilt_screen(db, "concentrated")
        # All funds have HHI=1.0 (single position each), pass min_hhi=0.1
        # But min_quarters_active=10 and our funds only have 4 quarters
        # So all should be filtered out
        assert len(results) == 0

    def test_long_track_record(self, db):
        """long_track_record requires 40 quarters — our test funds have 4."""
        _setup_three_funds(db)

        results = prebuilt_screen(db, "long_track_record")
        assert len(results) == 0

    def test_unknown_screen_raises(self, db):
        with pytest.raises(ValueError, match="Unknown screen"):
            prebuilt_screen(db, "nonexistent")

    def test_limit_parameter(self, db):
        """Prebuilt screen respects limit."""
        _setup_three_funds(db)

        results = prebuilt_screen(db, "top_performers", limit=1)
        assert len(results) <= 1

    def test_contrarian_screen(self, db):
        """contrarian screen filters by low correlation."""
        _setup_three_funds(db)

        results = prebuilt_screen(db, "contrarian")
        # Requires min_quarters_active=20 — our funds have 4
        assert len(results) == 0
