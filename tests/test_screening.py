"""Tests for fund screening metrics with synthetic data."""

import math
import statistics

import pytest

from db.database import get_connection, init_db
from analytics.screening import (
    compute_concentration_metrics,
    compute_fund_metrics,
    compute_turnover,
    _compute_max_drawdown,
)


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / "test.db"


@pytest.fixture
def db(db_path):
    init_db(db_path)
    conn = get_connection(db_path)
    yield conn
    conn.close()


# --- Helpers (same pattern as test_returns.py) ---

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


def _setup_fund_five_quarters(conn):
    """Set up a fund with 5 quarters of known data for comprehensive metric testing.

    Fund holds AAPL and MSFT with controlled prices to produce known returns.
    Also sets up SPY benchmark prices on the same dates.

    Quarter returns (pre-computed from prices):
      Q1→Q2: AAPL 100→110 (+10%), MSFT 200→210 (+5%)
        Portfolio: equal value 100k each → return = (10k+5k)/200k = 7.5%
      Q2→Q3: AAPL 110→105 (-4.545%), MSFT 210→200 (-4.762%)
        Portfolio: prev AAPL=110k, MSFT=105k → return = (-5k + -5k)/215k = -4.651%
      Q3→Q4: AAPL 105→120 (+14.286%), MSFT 200→230 (+15%)
        Portfolio: prev AAPL=105k, MSFT=100k → return = (15k + 15k)/205k = 14.634%
      Q4→Q5: AAPL 120→125 (+4.167%), MSFT 230→240 (+4.348%)
        Portfolio: prev AAPL=120k, MSFT=115k → return = (5k + 5k)/235k = 4.255%
    """
    _insert_security(conn, "CUSIP_AAPL", "AAPL", "APPLE INC")
    _insert_security(conn, "CUSIP_MSFT", "MSFT", "MICROSOFT")

    dates_prices = {
        "2024-03-31": {"AAPL": 100.0, "MSFT": 200.0},
        "2024-06-30": {"AAPL": 110.0, "MSFT": 210.0},
        "2024-09-30": {"AAPL": 105.0, "MSFT": 200.0},
        "2024-12-31": {"AAPL": 120.0, "MSFT": 230.0},
        "2025-03-31": {"AAPL": 125.0, "MSFT": 240.0},
    }
    for date, prices in dates_prices.items():
        for ticker, price in prices.items():
            _insert_price(conn, ticker, date, price)

    # SPY benchmark prices for correlation
    spy_prices = {
        "2024-03-31": 500.0,
        "2024-06-30": 540.0,
        "2024-09-30": 520.0,
        "2024-12-31": 570.0,
        "2025-03-31": 590.0,
    }
    for date, price in spy_prices.items():
        _insert_benchmark(conn, "SPY", date, price)

    _insert_filer(conn, "100", "Fund Alpha")
    quarters = [
        ("2024-03-31", 2024, 1),
        ("2024-06-30", 2024, 2),
        ("2024-09-30", 2024, 3),
        ("2024-12-31", 2024, 4),
        ("2025-03-31", 2025, 1),
    ]
    for i, (date, year, q) in enumerate(quarters):
        fid = _insert_filing(conn, "100", f"A-{i+1}", date, year, q)
        # 1000 shares AAPL, 500 shares MSFT every quarter
        aapl_val = 1000 * dates_prices[date]["AAPL"]
        msft_val = 500 * dates_prices[date]["MSFT"]
        _insert_holding(conn, fid, "CUSIP_AAPL", "APPLE INC", aapl_val, 1000)
        _insert_holding(conn, fid, "CUSIP_MSFT", "MICROSOFT", msft_val, 500)

    conn.commit()


class TestComputeMaxDrawdown:
    def test_no_drawdown(self):
        """Monotonically increasing series has 0 drawdown."""
        assert _compute_max_drawdown([1.0, 1.1, 1.2, 1.3]) == 0.0

    def test_known_drawdown(self):
        """Growth then drop: 1.0 → 1.2 → 0.96 → 1.1
        Peak = 1.2, trough = 0.96, drawdown = (0.96 - 1.2) / 1.2 = -0.2
        """
        result = _compute_max_drawdown([1.0, 1.2, 0.96, 1.1])
        assert result == pytest.approx(-0.2, abs=1e-6)

    def test_two_drawdowns_picks_worst(self):
        """Two drawdowns, picks the deeper one.
        1.0 → 1.5 → 1.2 → 1.8 → 1.0
        DD1: (1.2-1.5)/1.5 = -0.2
        DD2: (1.0-1.8)/1.8 = -0.4444
        """
        result = _compute_max_drawdown([1.0, 1.5, 1.2, 1.8, 1.0])
        assert result == pytest.approx(-4 / 9, abs=1e-6)

    def test_empty_list(self):
        assert _compute_max_drawdown([]) == 0.0

    def test_single_value(self):
        assert _compute_max_drawdown([1.0]) == 0.0

    def test_total_loss(self):
        """Goes to near zero."""
        result = _compute_max_drawdown([1.0, 0.5, 0.1])
        assert result == pytest.approx(-0.9, abs=1e-6)


class TestConcentrationMetrics:
    def test_equal_weight_portfolio(self, db):
        """n equal positions → HHI = 1/n."""
        n = 10
        for i in range(n):
            _insert_security(db, f"CUSIP_{i}", f"TICK{i}")
        _insert_filer(db, "100", "Equal Fund")
        fid = _insert_filing(db, "100", "EQ-1", "2024-03-31", 2024, 1)
        for i in range(n):
            _insert_price(db, f"TICK{i}", "2024-03-31", 100.0)
            _insert_holding(db, fid, f"CUSIP_{i}", f"ISSUER {i}", 10000, 100)
        db.commit()

        result = compute_concentration_metrics(db, "100")
        assert result["hhi"] == pytest.approx(1.0 / n, abs=1e-6)
        assert result["position_count"] == n
        # All equal, top 5 = 5/10 = 0.5
        assert result["top5_concentration"] == pytest.approx(0.5, abs=1e-6)

    def test_concentrated_portfolio(self, db):
        """One large position dominates."""
        _insert_security(db, "CUSIP_BIG", "BIG")
        _insert_security(db, "CUSIP_SMALL", "SMALL")
        _insert_price(db, "BIG", "2024-03-31", 100.0)
        _insert_price(db, "SMALL", "2024-03-31", 10.0)
        _insert_filer(db, "200", "Concentrated Fund")
        fid = _insert_filing(db, "200", "C-1", "2024-03-31", 2024, 1)
        # BIG: 90k, SMALL: 10k → total 100k
        _insert_holding(db, fid, "CUSIP_BIG", "BIG CORP", 90000, 900)
        _insert_holding(db, fid, "CUSIP_SMALL", "SMALL CORP", 10000, 1000)
        db.commit()

        result = compute_concentration_metrics(db, "200")
        # HHI = 0.9^2 + 0.1^2 = 0.81 + 0.01 = 0.82
        assert result["hhi"] == pytest.approx(0.82, abs=1e-6)
        assert result["top5_concentration"] == pytest.approx(1.0, abs=1e-6)
        assert result["position_count"] == 2

    def test_specific_report_date(self, db):
        """Can query a specific quarter, not just latest."""
        _insert_security(db, "CUSIP_A", "A")
        _insert_price(db, "A", "2024-03-31", 100.0)
        _insert_price(db, "A", "2024-06-30", 100.0)
        _insert_filer(db, "300", "Multi-Q Fund")

        fid1 = _insert_filing(db, "300", "MQ-1", "2024-03-31", 2024, 1)
        _insert_holding(db, fid1, "CUSIP_A", "A CORP", 100000, 1000)

        _insert_security(db, "CUSIP_B", "B")
        _insert_price(db, "B", "2024-06-30", 50.0)
        fid2 = _insert_filing(db, "300", "MQ-2", "2024-06-30", 2024, 2)
        _insert_holding(db, fid2, "CUSIP_A", "A CORP", 100000, 1000)
        _insert_holding(db, fid2, "CUSIP_B", "B CORP", 100000, 2000)
        db.commit()

        # Q1: single position → HHI = 1.0
        result_q1 = compute_concentration_metrics(db, "300", "2024-03-31")
        assert result_q1["hhi"] == pytest.approx(1.0, abs=1e-6)
        assert result_q1["position_count"] == 1

        # Q2: two equal positions → HHI = 0.5
        result_q2 = compute_concentration_metrics(db, "300", "2024-06-30")
        assert result_q2["hhi"] == pytest.approx(0.5, abs=1e-6)
        assert result_q2["position_count"] == 2

    def test_no_holdings_returns_empty(self, db):
        result = compute_concentration_metrics(db, "999")
        assert result == {}

    def test_options_excluded(self, db):
        """Options should not be included in concentration metrics."""
        _insert_security(db, "CUSIP_X", "X")
        _insert_price(db, "X", "2024-03-31", 100.0)
        _insert_filer(db, "400", "Opt Fund")
        fid = _insert_filing(db, "400", "O-1", "2024-03-31", 2024, 1)
        _insert_holding(db, fid, "CUSIP_X", "X CORP", 100000, 1000)
        _insert_holding(db, fid, "CUSIP_X", "X CORP PUT", 20000, 200, put_call="PUT")
        db.commit()

        result = compute_concentration_metrics(db, "400")
        assert result["position_count"] == 1
        assert result["hhi"] == pytest.approx(1.0, abs=1e-6)


class TestTurnover:
    def test_no_turnover(self, db):
        """Same positions every quarter → 0 turnover."""
        _insert_security(db, "CUSIP_A", "A")
        _insert_price(db, "A", "2024-03-31", 100.0)
        _insert_price(db, "A", "2024-06-30", 110.0)
        _insert_filer(db, "100", "Steady Fund")

        fid1 = _insert_filing(db, "100", "S-1", "2024-03-31", 2024, 1)
        _insert_holding(db, fid1, "CUSIP_A", "A CORP", 100000, 1000)
        fid2 = _insert_filing(db, "100", "S-2", "2024-06-30", 2024, 2)
        _insert_holding(db, fid2, "CUSIP_A", "A CORP", 110000, 1000)
        db.commit()

        result = compute_turnover(db, "100")
        assert result == pytest.approx(0.0, abs=1e-6)

    def test_full_turnover(self, db):
        """Complete position replacement each quarter."""
        _insert_security(db, "CUSIP_A", "A")
        _insert_security(db, "CUSIP_B", "B")
        _insert_price(db, "A", "2024-03-31", 100.0)
        _insert_price(db, "B", "2024-06-30", 100.0)
        _insert_filer(db, "200", "Active Fund")

        fid1 = _insert_filing(db, "200", "ACT-1", "2024-03-31", 2024, 1)
        _insert_holding(db, fid1, "CUSIP_A", "A CORP", 100000, 1000)
        fid2 = _insert_filing(db, "200", "ACT-2", "2024-06-30", 2024, 2)
        _insert_holding(db, fid2, "CUSIP_B", "B CORP", 100000, 1000)
        db.commit()

        result = compute_turnover(db, "200")
        # new_value=100k (B), exited_value=100k (A), total_curr=100k
        # turnover = |100k + 100k| / 100k = 2.0
        assert result == pytest.approx(2.0, abs=1e-6)

    def test_single_quarter_returns_none(self, db):
        """Need at least 2 quarters for turnover."""
        _insert_security(db, "CUSIP_A", "A")
        _insert_price(db, "A", "2024-03-31", 100.0)
        _insert_filer(db, "300", "One-Q Fund")
        fid = _insert_filing(db, "300", "OQ-1", "2024-03-31", 2024, 1)
        _insert_holding(db, fid, "CUSIP_A", "A CORP", 100000, 1000)
        db.commit()

        assert compute_turnover(db, "300") is None

    def test_nonexistent_cik(self, db):
        assert compute_turnover(db, "999") is None

    def test_partial_turnover(self, db):
        """Add one new position while keeping one."""
        _insert_security(db, "CUSIP_A", "A")
        _insert_security(db, "CUSIP_B", "B")
        _insert_price(db, "A", "2024-03-31", 100.0)
        _insert_price(db, "A", "2024-06-30", 100.0)
        _insert_price(db, "B", "2024-06-30", 100.0)
        _insert_filer(db, "400", "Partial Fund")

        fid1 = _insert_filing(db, "400", "P-1", "2024-03-31", 2024, 1)
        _insert_holding(db, fid1, "CUSIP_A", "A CORP", 100000, 1000)

        fid2 = _insert_filing(db, "400", "P-2", "2024-06-30", 2024, 2)
        _insert_holding(db, fid2, "CUSIP_A", "A CORP", 100000, 1000)
        _insert_holding(db, fid2, "CUSIP_B", "B CORP", 50000, 500)
        db.commit()

        result = compute_turnover(db, "400")
        # new_value=50k (B), exited_value=0, total_curr=150k
        # turnover = |50k + 0| / 150k = 0.3333
        assert result == pytest.approx(50000 / 150000, abs=1e-6)


class TestComputeFundMetrics:
    def test_cagr_known_returns(self, db):
        """Verify CAGR with 4 quarters of constant 2.5% return."""
        _insert_security(db, "CUSIP_A", "A")
        _insert_filer(db, "100", "Steady Growth")

        # Set up prices so each quarter gives exactly +2.5%
        # Use a single position for simplicity
        prices = [100.0, 102.5, 105.0625, 107.689, 110.381]
        dates = [
            ("2024-03-31", 2024, 1),
            ("2024-06-30", 2024, 2),
            ("2024-09-30", 2024, 3),
            ("2024-12-31", 2024, 4),
            ("2025-03-31", 2025, 1),
        ]
        for (date, _, _), price in zip(dates, prices):
            _insert_price(db, "A", date, price)
            _insert_benchmark(db, "SPY", date, price)  # same as fund for simplicity

        for i, (date, year, q) in enumerate(dates):
            fid = _insert_filing(db, "100", f"SG-{i+1}", date, year, q)
            val = 1000 * prices[i]
            _insert_holding(db, fid, "CUSIP_A", "A CORP", val, 1000)
        db.commit()

        metrics = compute_fund_metrics(db, "100")

        # 4 quarters of ~2.5% → CAGR ≈ (1.025)^4 - 1 ≈ 10.38%
        # But computed returns use (curr_computed - prev_computed)/prev_computed
        # which should approximate 2.5% each quarter
        assert metrics["annualized_return"] is not None
        assert metrics["annualized_return"] == pytest.approx(
            (1.025 ** 4) - 1, abs=0.005
        )

    def test_sharpe_ratio(self, db):
        """Verify Sharpe with known returns."""
        _setup_fund_five_quarters(db)
        metrics = compute_fund_metrics(db, "100")

        # Get quarterly returns to verify Sharpe manually
        from analytics.returns import compute_quarterly_returns
        qr = compute_quarterly_returns(db, "100")
        returns_list = [r["quarterly_return"] for r in qr]

        mean_r = statistics.mean(returns_list)
        std_r = statistics.stdev(returns_list)
        expected_sharpe = (mean_r - 0.01) / std_r * (4 ** 0.5)

        assert metrics["sharpe_ratio"] == pytest.approx(expected_sharpe, abs=1e-6)

    def test_sp500_correlation_perfectly_correlated(self, db):
        """Fund returns identical to SPY → correlation ≈ 1.0."""
        _insert_security(db, "CUSIP_SPY", "SPY_FUND")
        _insert_filer(db, "100", "Index Fund")

        # Use SPY-like prices for both fund and benchmark
        spy_prices = {
            "2024-03-31": 500.0,
            "2024-06-30": 520.0,
            "2024-09-30": 510.0,
            "2024-12-31": 550.0,
            "2025-03-31": 570.0,
        }
        for date, price in spy_prices.items():
            _insert_price(db, "SPY_FUND", date, price)
            _insert_benchmark(db, "SPY", date, price)

        dates = [
            ("2024-03-31", 2024, 1),
            ("2024-06-30", 2024, 2),
            ("2024-09-30", 2024, 3),
            ("2024-12-31", 2024, 4),
            ("2025-03-31", 2025, 1),
        ]
        for i, (date, year, q) in enumerate(dates):
            fid = _insert_filing(db, "100", f"IDX-{i+1}", date, year, q)
            val = 1000 * spy_prices[date]
            _insert_holding(db, fid, "CUSIP_SPY", "SPY FUND", val, 1000)
        db.commit()

        metrics = compute_fund_metrics(db, "100")
        assert metrics["sp500_correlation"] is not None
        assert metrics["sp500_correlation"] == pytest.approx(1.0, abs=1e-6)

    def test_sp500_correlation_uncorrelated(self, db):
        """Fund returns inversely related to SPY → correlation < 0."""
        _insert_security(db, "CUSIP_INV", "INV")
        _insert_filer(db, "100", "Inverse Fund")

        # Fund goes up when market goes down and vice versa
        fund_prices = {
            "2024-03-31": 100.0,
            "2024-06-30": 110.0,  # +10%
            "2024-09-30": 99.0,   # -10%
            "2024-12-31": 108.9,  # +10%
            "2025-03-31": 98.01,  # -10%
        }
        spy_prices = {
            "2024-03-31": 500.0,
            "2024-06-30": 450.0,  # -10%
            "2024-09-30": 495.0,  # +10%
            "2024-12-31": 445.5,  # -10%
            "2025-03-31": 490.05, # +10%
        }
        for date in fund_prices:
            _insert_price(db, "INV", date, fund_prices[date])
            _insert_benchmark(db, "SPY", date, spy_prices[date])

        dates = [
            ("2024-03-31", 2024, 1),
            ("2024-06-30", 2024, 2),
            ("2024-09-30", 2024, 3),
            ("2024-12-31", 2024, 4),
            ("2025-03-31", 2025, 1),
        ]
        for i, (date, year, q) in enumerate(dates):
            fid = _insert_filing(db, "100", f"INV-{i+1}", date, year, q)
            val = 1000 * fund_prices[date]
            _insert_holding(db, fid, "CUSIP_INV", "INV CORP", val, 1000)
        db.commit()

        metrics = compute_fund_metrics(db, "100")
        assert metrics["sp500_correlation"] is not None
        assert metrics["sp500_correlation"] == pytest.approx(-1.0, abs=1e-6)

    def test_max_drawdown(self, db):
        """Fund with growth then decline → measurable drawdown."""
        _setup_fund_five_quarters(db)
        metrics = compute_fund_metrics(db, "100")

        assert metrics["max_drawdown"] is not None
        assert metrics["max_drawdown"] < 0  # Drawdown is negative

    def test_hhi_and_concentration(self, db):
        """HHI and top-5 should come from latest quarter."""
        _setup_fund_five_quarters(db)
        metrics = compute_fund_metrics(db, "100")

        assert metrics["hhi"] is not None
        assert metrics["top5_concentration"] is not None
        # Fund has 2 positions, so top5 = 1.0
        assert metrics["top5_concentration"] == pytest.approx(1.0, abs=1e-6)

    def test_turnover(self, db):
        """Same positions every quarter → low/zero turnover."""
        _setup_fund_five_quarters(db)
        metrics = compute_fund_metrics(db, "100")

        assert metrics["avg_turnover"] is not None
        assert metrics["avg_turnover"] == pytest.approx(0.0, abs=1e-6)

    def test_quarters_active(self, db):
        """quarters_active counts the number of quarterly returns computed."""
        _setup_fund_five_quarters(db)
        metrics = compute_fund_metrics(db, "100")

        # 5 quarters → 4 quarterly returns
        assert metrics["quarters_active"] == 4

    def test_latest_aum(self, db):
        """latest_aum should be the total_value from the last quarter."""
        _setup_fund_five_quarters(db)
        metrics = compute_fund_metrics(db, "100")

        # Last quarter: AAPL 1000*125=125k, MSFT 500*240=120k → total 245k
        assert metrics["latest_aum"] is not None
        assert metrics["latest_aum"] > 0

    def test_avg_confidence(self, db):
        """All positions have prices → avg_confidence = 1.0."""
        _setup_fund_five_quarters(db)
        metrics = compute_fund_metrics(db, "100")

        assert metrics["avg_confidence"] == pytest.approx(1.0, abs=1e-6)

    def test_insufficient_data_returns_none(self, db):
        """< 4 quarters → CAGR, Sharpe, correlation, max_drawdown = None."""
        _insert_security(db, "CUSIP_A", "A")
        _insert_filer(db, "500", "Short Fund")

        # Only 3 quarters → 2 returns < MIN_QUARTERS_FOR_METRICS
        for i, (date, year, q) in enumerate([
            ("2024-03-31", 2024, 1),
            ("2024-06-30", 2024, 2),
            ("2024-09-30", 2024, 3),
        ]):
            _insert_price(db, "A", date, 100.0 + i * 5)
            fid = _insert_filing(db, "500", f"SF-{i+1}", date, year, q)
            _insert_holding(db, fid, "CUSIP_A", "A CORP", (100 + i * 5) * 1000, 1000)
        db.commit()

        metrics = compute_fund_metrics(db, "500")

        assert metrics["annualized_return"] is None
        assert metrics["sharpe_ratio"] is None
        assert metrics["sp500_correlation"] is None
        assert metrics["max_drawdown"] is None
        # These should still be computed
        assert metrics["quarters_active"] == 2
        assert metrics["hhi"] is not None
        assert metrics["avg_turnover"] is not None

    def test_all_expected_keys_present(self, db):
        """compute_fund_metrics should return all 10 expected keys."""
        _setup_fund_five_quarters(db)
        metrics = compute_fund_metrics(db, "100")

        expected_keys = {
            "annualized_return",
            "sharpe_ratio",
            "sp500_correlation",
            "max_drawdown",
            "hhi",
            "top5_concentration",
            "avg_turnover",
            "quarters_active",
            "latest_aum",
            "avg_confidence",
        }
        assert set(metrics.keys()) == expected_keys

    def test_nonexistent_cik(self, db):
        """Nonexistent CIK should return metrics dict with zeros/Nones."""
        metrics = compute_fund_metrics(db, "999999")
        assert metrics["quarters_active"] == 0
        assert metrics["annualized_return"] is None
        assert metrics["sharpe_ratio"] is None

    def test_no_benchmark_data(self, db):
        """Without SPY data, correlation should be None."""
        _insert_security(db, "CUSIP_A", "A")
        _insert_filer(db, "600", "No Bench Fund")

        dates = [
            ("2024-03-31", 2024, 1),
            ("2024-06-30", 2024, 2),
            ("2024-09-30", 2024, 3),
            ("2024-12-31", 2024, 4),
            ("2025-03-31", 2025, 1),
        ]
        for i, (date, year, q) in enumerate(dates):
            price = 100.0 + i * 5
            _insert_price(db, "A", date, price)
            fid = _insert_filing(db, "600", f"NB-{i+1}", date, year, q)
            _insert_holding(db, fid, "CUSIP_A", "A CORP", price * 1000, 1000)
        db.commit()

        metrics = compute_fund_metrics(db, "600")
        # Enough quarters for CAGR/Sharpe but no benchmark → correlation None
        assert metrics["annualized_return"] is not None
        assert metrics["sharpe_ratio"] is not None
        assert metrics["sp500_correlation"] is None
