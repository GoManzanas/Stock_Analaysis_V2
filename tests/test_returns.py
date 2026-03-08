"""Tests for quarterly return computation with synthetic data."""

import pytest

from db.database import get_connection, init_db
from analytics.returns import (
    compute_cumulative_returns,
    compute_quarterly_returns,
    get_nearest_price,
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


def _setup_two_funds_four_quarters(conn):
    """Set up 2 funds with 4 quarters each and known prices.

    Fund A (CIK 100): Holds AAPL and MSFT across Q1-Q4 2024.
    Fund B (CIK 200): Holds GOOG and TSLA across Q1-Q4 2024.

    Prices are chosen so returns can be hand-computed.
    """
    # Securities
    _insert_security(conn, "CUSIP_AAPL", "AAPL", "APPLE INC")
    _insert_security(conn, "CUSIP_MSFT", "MSFT", "MICROSOFT")
    _insert_security(conn, "CUSIP_GOOG", "GOOG", "ALPHABET")
    _insert_security(conn, "CUSIP_TSLA", "TSLA", "TESLA")

    # Prices on quarter-end dates
    dates_prices = {
        # date: {ticker: adj_close}
        "2024-03-31": {"AAPL": 100.0, "MSFT": 200.0, "GOOG": 150.0, "TSLA": 50.0},
        "2024-06-30": {"AAPL": 110.0, "MSFT": 220.0, "GOOG": 165.0, "TSLA": 45.0},
        "2024-09-30": {"AAPL": 105.0, "MSFT": 210.0, "GOOG": 180.0, "TSLA": 60.0},
        "2024-12-31": {"AAPL": 120.0, "MSFT": 240.0, "GOOG": 170.0, "TSLA": 55.0},
    }
    for date, prices in dates_prices.items():
        for ticker, price in prices.items():
            _insert_price(conn, ticker, date, price)

    # Fund A: CIK 100
    _insert_filer(conn, "100", "Fund Alpha")
    quarters = [
        ("2024-03-31", 2024, 1),
        ("2024-06-30", 2024, 2),
        ("2024-09-30", 2024, 3),
        ("2024-12-31", 2024, 4),
    ]
    for i, (date, year, q) in enumerate(quarters):
        fid = _insert_filing(conn, "100", f"A-{i+1}", date, year, q)
        # 1000 shares AAPL, 500 shares MSFT every quarter
        _insert_holding(conn, fid, "CUSIP_AAPL", "APPLE INC", 1000 * dates_prices[date]["AAPL"], 1000)
        _insert_holding(conn, fid, "CUSIP_MSFT", "MICROSOFT", 500 * dates_prices[date]["MSFT"], 500)

    # Fund B: CIK 200
    _insert_filer(conn, "200", "Fund Beta")
    for i, (date, year, q) in enumerate(quarters):
        fid = _insert_filing(conn, "200", f"B-{i+1}", date, year, q)
        _insert_holding(conn, fid, "CUSIP_GOOG", "ALPHABET", 800 * dates_prices[date]["GOOG"], 800)
        _insert_holding(conn, fid, "CUSIP_TSLA", "TESLA", 2000 * dates_prices[date]["TSLA"], 2000)

    conn.commit()


class TestGetNearestPrice:
    def test_exact_date_match(self, db):
        _insert_price(db, "AAPL", "2024-03-31", 170.0)
        db.commit()

        price = get_nearest_price(db, "AAPL", "2024-03-31")
        assert price == 170.0

    def test_weekend_fallback(self, db):
        """Report date on Saturday should find Friday's price."""
        _insert_price(db, "AAPL", "2024-03-29", 170.0)  # Friday
        db.commit()

        # Saturday
        price = get_nearest_price(db, "AAPL", "2024-03-30")
        assert price == 170.0

    def test_holiday_fallback(self, db):
        """Should look back multiple days for holidays."""
        _insert_price(db, "AAPL", "2024-03-27", 165.0)  # Wednesday
        db.commit()

        # Monday after a 4-day weekend (no Thu/Fri/Sat/Sun prices)
        price = get_nearest_price(db, "AAPL", "2024-04-01")
        assert price == 165.0

    def test_no_price_available(self, db):
        """Should return None when no price exists within 7 days."""
        _insert_price(db, "AAPL", "2024-03-01", 160.0)
        db.commit()

        price = get_nearest_price(db, "AAPL", "2024-03-31")
        assert price is None

    def test_picks_most_recent(self, db):
        """With multiple prices in range, picks the most recent."""
        _insert_price(db, "AAPL", "2024-03-27", 165.0)
        _insert_price(db, "AAPL", "2024-03-28", 168.0)
        _insert_price(db, "AAPL", "2024-03-29", 170.0)
        db.commit()

        price = get_nearest_price(db, "AAPL", "2024-03-30")
        assert price == 170.0


class TestComputeQuarterlyReturns:
    def test_basic_two_funds(self, db):
        """Two funds with 4 quarters each should produce 3 quarterly returns each."""
        _setup_two_funds_four_quarters(db)

        returns_a = compute_quarterly_returns(db, "100")
        assert len(returns_a) == 3

        returns_b = compute_quarterly_returns(db, "200")
        assert len(returns_b) == 3

    def test_hand_computed_returns_fund_a(self, db):
        """Verify Fund A returns match hand computation.

        Fund A holds 1000 AAPL + 500 MSFT every quarter (same shares).

        Q1 values: AAPL=100k, MSFT=100k → total=200k
        Q2 values: AAPL=110k, MSFT=110k → total=220k
        Q2 return: Both positions return 10% → portfolio return = 10%

        Q3 values: AAPL=105k, MSFT=105k → total=210k
        AAPL return: (105k-110k)/110k = -4.545%
        MSFT return: (105k-110k)/110k = -4.545%
        Q3 return: -4.545% (equal weights, same return)

        Q4 values: AAPL=120k, MSFT=120k → total=240k
        AAPL return: (120k-105k)/105k = 14.286%
        MSFT return: (120k-105k)/105k = 14.286%
        Q4 return: 14.286%
        """
        _setup_two_funds_four_quarters(db)
        returns_a = compute_quarterly_returns(db, "100")

        assert returns_a[0]["report_date"] == "2024-06-30"
        assert returns_a[0]["quarterly_return"] == pytest.approx(0.10, abs=1e-6)

        assert returns_a[1]["report_date"] == "2024-09-30"
        assert returns_a[1]["quarterly_return"] == pytest.approx(-0.04545454545, abs=1e-6)

        assert returns_a[2]["report_date"] == "2024-12-31"
        assert returns_a[2]["quarterly_return"] == pytest.approx(0.14285714285, abs=1e-6)

    def test_hand_computed_returns_fund_b(self, db):
        """Verify Fund B returns match hand computation.

        Fund B holds 800 GOOG + 2000 TSLA.

        Q1 values: GOOG=120k, TSLA=100k → total=220k
        Q2 values: GOOG=132k, TSLA=90k → total=222k
        GOOG return: (132k-120k)/120k = 10%
        TSLA return: (90k-100k)/100k = -10%
        Q2 return: (0.10*120k + (-0.10)*100k) / 220k = (12k - 10k) / 220k = 0.009090909
        """
        _setup_two_funds_four_quarters(db)
        returns_b = compute_quarterly_returns(db, "200")

        assert returns_b[0]["report_date"] == "2024-06-30"
        expected_q2 = (0.10 * 120000 + (-0.10) * 100000) / 220000
        assert returns_b[0]["quarterly_return"] == pytest.approx(expected_q2, abs=1e-6)

    def test_return_fields_present(self, db):
        """Each return dict should have all required fields."""
        _setup_two_funds_four_quarters(db)
        returns_a = compute_quarterly_returns(db, "100")

        required_keys = {"cik", "report_date", "quarterly_return", "confidence", "position_count", "total_value"}
        for r in returns_a:
            assert required_keys.issubset(r.keys())
            assert r["cik"] == "100"

    def test_confidence_all_priced(self, db):
        """When all positions have prices, confidence should be 1.0."""
        _setup_two_funds_four_quarters(db)
        returns_a = compute_quarterly_returns(db, "100")

        for r in returns_a:
            assert r["confidence"] == pytest.approx(1.0, abs=1e-6)

    def test_new_position_contributes_zero_return(self, db):
        """A new position entering in Q2 should contribute 0% return.

        Fund C holds only AAPL in Q1, then adds MSFT in Q2.
        Q1: AAPL 1000 shares @ 100 = 100k
        Q2: AAPL 1000 shares @ 110 = 110k, MSFT 500 shares @ 220 = 110k
        AAPL return = 10%, weight = 100k (prev quarter)
        MSFT return = 0%, weight = 110k (curr quarter, new position)
        Portfolio return = (0.10 * 100k + 0 * 110k) / (100k + 110k) = 10k / 210k
        """
        _insert_security(db, "CUSIP_AAPL", "AAPL", "APPLE")
        _insert_security(db, "CUSIP_MSFT", "MSFT", "MICROSOFT")
        _insert_price(db, "AAPL", "2024-03-31", 100.0)
        _insert_price(db, "AAPL", "2024-06-30", 110.0)
        _insert_price(db, "MSFT", "2024-06-30", 220.0)

        _insert_filer(db, "300", "Fund Charlie")
        fid1 = _insert_filing(db, "300", "C-1", "2024-03-31", 2024, 1)
        _insert_holding(db, fid1, "CUSIP_AAPL", "APPLE", 100000, 1000)

        fid2 = _insert_filing(db, "300", "C-2", "2024-06-30", 2024, 2)
        _insert_holding(db, fid2, "CUSIP_AAPL", "APPLE", 110000, 1000)
        _insert_holding(db, fid2, "CUSIP_MSFT", "MICROSOFT", 110000, 500)
        db.commit()

        returns = compute_quarterly_returns(db, "300")
        assert len(returns) == 1

        expected = (0.10 * 100000) / (100000 + 110000)
        assert returns[0]["quarterly_return"] == pytest.approx(expected, abs=1e-6)

    def test_exited_position_contributes_zero_return(self, db):
        """An exited position should contribute 0% return.

        Fund D holds AAPL + MSFT in Q1, only AAPL in Q2.
        Q1: AAPL 1000 @ 100 = 100k, MSFT 500 @ 200 = 100k
        Q2: AAPL 1000 @ 110 = 110k
        AAPL (continuing): return = 10%, weight = 100k
        MSFT (exited): 0% return, no weight contribution
        Portfolio return = (0.10 * 100k) / 100k = 10%
        """
        _insert_security(db, "CUSIP_AAPL", "AAPL", "APPLE")
        _insert_security(db, "CUSIP_MSFT", "MSFT", "MICROSOFT")
        _insert_price(db, "AAPL", "2024-03-31", 100.0)
        _insert_price(db, "MSFT", "2024-03-31", 200.0)
        _insert_price(db, "AAPL", "2024-06-30", 110.0)

        _insert_filer(db, "400", "Fund Delta")
        fid1 = _insert_filing(db, "400", "D-1", "2024-03-31", 2024, 1)
        _insert_holding(db, fid1, "CUSIP_AAPL", "APPLE", 100000, 1000)
        _insert_holding(db, fid1, "CUSIP_MSFT", "MICROSOFT", 100000, 500)

        fid2 = _insert_filing(db, "400", "D-2", "2024-06-30", 2024, 2)
        _insert_holding(db, fid2, "CUSIP_AAPL", "APPLE", 110000, 1000)
        db.commit()

        returns = compute_quarterly_returns(db, "400")
        assert len(returns) == 1
        # Only continuing position (AAPL) contributes
        assert returns[0]["quarterly_return"] == pytest.approx(0.10, abs=1e-6)

    def test_confidence_with_unresolved_cusips(self, db):
        """Holdings with unresolved CUSIPs should reduce confidence."""
        _insert_security(db, "CUSIP_AAPL", "AAPL", "APPLE")
        # CUSIP_UNKN has no security entry → unresolved
        _insert_price(db, "AAPL", "2024-03-31", 100.0)
        _insert_price(db, "AAPL", "2024-06-30", 110.0)

        _insert_filer(db, "500", "Fund Echo")
        fid1 = _insert_filing(db, "500", "E-1", "2024-03-31", 2024, 1)
        _insert_holding(db, fid1, "CUSIP_AAPL", "APPLE", 100000, 1000)
        _insert_holding(db, fid1, "CUSIP_UNKN", "UNKNOWN CORP", 50000, 500)

        fid2 = _insert_filing(db, "500", "E-2", "2024-06-30", 2024, 2)
        _insert_holding(db, fid2, "CUSIP_AAPL", "APPLE", 110000, 1000)
        _insert_holding(db, fid2, "CUSIP_UNKN", "UNKNOWN CORP", 55000, 500)
        db.commit()

        returns = compute_quarterly_returns(db, "500")
        assert len(returns) == 1
        # 2 equity positions, 1 priced → confidence = 0.5
        assert returns[0]["confidence"] == pytest.approx(0.5, abs=1e-6)

    def test_single_quarter_returns_empty(self, db):
        """A fund with only 1 quarter should return empty list (need 2 for returns)."""
        _insert_security(db, "CUSIP_AAPL", "AAPL", "APPLE")
        _insert_price(db, "AAPL", "2024-03-31", 100.0)

        _insert_filer(db, "600", "Fund Foxtrot")
        fid1 = _insert_filing(db, "600", "F-1", "2024-03-31", 2024, 1)
        _insert_holding(db, fid1, "CUSIP_AAPL", "APPLE", 100000, 1000)
        db.commit()

        returns = compute_quarterly_returns(db, "600")
        assert returns == []

    def test_nonexistent_cik_returns_empty(self, db):
        """A CIK with no data should return empty list."""
        returns = compute_quarterly_returns(db, "999999")
        assert returns == []

    def test_options_excluded(self, db):
        """Options (put_call not null) should be excluded from return computation."""
        _insert_security(db, "CUSIP_AAPL", "AAPL", "APPLE")
        _insert_price(db, "AAPL", "2024-03-31", 100.0)
        _insert_price(db, "AAPL", "2024-06-30", 110.0)

        _insert_filer(db, "700", "Fund Golf")
        fid1 = _insert_filing(db, "700", "G-1", "2024-03-31", 2024, 1)
        _insert_holding(db, fid1, "CUSIP_AAPL", "APPLE", 100000, 1000)
        _insert_holding(db, fid1, "CUSIP_AAPL", "APPLE PUT", 20000, 200, put_call="PUT")

        fid2 = _insert_filing(db, "700", "G-2", "2024-06-30", 2024, 2)
        _insert_holding(db, fid2, "CUSIP_AAPL", "APPLE", 110000, 1000)
        _insert_holding(db, fid2, "CUSIP_AAPL", "APPLE PUT", 15000, 200, put_call="PUT")
        db.commit()

        returns = compute_quarterly_returns(db, "700")
        assert len(returns) == 1
        # Only equity AAPL: (110k-100k)/100k = 10%
        assert returns[0]["quarterly_return"] == pytest.approx(0.10, abs=1e-6)


class TestComputeCumulativeReturns:
    def test_basic_cumulative(self):
        """Growth of $1 with known quarterly returns."""
        quarterly = [
            {"cik": "1", "report_date": "2024-06-30", "quarterly_return": 0.10, "confidence": 1.0, "position_count": 2, "total_value": 200000},
            {"cik": "1", "report_date": "2024-09-30", "quarterly_return": -0.05, "confidence": 1.0, "position_count": 2, "total_value": 190000},
            {"cik": "1", "report_date": "2024-12-31", "quarterly_return": 0.20, "confidence": 1.0, "position_count": 2, "total_value": 228000},
        ]

        result = compute_cumulative_returns(quarterly)
        assert len(result) == 3

        # After Q2: 1.0 * 1.10 = 1.10
        assert result[0]["cumulative_value"] == pytest.approx(1.10, abs=1e-6)
        # After Q3: 1.10 * 0.95 = 1.045
        assert result[1]["cumulative_value"] == pytest.approx(1.045, abs=1e-6)
        # After Q4: 1.045 * 1.20 = 1.254
        assert result[2]["cumulative_value"] == pytest.approx(1.254, abs=1e-6)

    def test_cumulative_preserves_original_fields(self):
        """cumulative_value should be added without removing existing fields."""
        quarterly = [
            {"cik": "1", "report_date": "2024-06-30", "quarterly_return": 0.05, "confidence": 0.9, "position_count": 10, "total_value": 500000},
        ]
        result = compute_cumulative_returns(quarterly)
        assert result[0]["cik"] == "1"
        assert result[0]["report_date"] == "2024-06-30"
        assert result[0]["quarterly_return"] == 0.05
        assert result[0]["confidence"] == 0.9
        assert "cumulative_value" in result[0]

    def test_cumulative_does_not_mutate_input(self):
        """Input list should not be modified."""
        quarterly = [
            {"cik": "1", "report_date": "2024-06-30", "quarterly_return": 0.10, "confidence": 1.0, "position_count": 2, "total_value": 200000},
        ]
        compute_cumulative_returns(quarterly)
        assert "cumulative_value" not in quarterly[0]

    def test_empty_input(self):
        assert compute_cumulative_returns([]) == []

    def test_all_negative_returns(self):
        """Cumulative should decline monotonically with negative returns."""
        quarterly = [
            {"cik": "1", "report_date": "2024-06-30", "quarterly_return": -0.10, "confidence": 1.0, "position_count": 1, "total_value": 100},
            {"cik": "1", "report_date": "2024-09-30", "quarterly_return": -0.20, "confidence": 1.0, "position_count": 1, "total_value": 80},
        ]
        result = compute_cumulative_returns(quarterly)
        assert result[0]["cumulative_value"] == pytest.approx(0.90, abs=1e-6)
        assert result[1]["cumulative_value"] == pytest.approx(0.72, abs=1e-6)


class TestIntegrationWithCumulativeReturns:
    def test_end_to_end(self, db):
        """Compute quarterly returns, then compound into cumulative."""
        _setup_two_funds_four_quarters(db)

        quarterly = compute_quarterly_returns(db, "100")
        cumulative = compute_cumulative_returns(quarterly)

        assert len(cumulative) == 3
        assert all("cumulative_value" in c for c in cumulative)

        # Final cumulative value = (1 + Q2_ret) * (1 + Q3_ret) * (1 + Q4_ret)
        expected = 1.0
        for q in quarterly:
            expected *= (1 + q["quarterly_return"])
        assert cumulative[-1]["cumulative_value"] == pytest.approx(expected, abs=1e-6)
