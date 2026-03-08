"""Tests for the /api/funds endpoints."""

import pytest
from fastapi.testclient import TestClient

from api.cache import refresh_cache
from api.deps import get_db
from api.main import app
from db.database import get_connection, init_db


@pytest.fixture
def db(tmp_path):
    db_path = tmp_path / "test.db"
    init_db(db_path)
    conn = get_connection(db_path)
    yield conn, db_path
    conn.close()


@pytest.fixture
def client(db):
    _, db_path = db

    def override_get_db():
        test_conn = get_connection(db_path)
        try:
            yield test_conn
        finally:
            test_conn.close()

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c
    app.dependency_overrides.clear()


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


def _insert_security(conn, cusip, ticker):
    conn.execute(
        "INSERT OR IGNORE INTO securities (cusip, ticker, name, exchange, resolution_confidence) "
        "VALUES (?, ?, ?, ?, ?)",
        (cusip, ticker, "TEST", "US", 1.0),
    )


def _insert_holding(conn, filing_id, cusip, value, shares):
    conn.execute(
        """INSERT INTO holdings (filing_id, cusip, issuer_name, value, shares, sh_prn_type)
        VALUES (?, ?, ?, ?, ?, ?)""",
        (filing_id, cusip, "TEST CORP", value, shares, "SH"),
    )


def _insert_price(conn, ticker, date, adj_close):
    conn.execute(
        "INSERT OR IGNORE INTO prices (ticker, date, close, adj_close, volume) VALUES (?, ?, ?, ?, ?)",
        (ticker, date, adj_close, adj_close, 1000000),
    )


def _insert_benchmark(conn, date, adj_close):
    conn.execute(
        "INSERT OR IGNORE INTO benchmark_prices (ticker, date, close, adj_close, volume) VALUES (?, ?, ?, ?, ?)",
        ("SPY", date, adj_close, adj_close, 1000000),
    )


QUARTERS = [
    ("2024-03-31", 2024, 1),
    ("2024-06-30", 2024, 2),
    ("2024-09-30", 2024, 3),
    ("2024-12-31", 2024, 4),
    ("2025-03-31", 2025, 1),
]


def _setup_fund(conn, cik, name, prices):
    """Create a fund with 5 quarters of single-position holdings."""
    cusip = f"CUSIP_{cik}"
    ticker = f"T{cik}"
    _insert_filer(conn, cik, name)
    _insert_security(conn, cusip, ticker)

    spy_prices = [500.0, 540.0, 520.0, 570.0, 590.0]
    for i, (date, year, q) in enumerate(QUARTERS):
        _insert_price(conn, ticker, date, prices[i])
        _insert_benchmark(conn, date, spy_prices[i])
        fid = _insert_filing(conn, cik, f"{cik}-{i+1}", date, year, q)
        _insert_holding(conn, fid, cusip, 1000 * prices[i], 1000)

    conn.commit()


class TestListFunds:
    def test_empty_cache(self, client):
        resp = client.get("/api/funds")
        assert resp.status_code == 200
        data = resp.json()
        assert data["items"] == []
        assert data["total"] == 0

    def test_with_cached_funds(self, client, db):
        conn, _ = db
        _setup_fund(conn, "100", "Alpha Fund", [100, 110, 120, 135, 150])
        _setup_fund(conn, "200", "Beta Fund", [100, 95, 90, 85, 80])
        refresh_cache(conn)

        resp = client.get("/api/funds")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2
        assert len(data["items"]) == 2

    def test_search(self, client, db):
        conn, _ = db
        _setup_fund(conn, "100", "Alpha Fund", [100, 110, 120, 135, 150])
        _setup_fund(conn, "200", "Beta Fund", [100, 95, 90, 85, 80])
        refresh_cache(conn)

        resp = client.get("/api/funds?search=Alpha")
        data = resp.json()
        assert data["total"] == 1
        assert data["items"][0]["name"] == "Alpha Fund"

    def test_pagination(self, client, db):
        conn, _ = db
        _setup_fund(conn, "100", "Alpha Fund", [100, 110, 120, 135, 150])
        _setup_fund(conn, "200", "Beta Fund", [100, 95, 90, 85, 80])
        refresh_cache(conn)

        resp = client.get("/api/funds?page=1&page_size=1")
        data = resp.json()
        assert data["total"] == 2
        assert len(data["items"]) == 1
        assert data["page"] == 1
        assert data["page_size"] == 1

    def test_filter_min_return(self, client, db):
        conn, _ = db
        _setup_fund(conn, "100", "Alpha Fund", [100, 110, 120, 135, 150])
        _setup_fund(conn, "200", "Beta Fund", [100, 95, 90, 85, 80])
        refresh_cache(conn)

        resp = client.get("/api/funds?min_return=0.0")
        data = resp.json()
        # Only Alpha has positive returns
        assert data["total"] == 1
        assert data["items"][0]["cik"] == "100"


class TestGetFund:
    def test_found(self, client, db):
        conn, _ = db
        _setup_fund(conn, "100", "Alpha Fund", [100, 110, 120, 135, 150])
        refresh_cache(conn)

        resp = client.get("/api/funds/100")
        assert resp.status_code == 200
        data = resp.json()
        assert data["cik"] == "100"
        assert data["name"] == "Alpha Fund"
        assert data["annualized_return"] is not None

    def test_not_found(self, client):
        resp = client.get("/api/funds/999999")
        assert resp.status_code == 404


class TestGetReturns:
    def test_quarterly_returns(self, client, db):
        conn, _ = db
        _setup_fund(conn, "100", "Alpha Fund", [100, 110, 120, 135, 150])

        resp = client.get("/api/funds/100/returns")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 4  # 5 quarters -> 4 returns
        assert all("quarterly_return" in r for r in data)

    def test_cumulative(self, client, db):
        conn, _ = db
        _setup_fund(conn, "100", "Alpha Fund", [100, 110, 120, 135, 150])

        resp = client.get("/api/funds/100/returns?cumulative=true")
        assert resp.status_code == 200
        data = resp.json()
        assert all("cumulative_value" in r for r in data)

    def test_not_found(self, client):
        resp = client.get("/api/funds/999999/returns")
        assert resp.status_code == 404


class TestGetHoldings:
    def test_latest_quarter(self, client, db):
        conn, _ = db
        _setup_fund(conn, "100", "Alpha Fund", [100, 110, 120, 135, 150])

        resp = client.get("/api/funds/100/holdings")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) >= 1
        assert "cusip" in data[0]
        assert "weight" in data[0]

    def test_specific_quarter(self, client, db):
        conn, _ = db
        _setup_fund(conn, "100", "Alpha Fund", [100, 110, 120, 135, 150])

        resp = client.get("/api/funds/100/holdings?quarter=2024Q1")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) >= 1


class TestHoldingsDiff:
    def test_diff(self, client, db):
        conn, _ = db
        _setup_fund(conn, "100", "Alpha Fund", [100, 110, 120, 135, 150])

        resp = client.get("/api/funds/100/holdings/diff?q1=2024Q1&q2=2024Q2")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) >= 1
        assert data[0]["status"] in ("added", "removed", "changed", "unchanged")

    def test_bad_quarter_format(self, client, db):
        conn, _ = db
        _setup_fund(conn, "100", "Alpha Fund", [100, 110, 120, 135, 150])

        resp = client.get("/api/funds/100/holdings/diff?q1=bad&q2=2024Q2")
        assert resp.status_code == 400


class TestFilings:
    def test_list_filings(self, client, db):
        conn, _ = db
        _setup_fund(conn, "100", "Alpha Fund", [100, 110, 120, 135, 150])

        resp = client.get("/api/funds/100/filings")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 5


class TestCompare:
    def test_compare_two_funds(self, client, db):
        conn, _ = db
        _setup_fund(conn, "100", "Alpha Fund", [100, 110, 120, 135, 150])
        _setup_fund(conn, "200", "Beta Fund", [100, 95, 90, 85, 80])
        refresh_cache(conn)

        resp = client.get("/api/funds/100/compare?vs=200")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2
        ciks = {d["cik"] for d in data}
        assert ciks == {"100", "200"}
