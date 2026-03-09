"""Shared test fixtures for API tests."""

import pytest
from fastapi.testclient import TestClient

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

QUARTERS = [
    ("2024-03-31", 2024, 1),
    ("2024-06-30", 2024, 2),
    ("2024-09-30", 2024, 3),
    ("2024-12-31", 2024, 4),
    ("2025-03-31", 2025, 1),
]


def insert_filer(conn, cik, name="Test Fund"):
    conn.execute("INSERT INTO filers (cik, name) VALUES (?, ?)", (cik, name))


def insert_filing(conn, cik, accession, report_date, year, quarter):
    conn.execute(
        """INSERT INTO filings (cik, accession_number, report_date, report_year,
           report_quarter, form_type, total_value, holding_count, source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (cik, accession, report_date, year, quarter, "13F-HR", 0, 0, "bulk"),
    )
    return conn.execute(
        "SELECT id FROM filings WHERE accession_number = ?", (accession,)
    ).fetchone()[0]


def insert_security(conn, cusip, ticker, name="TEST"):
    conn.execute(
        "INSERT OR IGNORE INTO securities (cusip, ticker, name, exchange, resolution_confidence) "
        "VALUES (?, ?, ?, ?, ?)",
        (cusip, ticker, name, "US", 1.0),
    )


def insert_holding(conn, filing_id, cusip, value, shares):
    conn.execute(
        """INSERT INTO holdings (filing_id, cusip, issuer_name, value, shares, sh_prn_type)
        VALUES (?, ?, ?, ?, ?, ?)""",
        (filing_id, cusip, "TEST CORP", value, shares, "SH"),
    )


def insert_price(conn, ticker, date, adj_close):
    conn.execute(
        "INSERT OR IGNORE INTO prices (ticker, date, close, adj_close, volume) VALUES (?, ?, ?, ?, ?)",
        (ticker, date, adj_close, adj_close, 1000000),
    )


def insert_benchmark(conn, date, adj_close):
    conn.execute(
        "INSERT OR IGNORE INTO benchmark_prices (ticker, date, close, adj_close, volume) VALUES (?, ?, ?, ?, ?)",
        ("SPY", date, adj_close, adj_close, 1000000),
    )


def setup_fund(conn, cik, name, prices):
    """Create a fund with 5 quarters of single-position holdings."""
    cusip = f"CUSIP_{cik}"
    ticker = f"T{cik}"
    insert_filer(conn, cik, name)
    insert_security(conn, cusip, ticker)

    spy_prices = [500.0, 540.0, 520.0, 570.0, 590.0]
    for i, (date, year, q) in enumerate(QUARTERS):
        insert_price(conn, ticker, date, prices[i])
        insert_benchmark(conn, date, spy_prices[i])
        fid = insert_filing(conn, cik, f"{cik}-{i+1}", date, year, q)
        insert_holding(conn, fid, cusip, 1000 * prices[i], 1000)

    conn.commit()
