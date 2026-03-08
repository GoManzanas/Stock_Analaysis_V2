"""Tests for fund metrics cache logic."""

import pytest

from api.cache import get_stale_ciks, refresh_cache, is_cache_fresh, get_cache_stats
from db.database import get_connection, init_db, query_one


@pytest.fixture
def db(tmp_path):
    db_path = tmp_path / "test.db"
    init_db(db_path)
    conn = get_connection(db_path)
    yield conn
    conn.close()


# --- Helpers (same pattern as test_ranking.py) ---

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
        (filing_id, cusip, "TEST", value, shares, "SH"),
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

SPY_PRICES = [500.0, 540.0, 520.0, 570.0, 590.0]


def _setup_fund(conn, cik, name, prices):
    """Create a fund with 5 quarters of holdings at given prices."""
    cusip = f"CUSIP_{cik}"
    ticker = f"T{cik}"
    _insert_filer(conn, cik, name)
    _insert_security(conn, cusip, ticker)

    for i, (date, year, q) in enumerate(QUARTERS):
        _insert_price(conn, ticker, date, prices[i])
        _insert_benchmark(conn, date, SPY_PRICES[i])
        fid = _insert_filing(conn, cik, f"{cik}-{i+1}", date, year, q)
        _insert_holding(conn, fid, cusip, 1000 * prices[i], 1000)

    conn.commit()


# --- Tests ---

class TestGetStaleCiks:
    def test_all_stale_when_cache_empty(self, db):
        _setup_fund(db, "100", "Alpha", [100, 110, 120, 135, 150])
        _setup_fund(db, "200", "Beta", [100, 95, 90, 85, 80])

        stale = get_stale_ciks(db)
        assert set(stale) == {"100", "200"}

    def test_empty_when_no_filers(self, db):
        assert get_stale_ciks(db) == []

    def test_excludes_single_filing_filers(self, db):
        """Filers with only 1 filing can't compute returns, so skip them."""
        _insert_filer(db, "999", "Single")
        _insert_security(db, "C999", "T999")
        _insert_price(db, "T999", "2024-03-31", 100.0)
        _insert_benchmark(db, "2024-03-31", 500.0)
        fid = _insert_filing(db, "999", "999-1", "2024-03-31", 2024, 1)
        _insert_holding(db, fid, "C999", 100000, 1000)
        db.commit()

        assert get_stale_ciks(db) == []


class TestRefreshCache:
    def test_populates_cache(self, db):
        _setup_fund(db, "100", "Alpha", [100, 110, 120, 135, 150])

        count = refresh_cache(db)
        assert count == 1

        row = query_one(db, "SELECT * FROM fund_metrics_cache WHERE cik = ?", ("100",))
        assert row is not None
        assert row["name"] == "Alpha"
        assert row["quarters_active"] == 4  # 5 filings -> 4 quarterly returns
        assert row["annualized_return"] is not None
        assert row["computed_at"] is not None

    def test_cache_becomes_fresh_after_refresh(self, db):
        _setup_fund(db, "100", "Alpha", [100, 110, 120, 135, 150])

        assert not is_cache_fresh(db)
        refresh_cache(db)
        assert is_cache_fresh(db)

    def test_new_filing_makes_stale(self, db):
        _setup_fund(db, "100", "Alpha", [100, 110, 120, 135, 150])
        refresh_cache(db)
        assert is_cache_fresh(db)

        # Add a new filing with a future scraped_at
        _insert_price(db, "T100", "2025-06-30", 160.0)
        _insert_benchmark(db, "2025-06-30", 610.0)
        db.execute(
            """INSERT INTO filings (cik, accession_number, report_date, report_year,
               report_quarter, form_type, total_value, holding_count, source, scraped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now', '+1 second'))""",
            ("100", "100-new", "2025-06-30", 2025, 2, "13F-HR", 0, 0, "bulk"),
        )
        fid = db.execute(
            "SELECT id FROM filings WHERE accession_number = ?", ("100-new",)
        ).fetchone()[0]
        _insert_holding(db, fid, "CUSIP_100", 160000, 1000)
        db.commit()

        stale = get_stale_ciks(db)
        assert "100" in stale

    def test_refresh_specific_ciks(self, db):
        _setup_fund(db, "100", "Alpha", [100, 110, 120, 135, 150])
        _setup_fund(db, "200", "Beta", [100, 95, 90, 85, 80])

        count = refresh_cache(db, ciks=["100"])
        assert count == 1

        row_a = query_one(db, "SELECT * FROM fund_metrics_cache WHERE cik = ?", ("100",))
        row_b = query_one(db, "SELECT * FROM fund_metrics_cache WHERE cik = ?", ("200",))
        assert row_a is not None
        assert row_b is None

    def test_progress_callback(self, db):
        _setup_fund(db, "100", "Alpha", [100, 110, 120, 135, 150])
        _setup_fund(db, "200", "Beta", [100, 95, 90, 85, 80])

        progress = []
        refresh_cache(db, progress_callback=lambda cur, tot: progress.append((cur, tot)))

        assert len(progress) == 2
        assert progress[0] == (1, 2)
        assert progress[1] == (2, 2)


class TestCacheStats:
    def test_empty_stats(self, db):
        stats = get_cache_stats(db)
        assert stats["total_cached"] == 0
        assert stats["stale_count"] == 0

    def test_stats_after_refresh(self, db):
        _setup_fund(db, "100", "Alpha", [100, 110, 120, 135, 150])
        refresh_cache(db)

        stats = get_cache_stats(db)
        assert stats["total_cached"] == 1
        assert stats["stale_count"] == 0
        assert stats["last_refresh"] is not None
