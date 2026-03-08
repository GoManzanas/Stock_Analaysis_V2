"""Tests for database initialization, schema, and helpers."""

import json
import sqlite3

import pytest

from db.database import (
    execute_many,
    get_connection,
    get_table_count,
    init_db,
    insert_or_ignore,
    query_all,
    query_one,
    row_to_dict,
    transaction,
    upsert,
)


@pytest.fixture
def db_path(tmp_path):
    """Provide a temporary database path."""
    return tmp_path / "test.db"


@pytest.fixture
def db(db_path):
    """Provide an initialized database connection."""
    init_db(db_path)
    conn = get_connection(db_path)
    yield conn
    conn.close()


class TestConnection:
    def test_wal_mode(self, db):
        result = db.execute("PRAGMA journal_mode").fetchone()
        assert result[0] == "wal"

    def test_foreign_keys_enabled(self, db):
        result = db.execute("PRAGMA foreign_keys").fetchone()
        assert result[0] == 1

    def test_row_factory(self, db):
        assert db.row_factory == sqlite3.Row


class TestSchemaInit:
    def test_creates_all_tables(self, db):
        tables = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        table_names = sorted(r[0] for r in tables if r[0] != "sqlite_sequence")
        expected = sorted([
            "audit_results",
            "benchmark_prices",
            "corporate_actions",
            "filers",
            "filings",
            "holdings",
            "prices",
            "schema_version",
            "scrape_jobs",
            "securities",
        ])
        assert table_names == expected

    def test_schema_version_recorded(self, db):
        row = db.execute("SELECT version FROM schema_version").fetchone()
        assert row[0] == 1

    def test_idempotent_init(self, db_path):
        """Running init_db twice should not fail or duplicate data."""
        init_db(db_path)
        init_db(db_path)
        conn = get_connection(db_path)
        count = conn.execute("SELECT COUNT(*) FROM schema_version").fetchone()[0]
        conn.close()
        assert count == 1


class TestFilers:
    def test_insert_and_query(self, db):
        db.execute(
            "INSERT INTO filers (cik, name, address) VALUES (?, ?, ?)",
            ("0001234567", "Test Fund", "123 Main St"),
        )
        db.commit()
        row = query_one(db, "SELECT * FROM filers WHERE cik = ?", ("0001234567",))
        assert row is not None
        d = row_to_dict(row)
        assert d["name"] == "Test Fund"
        assert d["filing_count"] == 0


class TestFilings:
    def test_insert_with_fk(self, db):
        db.execute("INSERT INTO filers (cik, name) VALUES (?, ?)", ("0001", "Fund A"))
        db.execute(
            "INSERT INTO filings (cik, accession_number, report_date, report_year, report_quarter, form_type, source) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("0001", "0001-24-000001", "2024-12-31", 2024, 4, "13F-HR", "bulk"),
        )
        db.commit()
        row = query_one(db, "SELECT * FROM filings WHERE cik = ?", ("0001",))
        assert row is not None
        assert row_to_dict(row)["report_quarter"] == 4

    def test_unique_accession_number(self, db):
        db.execute("INSERT INTO filers (cik, name) VALUES (?, ?)", ("0001", "Fund A"))
        db.execute(
            "INSERT INTO filings (cik, accession_number, form_type, source) VALUES (?, ?, ?, ?)",
            ("0001", "0001-24-000001", "13F-HR", "bulk"),
        )
        db.commit()
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(
                "INSERT INTO filings (cik, accession_number, form_type, source) VALUES (?, ?, ?, ?)",
                ("0001", "0001-24-000001", "13F-HR", "bulk"),
            )

    def test_fk_constraint(self, db):
        """Filing with non-existent CIK should fail."""
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(
                "INSERT INTO filings (cik, accession_number, form_type, source) VALUES (?, ?, ?, ?)",
                ("9999", "9999-24-000001", "13F-HR", "bulk"),
            )


class TestHoldings:
    def test_insert_holding(self, db):
        db.execute("INSERT INTO filers (cik, name) VALUES (?, ?)", ("0001", "Fund A"))
        db.execute(
            "INSERT INTO filings (cik, accession_number, form_type, source) VALUES (?, ?, ?, ?)",
            ("0001", "0001-24-000001", "13F-HR", "bulk"),
        )
        filing_id = db.execute("SELECT id FROM filings").fetchone()[0]
        db.execute(
            "INSERT INTO holdings (filing_id, cusip, issuer_name, value, shares, sh_prn_type) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (filing_id, "037833100", "APPLE INC", 150000000.0, 1000000.0, "SH"),
        )
        db.commit()
        row = query_one(db, "SELECT * FROM holdings WHERE cusip = ?", ("037833100",))
        assert row is not None
        assert row_to_dict(row)["value"] == 150000000.0


class TestSecurities:
    def test_insert_unresolved(self, db):
        db.execute(
            "INSERT INTO securities (cusip, name) VALUES (?, ?)",
            ("037833100", "APPLE INC"),
        )
        db.commit()
        row = query_one(db, "SELECT * FROM securities WHERE cusip = ?", ("037833100",))
        d = row_to_dict(row)
        assert d["ticker"] is None
        assert d["resolution_confidence"] == 0.0
        assert d["is_active"] == 1


class TestPrices:
    def test_composite_pk(self, db):
        db.execute(
            "INSERT INTO prices (ticker, date, close, adj_close, volume) VALUES (?, ?, ?, ?, ?)",
            ("AAPL", "2024-01-02", 185.50, 184.20, 50000000),
        )
        db.commit()
        row = query_one(
            db, "SELECT * FROM prices WHERE ticker = ? AND date = ?", ("AAPL", "2024-01-02")
        )
        assert row is not None
        assert row_to_dict(row)["adj_close"] == 184.20

    def test_duplicate_pk_rejected(self, db):
        db.execute(
            "INSERT INTO prices (ticker, date, close) VALUES (?, ?, ?)",
            ("AAPL", "2024-01-02", 185.50),
        )
        db.commit()
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(
                "INSERT INTO prices (ticker, date, close) VALUES (?, ?, ?)",
                ("AAPL", "2024-01-02", 186.00),
            )


class TestScrapeJobs:
    def test_job_lifecycle(self, db):
        db.execute(
            "INSERT INTO scrape_jobs (job_type, target, status, started_at) VALUES (?, ?, ?, datetime('now'))",
            ("bulk_13f", "2024Q4", "running"),
        )
        db.commit()
        row = query_one(db, "SELECT * FROM scrape_jobs WHERE target = ?", ("2024Q4",))
        assert row_to_dict(row)["status"] == "running"

        db.execute(
            "UPDATE scrape_jobs SET status = ?, progress = ?, completed_at = datetime('now') WHERE target = ?",
            ("completed", json.dumps({"rows": 150000}), "2024Q4"),
        )
        db.commit()
        row = query_one(db, "SELECT * FROM scrape_jobs WHERE target = ?", ("2024Q4",))
        d = row_to_dict(row)
        assert d["status"] == "completed"
        assert json.loads(d["progress"])["rows"] == 150000


class TestHelpers:
    def test_transaction_commit(self, db):
        with transaction(db):
            db.execute("INSERT INTO filers (cik, name) VALUES (?, ?)", ("0001", "A"))
        assert get_table_count(db, "filers") == 1

    def test_transaction_rollback(self, db):
        try:
            with transaction(db):
                db.execute("INSERT INTO filers (cik, name) VALUES (?, ?)", ("0001", "A"))
                raise ValueError("test error")
        except ValueError:
            pass
        assert get_table_count(db, "filers") == 0

    def test_insert_or_ignore(self, db):
        rows = [("037833100", "APPLE INC"), ("594918104", "MSFT")]
        insert_or_ignore(db, "securities", ["cusip", "name"], rows)
        db.commit()
        assert get_table_count(db, "securities") == 2

        # Insert again — should be ignored
        insert_or_ignore(db, "securities", ["cusip", "name"], rows)
        db.commit()
        assert get_table_count(db, "securities") == 2

    def test_upsert(self, db):
        db.execute(
            "INSERT INTO securities (cusip, name) VALUES (?, ?)",
            ("037833100", "APPLE INC"),
        )
        db.commit()

        upsert(
            db,
            "securities",
            ["cusip", "name", "ticker"],
            [("037833100", "APPLE INC", "AAPL")],
            conflict_columns=["cusip"],
            update_columns=["ticker"],
        )
        db.commit()

        row = query_one(db, "SELECT * FROM securities WHERE cusip = ?", ("037833100",))
        assert row_to_dict(row)["ticker"] == "AAPL"

    def test_execute_many(self, db):
        db.execute("INSERT INTO filers (cik, name) VALUES (?, ?)", ("0001", "Fund"))
        db.execute(
            "INSERT INTO filings (cik, accession_number, form_type, source) VALUES (?, ?, ?, ?)",
            ("0001", "0001-24-000001", "13F-HR", "bulk"),
        )
        filing_id = db.execute("SELECT id FROM filings").fetchone()[0]
        rows = [
            (filing_id, f"CUSIP{i:04d}", f"ISSUER {i}", 1000.0 * i, 100.0 * i, "SH")
            for i in range(100)
        ]
        count = execute_many(
            db,
            "INSERT INTO holdings (filing_id, cusip, issuer_name, value, shares, sh_prn_type) VALUES (?, ?, ?, ?, ?, ?)",
            rows,
        )
        db.commit()
        assert get_table_count(db, "holdings") == 100

    def test_query_all(self, db):
        db.execute("INSERT INTO filers (cik, name) VALUES (?, ?)", ("0001", "A"))
        db.execute("INSERT INTO filers (cik, name) VALUES (?, ?)", ("0002", "B"))
        db.commit()
        rows = query_all(db, "SELECT * FROM filers ORDER BY cik")
        assert len(rows) == 2
        assert rows[0]["cik"] == "0001"

    def test_get_table_count(self, db):
        assert get_table_count(db, "filers") == 0
        db.execute("INSERT INTO filers (cik, name) VALUES (?, ?)", ("0001", "A"))
        db.commit()
        assert get_table_count(db, "filers") == 1
