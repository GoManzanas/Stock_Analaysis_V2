"""Tests for audit pipeline with synthetic data containing planted anomalies."""

import pytest

from db.database import get_connection, get_table_count, init_db, query_all
from audit.holdings_auditor import audit_filing_errors, audit_value_scale, run_holdings_audit
from audit.price_auditor import audit_return_outliers, audit_stale_prices, run_price_audit
from audit.reconciler import reconcile_filings, run_reconciliation


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / "test.db"


@pytest.fixture
def db(db_path):
    init_db(db_path)
    conn = get_connection(db_path)
    yield conn
    conn.close()


def _setup_basic_data(conn):
    """Insert a filer, filing, and securities for testing."""
    conn.execute("INSERT INTO filers (cik, name) VALUES (?, ?)", ("1", "Test Fund"))
    conn.execute(
        """INSERT INTO filings (cik, accession_number, report_date, report_year,
           report_quarter, form_type, total_value, holding_count, source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        ("1", "0001-24-000001", "2024-03-31", 2024, 1, "13F-HR", 10000000, 2, "bulk"),
    )
    filing_id = conn.execute("SELECT id FROM filings").fetchone()[0]

    conn.execute(
        "INSERT INTO securities (cusip, name, ticker, exchange, resolution_confidence) VALUES (?, ?, ?, ?, ?)",
        ("037833100", "APPLE INC", "AAPL", "US", 1.0),
    )
    conn.execute(
        "INSERT INTO securities (cusip, name, ticker, exchange, resolution_confidence) VALUES (?, ?, ?, ?, ?)",
        ("594918104", "MICROSOFT", "MSFT", "US", 1.0),
    )
    conn.commit()
    return filing_id


class TestValueScaleAudit:
    def test_detects_overstated_value(self, db):
        """Value ~1000x too high should be flagged."""
        filing_id = _setup_basic_data(db)

        # AAPL trading at ~170, but value implies $170,000 per share (1000x off)
        db.execute(
            """INSERT INTO holdings (filing_id, cusip, issuer_name, value, shares, sh_prn_type)
            VALUES (?, ?, ?, ?, ?, ?)""",
            (filing_id, "037833100", "APPLE INC", 170000000, 1000, "SH"),  # $170,000/share
        )
        db.execute(
            "INSERT INTO prices (ticker, date, close, adj_close, volume) VALUES (?, ?, ?, ?, ?)",
            ("AAPL", "2024-03-31", 170.0, 170.0, 50000000),
        )
        db.commit()

        findings = audit_value_scale(db)
        assert findings == 1

    def test_correct_value_no_flag(self, db):
        """Correctly valued holdings should not be flagged."""
        filing_id = _setup_basic_data(db)

        db.execute(
            """INSERT INTO holdings (filing_id, cusip, issuer_name, value, shares, sh_prn_type)
            VALUES (?, ?, ?, ?, ?, ?)""",
            (filing_id, "037833100", "APPLE INC", 170000, 1000, "SH"),  # $170/share
        )
        db.execute(
            "INSERT INTO prices (ticker, date, close, adj_close, volume) VALUES (?, ?, ?, ?, ?)",
            ("AAPL", "2024-03-31", 170.0, 170.0, 50000000),
        )
        db.commit()

        findings = audit_value_scale(db)
        assert findings == 0


class TestFilingErrorsAudit:
    def test_detects_zero_shares(self, db):
        filing_id = _setup_basic_data(db)
        db.execute(
            """INSERT INTO holdings (filing_id, cusip, issuer_name, value, shares, sh_prn_type)
            VALUES (?, ?, ?, ?, ?, ?)""",
            (filing_id, "037833100", "APPLE INC", 100000, 0, "SH"),
        )
        db.commit()

        findings = audit_filing_errors(db)
        assert findings >= 1

    def test_detects_negative_value(self, db):
        filing_id = _setup_basic_data(db)
        db.execute(
            """INSERT INTO holdings (filing_id, cusip, issuer_name, value, shares, sh_prn_type)
            VALUES (?, ?, ?, ?, ?, ?)""",
            (filing_id, "037833100", "APPLE INC", -100000, 1000, "SH"),
        )
        db.commit()

        findings = audit_filing_errors(db)
        assert findings >= 1

    def test_detects_duplicate_cusips(self, db):
        filing_id = _setup_basic_data(db)
        # Two entries for same CUSIP without PUT/CALL distinction
        db.execute(
            """INSERT INTO holdings (filing_id, cusip, issuer_name, value, shares, sh_prn_type)
            VALUES (?, ?, ?, ?, ?, ?)""",
            (filing_id, "037833100", "APPLE INC", 100000, 1000, "SH"),
        )
        db.execute(
            """INSERT INTO holdings (filing_id, cusip, issuer_name, value, shares, sh_prn_type)
            VALUES (?, ?, ?, ?, ?, ?)""",
            (filing_id, "037833100", "APPLE INC", 200000, 2000, "SH"),
        )
        db.commit()

        findings = audit_filing_errors(db)
        assert findings >= 1


class TestReturnOutliersAudit:
    def test_detects_large_price_move(self, db):
        _setup_basic_data(db)
        # AAPL drops 60% in one day (no corporate action)
        db.execute("INSERT INTO prices (ticker, date, close, adj_close) VALUES (?, ?, ?, ?)",
                   ("AAPL", "2024-03-28", 170.0, 170.0))
        db.execute("INSERT INTO prices (ticker, date, close, adj_close) VALUES (?, ?, ?, ?)",
                   ("AAPL", "2024-03-29", 68.0, 68.0))
        db.commit()

        findings = audit_return_outliers(db)
        assert findings >= 1

        # Check it's flagged as warning (no corporate action)
        result = db.execute(
            "SELECT * FROM audit_results WHERE audit_type = 'return_outlier'"
        ).fetchone()
        assert result["severity"] == "warning"

    def test_split_explained_is_info(self, db):
        _setup_basic_data(db)
        # AAPL splits 4:1 — price drops 75% but corporate action exists
        db.execute("INSERT INTO prices (ticker, date, close, adj_close) VALUES (?, ?, ?, ?)",
                   ("AAPL", "2024-03-28", 680.0, 170.0))
        db.execute("INSERT INTO prices (ticker, date, close, adj_close) VALUES (?, ?, ?, ?)",
                   ("AAPL", "2024-03-29", 170.0, 170.0))
        db.execute(
            "INSERT INTO corporate_actions (ticker, action_type, effective_date, details, source) VALUES (?, ?, ?, ?, ?)",
            ("AAPL", "split", "2024-03-29", '{"ratio": 4.0}', "eodhd"),
        )
        db.commit()

        findings = audit_return_outliers(db)
        assert findings >= 1

        result = db.execute(
            "SELECT * FROM audit_results WHERE audit_type = 'return_outlier'"
        ).fetchone()
        assert result["severity"] == "info"

    def test_normal_move_not_flagged(self, db):
        _setup_basic_data(db)
        db.execute("INSERT INTO prices (ticker, date, close, adj_close) VALUES (?, ?, ?, ?)",
                   ("AAPL", "2024-03-28", 170.0, 170.0))
        db.execute("INSERT INTO prices (ticker, date, close, adj_close) VALUES (?, ?, ?, ?)",
                   ("AAPL", "2024-03-29", 175.0, 175.0))
        db.commit()

        findings = audit_return_outliers(db)
        assert findings == 0


class TestReconciliation:
    def test_flags_large_discrepancy(self, db):
        filing_id = _setup_basic_data(db)

        # Reported total_value = $10M, but holdings × prices = ~$170K (huge discrepancy)
        db.execute(
            """INSERT INTO holdings (filing_id, cusip, issuer_name, value, shares, sh_prn_type)
            VALUES (?, ?, ?, ?, ?, ?)""",
            (filing_id, "037833100", "APPLE INC", 170000, 1000, "SH"),
        )
        db.execute(
            "INSERT INTO prices (ticker, date, close, adj_close, volume) VALUES (?, ?, ?, ?, ?)",
            ("AAPL", "2024-03-31", 170.0, 170.0, 50000000),
        )
        db.commit()

        findings = reconcile_filings(db)
        assert findings >= 1

    def test_matching_values_no_flag(self, db):
        """When computed ≈ reported, no finding should be generated."""
        conn = db
        conn.execute("INSERT INTO filers (cik, name) VALUES (?, ?)", ("2", "Good Fund"))
        conn.execute(
            """INSERT INTO filings (cik, accession_number, report_date, report_year,
               report_quarter, form_type, total_value, holding_count, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("2", "0002-24-000001", "2024-03-31", 2024, 1, "13F-HR", 170000, 1, "bulk"),
        )
        filing_id = conn.execute("SELECT id FROM filings WHERE cik = '2'").fetchone()[0]

        conn.execute(
            "INSERT OR IGNORE INTO securities (cusip, name, ticker, exchange) VALUES (?, ?, ?, ?)",
            ("037833100", "APPLE INC", "AAPL", "US"),
        )
        conn.execute(
            """INSERT INTO holdings (filing_id, cusip, issuer_name, value, shares, sh_prn_type)
            VALUES (?, ?, ?, ?, ?, ?)""",
            (filing_id, "037833100", "APPLE INC", 170000, 1000, "SH"),
        )
        conn.execute(
            "INSERT OR IGNORE INTO prices (ticker, date, close, adj_close, volume) VALUES (?, ?, ?, ?, ?)",
            ("AAPL", "2024-03-31", 170.0, 170.0, 50000000),
        )
        conn.commit()

        findings = reconcile_filings(conn)
        assert findings == 0


class TestRunFunctions:
    def test_run_holdings_audit(self, db):
        _setup_basic_data(db)
        result = run_holdings_audit(db)
        assert "total" in result

    def test_run_price_audit(self, db):
        _setup_basic_data(db)
        result = run_price_audit(db)
        assert "total" in result

    def test_run_reconciliation(self, db):
        _setup_basic_data(db)
        result = run_reconciliation(db)
        assert "discrepancies" in result
