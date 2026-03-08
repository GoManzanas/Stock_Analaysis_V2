"""Tests for the /api/stats endpoint."""

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
    conn, db_path = db

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


class TestStatsEndpoint:
    def test_empty_db(self, client):
        resp = client.get("/api/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_filers"] == 0
        assert data["total_filings"] == 0
        assert data["total_holdings"] == 0
        assert data["cache_stats"]["total_cached"] == 0

    def test_with_data(self, client, db):
        conn, _ = db
        conn.execute("INSERT INTO filers (cik, name) VALUES ('100', 'Test Fund')")
        conn.execute(
            """INSERT INTO filings (cik, accession_number, report_date, report_year,
               report_quarter, form_type, total_value, holding_count, source)
            VALUES ('100', 'acc-1', '2024-03-31', 2024, 1, '13F-HR', 0, 0, 'bulk')"""
        )
        conn.commit()

        resp = client.get("/api/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_filers"] == 1
        assert data["total_filings"] == 1
