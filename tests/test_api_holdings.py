"""Tests for holdings API endpoints."""

import pytest

from tests.conftest import (
    insert_filer,
    insert_filing,
    insert_holding,
    insert_price,
    insert_security,
    QUARTERS,
)


def _setup_position_data(conn):
    insert_filer(conn, "100", "Test Fund")
    insert_security(conn, "CUSIP1", "AAPL", "Apple Inc")
    insert_security(conn, "CUSIP2", "MSFT", "Microsoft Corp")

    for i, (date, year, q) in enumerate(QUARTERS[:3]):
        insert_price(conn, "AAPL", date, 150.0 + i * 10)
        fid = insert_filing(conn, "100", f"acc-{i}", date, year, q)
        insert_holding(conn, fid, "CUSIP1", 15000 + i * 1000, 100)
        insert_holding(conn, fid, "CUSIP2", 5000, 50)
    conn.commit()


class TestPositionHistory:
    def test_position_across_quarters(self, db, client):
        conn, _ = db
        _setup_position_data(conn)

        resp = client.get("/api/holdings/position-history", params={"cik": "100", "cusip": "CUSIP1"})
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 3

        # Results should be in date order
        dates = [d["report_date"] for d in data]
        assert dates == sorted(dates)

        # Check shares/value/price for first quarter
        assert data[0]["shares"] == 100
        assert data[0]["value"] == 15000
        assert data[0]["price"] == 150.0
        assert data[0]["quarter"] == "2024Q1"

        # Check second quarter
        assert data[1]["shares"] == 100
        assert data[1]["value"] == 16000
        assert data[1]["price"] == 160.0
        assert data[1]["quarter"] == "2024Q2"

        # Check third quarter
        assert data[2]["shares"] == 100
        assert data[2]["value"] == 17000
        assert data[2]["price"] == 170.0
        assert data[2]["quarter"] == "2024Q3"

    def test_weight_computation(self, db, client):
        conn, _ = db
        _setup_position_data(conn)

        resp = client.get("/api/holdings/position-history", params={"cik": "100", "cusip": "CUSIP1"})
        assert resp.status_code == 200
        data = resp.json()

        # First quarter: CUSIP1 value=15000, CUSIP2 value=5000, total=20000
        # weight should be 15000/20000 = 0.75
        assert data[0]["weight"] == pytest.approx(15000 / (15000 + 5000), rel=1e-4)

        # Second quarter: CUSIP1 value=16000, CUSIP2 value=5000, total=21000
        assert data[1]["weight"] == pytest.approx(16000 / (16000 + 5000), rel=1e-4)

    def test_no_data(self, db, client):
        conn, _ = db
        # Don't insert any data — query with valid but nonexistent cik+cusip
        resp = client.get("/api/holdings/position-history", params={"cik": "999", "cusip": "NOPE"})
        assert resp.status_code == 200
        assert resp.json() == []

    def test_missing_params(self, client):
        # Missing both params
        resp = client.get("/api/holdings/position-history")
        assert resp.status_code == 422

        # Missing cusip
        resp = client.get("/api/holdings/position-history", params={"cik": "100"})
        assert resp.status_code == 422

        # Missing cik
        resp = client.get("/api/holdings/position-history", params={"cusip": "CUSIP1"})
        assert resp.status_code == 422
