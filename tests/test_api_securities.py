"""Tests for the /api/securities endpoints."""

from tests.conftest import insert_filer, insert_filing, insert_holding, insert_security


class TestGetSecurity:
    def test_found(self, db, client):
        conn, _ = db
        insert_security(conn, "111111111", "AAPL", name="Apple Inc")
        conn.commit()

        resp = client.get("/api/securities/111111111")
        assert resp.status_code == 200
        data = resp.json()
        assert data["cusip"] == "111111111"
        assert data["ticker"] == "AAPL"
        assert data["name"] == "Apple Inc"
        assert data["exchange"] == "US"
        assert data["resolution_confidence"] == 1.0

    def test_not_found(self, client):
        resp = client.get("/api/securities/NONEXIST9")
        assert resp.status_code == 404


class TestSecuritySearch:
    def test_search_by_ticker(self, db, client):
        conn, _ = db
        insert_security(conn, "111111111", "AAPL", name="Apple Inc")
        insert_security(conn, "222222222", "MSFT", name="Microsoft Corp")
        conn.commit()

        resp = client.get("/api/securities/search", params={"q": "AAPL"})
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["ticker"] == "AAPL"

    def test_search_by_name(self, db, client):
        conn, _ = db
        insert_security(conn, "111111111", "AAPL", name="Apple Inc")
        insert_security(conn, "222222222", "MSFT", name="Microsoft Corp")
        conn.commit()

        resp = client.get("/api/securities/search", params={"q": "Apple"})
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["cusip"] == "111111111"

    def test_exact_ticker_first(self, db, client):
        conn, _ = db
        insert_security(conn, "111111111", "AA", name="Alcoa Corp")
        insert_security(conn, "222222222", "AAPL", name="Apple Inc")
        insert_security(conn, "333333333", "AAL", name="American Airlines")
        conn.commit()

        resp = client.get("/api/securities/search", params={"q": "AA"})
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 3
        # Exact match first
        assert data[0]["ticker"] == "AA"
        # Prefix matches next (AAL, AAPL sorted by name)
        prefix_tickers = {data[1]["ticker"], data[2]["ticker"]}
        assert prefix_tickers == {"AAPL", "AAL"}


class TestSecurityHolders:
    def test_holders_for_quarter(self, db, client):
        conn, _ = db
        cusip = "111111111"
        insert_security(conn, cusip, "AAPL", name="Apple Inc")
        insert_filer(conn, "100", "Fund Alpha")
        insert_filer(conn, "200", "Fund Beta")
        fid1 = insert_filing(conn, "100", "ACC-100-1", "2024-12-31", 2024, 4)
        fid2 = insert_filing(conn, "200", "ACC-200-1", "2024-12-31", 2024, 4)
        insert_holding(conn, fid1, cusip, 5000000, 10000)
        insert_holding(conn, fid2, cusip, 3000000, 6000)
        conn.commit()

        resp = client.get(f"/api/securities/{cusip}/holders", params={"quarter": "2024Q4"})
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2
        # Ordered by value DESC
        assert data[0]["cik"] == "100"
        assert data[0]["value"] == 5000000
        assert data[1]["cik"] == "200"
        assert data[1]["value"] == 3000000
        # Weights should sum to 1
        total_weight = sum(h["weight"] for h in data)
        assert abs(total_weight - 1.0) < 1e-9

    def test_default_latest_quarter(self, db, client):
        conn, _ = db
        cusip = "111111111"
        insert_security(conn, cusip, "AAPL", name="Apple Inc")
        insert_filer(conn, "100", "Fund Alpha")
        fid1 = insert_filing(conn, "100", "ACC-100-Q3", "2024-09-30", 2024, 3)
        fid2 = insert_filing(conn, "100", "ACC-100-Q4", "2024-12-31", 2024, 4)
        insert_holding(conn, fid1, cusip, 4000000, 8000)
        insert_holding(conn, fid2, cusip, 5000000, 10000)
        conn.commit()

        resp = client.get(f"/api/securities/{cusip}/holders")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        # Should return the latest quarter (Q4)
        assert data[0]["value"] == 5000000

    def test_cusip_not_found(self, client):
        resp = client.get("/api/securities/NONEXIST9/holders")
        assert resp.status_code == 200
        assert resp.json() == []


class TestSecurityHolderHistory:
    def test_history_across_quarters(self, db, client):
        conn, _ = db
        cusip = "111111111"
        insert_security(conn, cusip, "AAPL", name="Apple Inc")
        insert_filer(conn, "100", "Fund Alpha")
        insert_filer(conn, "200", "Fund Beta")

        quarters = [
            ("2024-06-30", 2024, 2),
            ("2024-09-30", 2024, 3),
            ("2024-12-31", 2024, 4),
        ]
        for i, (date, year, q) in enumerate(quarters):
            fid1 = insert_filing(conn, "100", f"ACC-100-{i}", date, year, q)
            insert_holding(conn, fid1, cusip, 1000000 * (i + 1), 1000 * (i + 1))
            if i >= 1:
                # Second fund joins in Q3
                fid2 = insert_filing(conn, "200", f"ACC-200-{i}", date, year, q)
                insert_holding(conn, fid2, cusip, 500000, 500)
        conn.commit()

        resp = client.get(f"/api/securities/{cusip}/holders/history")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 3
        # Q2: 1 holder
        assert data[0]["quarter"] == "2024Q2"
        assert data[0]["holder_count"] == 1
        # Q3: 2 holders
        assert data[1]["quarter"] == "2024Q3"
        assert data[1]["holder_count"] == 2
        # Q4: 2 holders
        assert data[2]["quarter"] == "2024Q4"
        assert data[2]["holder_count"] == 2

    def test_empty_history(self, client):
        resp = client.get("/api/securities/NONEXIST9/holders/history")
        assert resp.status_code == 200
        assert resp.json() == []
