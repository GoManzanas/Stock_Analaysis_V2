"""Tests for the prices API router."""

from tests.conftest import insert_benchmark, insert_price


def _insert_full_price(conn, ticker, date, open_p, high, low, close, adj_close, volume):
    conn.execute(
        "INSERT OR IGNORE INTO prices (ticker, date, open, high, low, close, adj_close, volume) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (ticker, date, open_p, high, low, close, adj_close, volume),
    )


class TestGetPrices:
    def test_full_range(self, db, client):
        conn, _ = db
        _insert_full_price(conn, "AAPL", "2024-01-02", 180.0, 185.0, 179.0, 183.0, 182.5, 50000000)
        _insert_full_price(conn, "AAPL", "2024-01-03", 183.0, 186.0, 182.0, 185.0, 184.5, 45000000)
        _insert_full_price(conn, "AAPL", "2024-01-04", 185.0, 187.0, 184.0, 186.0, 185.5, 42000000)
        conn.commit()

        resp = client.get("/api/prices/AAPL")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 3
        assert data[0]["date"] == "2024-01-02"
        assert data[1]["date"] == "2024-01-03"
        assert data[2]["date"] == "2024-01-04"
        assert data[0]["open"] == 180.0
        assert data[0]["volume"] == 50000000

    def test_date_filter(self, db, client):
        conn, _ = db
        for i in range(1, 6):
            _insert_full_price(
                conn, "MSFT", f"2024-01-0{i}",
                300.0 + i, 305.0 + i, 299.0 + i, 302.0 + i, 301.0 + i, 30000000 + i,
            )
        conn.commit()

        resp = client.get("/api/prices/MSFT", params={"start_date": "2024-01-02", "end_date": "2024-01-04"})
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 3
        assert data[0]["date"] == "2024-01-02"
        assert data[-1]["date"] == "2024-01-04"

    def test_unknown_ticker(self, db, client):
        resp = client.get("/api/prices/UNKNOWN")
        assert resp.status_code == 200
        assert resp.json() == []


class TestBenchmarkComparison:
    def test_comparison(self, db, client):
        conn, _ = db
        _insert_full_price(conn, "AAPL", "2024-01-02", 180.0, 185.0, 179.0, 183.0, 182.5, 50000000)
        _insert_full_price(conn, "AAPL", "2024-01-03", 183.0, 186.0, 182.0, 185.0, 184.5, 45000000)
        insert_benchmark(conn, "2024-01-02", 470.0)
        insert_benchmark(conn, "2024-01-03", 472.0)
        conn.commit()

        resp = client.get("/api/prices/AAPL/benchmark")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2
        assert data[0]["ticker_close"] == 183.0
        assert data[0]["ticker_adj_close"] == 182.5
        assert data[0]["benchmark_close"] == 470.0
        assert data[0]["benchmark_adj_close"] == 470.0
        assert data[1]["date"] == "2024-01-03"

    def test_missing_benchmark_dates(self, db, client):
        conn, _ = db
        _insert_full_price(conn, "GOOG", "2024-01-02", 140.0, 142.0, 139.0, 141.0, 140.5, 20000000)
        _insert_full_price(conn, "GOOG", "2024-01-03", 141.0, 143.0, 140.0, 142.0, 141.5, 21000000)
        # Only insert benchmark for one date
        insert_benchmark(conn, "2024-01-02", 470.0)
        conn.commit()

        resp = client.get("/api/prices/GOOG/benchmark")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2
        # First date has benchmark
        assert data[0]["benchmark_close"] == 470.0
        # Second date has no benchmark
        assert data[1]["benchmark_close"] is None
        assert data[1]["benchmark_adj_close"] is None
        # Ticker data should still be present
        assert data[1]["ticker_close"] == 142.0
