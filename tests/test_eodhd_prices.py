"""Tests for EODHD price downloader."""

from unittest.mock import patch, MagicMock

import pytest

from db.database import get_connection, get_table_count, init_db, query_all, query_one
from scrapers.eodhd_prices import PriceScraper, fetch_eod_prices


class TestFetchEodPrices:
    @patch("scrapers.eodhd_prices.requests.get")
    def test_returns_prices(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = [
            {"date": "2024-01-02", "open": 185.0, "high": 186.0, "low": 184.0,
             "close": 185.5, "adjusted_close": 184.2, "volume": 50000000},
        ]
        mock_get.return_value = mock_resp

        prices = fetch_eod_prices("AAPL")
        assert len(prices) == 1
        assert prices[0]["close"] == 185.5

    @patch("scrapers.eodhd_prices.requests.get")
    def test_handles_error(self, mock_get):
        mock_get.side_effect = Exception("timeout")
        prices = fetch_eod_prices("AAPL")
        assert prices == []


class TestPriceScraper:
    @patch("scrapers.eodhd_prices.fetch_eod_prices")
    @patch("scrapers.eodhd_prices.time.sleep")
    def test_downloads_and_stores(self, mock_sleep, mock_fetch, tmp_path):
        db_path = tmp_path / "test.db"
        init_db(db_path)
        conn = get_connection(db_path)
        conn.execute(
            "INSERT INTO securities (cusip, name, ticker, exchange) VALUES (?, ?, ?, ?)",
            ("037833100", "APPLE INC", "AAPL", "US"),
        )
        conn.commit()
        conn.close()

        mock_fetch.return_value = [
            {"date": "2024-01-02", "open": 185.0, "high": 186.0, "low": 184.0,
             "close": 185.5, "adjusted_close": 184.2, "volume": 50000000},
            {"date": "2024-01-03", "open": 184.0, "high": 185.0, "low": 183.0,
             "close": 184.0, "adjusted_close": 183.0, "volume": 45000000},
        ]

        with PriceScraper(db_path) as scraper:
            scraper.run(batch_size=10)

        conn = get_connection(db_path)
        prices = query_all(conn, "SELECT * FROM prices WHERE ticker = ? ORDER BY date", ("AAPL",))
        assert len(prices) == 2
        assert prices[0]["adj_close"] == 184.2

        # Check benchmarks were also attempted
        assert mock_fetch.call_count >= 3  # AAPL + SPY + GSPC
        conn.close()

    @patch("scrapers.eodhd_prices.fetch_eod_prices")
    @patch("scrapers.eodhd_prices.time.sleep")
    def test_incremental_download(self, mock_sleep, mock_fetch, tmp_path):
        """Should use from_date based on existing data."""
        db_path = tmp_path / "test.db"
        init_db(db_path)
        conn = get_connection(db_path)
        conn.execute(
            "INSERT INTO securities (cusip, name, ticker, exchange) VALUES (?, ?, ?, ?)",
            ("037833100", "APPLE INC", "AAPL", "US"),
        )
        # Pre-populate some prices
        conn.execute(
            "INSERT INTO prices (ticker, date, close, adj_close, volume) VALUES (?, ?, ?, ?, ?)",
            ("AAPL", "2024-01-02", 185.5, 184.2, 50000000),
        )
        conn.commit()
        conn.close()

        mock_fetch.return_value = []

        with PriceScraper(db_path) as scraper:
            scraper.run(batch_size=10)

        # Should have called with from_date = "2024-01-02" (the max existing date)
        calls = mock_fetch.call_args_list
        aapl_call = [c for c in calls if c[0][0] == "AAPL"][0]
        assert aapl_call[0][2] == "2024-01-02"  # from_date

    @patch("scrapers.eodhd_prices.fetch_eod_prices")
    @patch("scrapers.eodhd_prices.time.sleep")
    def test_idempotent_insert(self, mock_sleep, mock_fetch, tmp_path):
        """INSERT OR IGNORE should handle duplicates gracefully."""
        db_path = tmp_path / "test.db"
        init_db(db_path)
        conn = get_connection(db_path)
        conn.execute(
            "INSERT INTO securities (cusip, name, ticker, exchange) VALUES (?, ?, ?, ?)",
            ("037833100", "APPLE INC", "AAPL", "US"),
        )
        conn.commit()
        conn.close()

        mock_fetch.return_value = [
            {"date": "2024-01-02", "open": 185.0, "high": 186.0, "low": 184.0,
             "close": 185.5, "adjusted_close": 184.2, "volume": 50000000},
        ]

        # Run twice
        with PriceScraper(db_path) as scraper:
            scraper.run(batch_size=10)

        # Reset job status to allow re-run
        conn = get_connection(db_path)
        conn.execute("DELETE FROM scrape_jobs")
        conn.commit()
        conn.close()

        with PriceScraper(db_path) as scraper:
            scraper.run(batch_size=10)

        conn = get_connection(db_path)
        count = get_table_count(conn, "prices")
        assert count == 1  # No duplicates
        conn.close()
