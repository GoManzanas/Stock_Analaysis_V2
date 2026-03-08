"""Tests for corporate actions scraper."""

from unittest.mock import patch, MagicMock

import pytest

from db.database import get_connection, get_table_count, init_db, query_all
from scrapers.eodhd_corporate import CorporateActionsScraper, _parse_split_ratio, fetch_splits


class TestParseSplitRatio:
    def test_forward_split(self):
        assert _parse_split_ratio("4/1") == 4.0

    def test_reverse_split(self):
        assert _parse_split_ratio("1/10") == 0.1

    def test_fractional(self):
        assert _parse_split_ratio("3/2") == 1.5

    def test_invalid(self):
        assert _parse_split_ratio("abc") is None
        assert _parse_split_ratio("") is None


class TestFetchSplits:
    @patch("scrapers.eodhd_corporate.requests.get")
    def test_returns_splits(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = [
            {"date": "2020-08-31", "split": "4/1"},
            {"date": "2014-06-09", "split": "7/1"},
        ]
        mock_get.return_value = mock_resp

        splits = fetch_splits("AAPL")
        assert len(splits) == 2
        assert splits[0]["split"] == "4/1"


class TestCorporateActionsScraper:
    @patch("scrapers.eodhd_corporate.fetch_splits")
    @patch("scrapers.eodhd_corporate.time.sleep")
    def test_downloads_and_stores(self, mock_sleep, mock_splits, tmp_path):
        db_path = tmp_path / "test.db"
        init_db(db_path)
        conn = get_connection(db_path)
        conn.execute(
            "INSERT INTO securities (cusip, name, ticker, exchange) VALUES (?, ?, ?, ?)",
            ("037833100", "APPLE INC", "AAPL", "US"),
        )
        conn.commit()
        conn.close()

        mock_splits.return_value = [
            {"date": "2020-08-31", "split": "4/1"},
            {"date": "2014-06-09", "split": "7/1"},
        ]

        with CorporateActionsScraper(db_path) as scraper:
            scraper.run(batch_size=10)

        conn = get_connection(db_path)
        actions = query_all(conn, "SELECT * FROM corporate_actions ORDER BY effective_date")
        assert len(actions) == 2
        assert actions[0]["ticker"] == "AAPL"
        assert actions[0]["action_type"] == "split"
        conn.close()

    @patch("scrapers.eodhd_corporate.fetch_splits")
    @patch("scrapers.eodhd_corporate.time.sleep")
    def test_reverse_split_type(self, mock_sleep, mock_splits, tmp_path):
        db_path = tmp_path / "test.db"
        init_db(db_path)
        conn = get_connection(db_path)
        conn.execute(
            "INSERT INTO securities (cusip, name, ticker, exchange) VALUES (?, ?, ?, ?)",
            ("123456789", "REVERSE CO", "REV", "US"),
        )
        conn.commit()
        conn.close()

        mock_splits.return_value = [{"date": "2023-01-15", "split": "1/10"}]

        with CorporateActionsScraper(db_path) as scraper:
            scraper.run(batch_size=10)

        conn = get_connection(db_path)
        action = conn.execute("SELECT * FROM corporate_actions").fetchone()
        assert action["action_type"] == "reverse_split"
        conn.close()
