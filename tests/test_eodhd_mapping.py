"""Tests for CUSIP to ticker resolution via EODHD APIs."""

from unittest.mock import patch, MagicMock

import pytest

from db.database import get_connection, init_db, query_one
from scrapers.eodhd_mapping import (
    CusipResolver,
    _extract_ticker_info,
    resolve_cusip_via_mapping,
    resolve_cusip_via_search,
)


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / "test.db"


@pytest.fixture
def db(db_path):
    init_db(db_path)
    conn = get_connection(db_path)
    yield conn
    conn.close()


class TestExtractTickerInfo:
    def test_standard_response(self):
        result = {
            "Code": "AAPL",
            "Exchange": "US",
            "Name": "Apple Inc",
            "Type": "Common Stock",
        }
        info = _extract_ticker_info(result)
        assert info["ticker"] == "AAPL"
        assert info["eodhd_symbol"] == "AAPL.US"
        assert info["name"] == "Apple Inc"

    def test_lowercase_keys(self):
        result = {
            "code": "MSFT",
            "exchange": "US",
            "name": "Microsoft Corp",
            "type": "Common Stock",
        }
        info = _extract_ticker_info(result)
        assert info["ticker"] == "MSFT"
        assert info["eodhd_symbol"] == "MSFT.US"


class TestResolveCusipViaMapping:
    @patch("scrapers.eodhd_mapping.requests.get")
    def test_successful_mapping(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = [
            {"Code": "AAPL", "Exchange": "US", "Name": "Apple Inc", "Type": "Common Stock"}
        ]
        mock_get.return_value = mock_resp

        result = resolve_cusip_via_mapping("037833100")
        assert result is not None
        assert result["Code"] == "AAPL"

    @patch("scrapers.eodhd_mapping.requests.get")
    def test_fallback_to_6_digit(self, mock_get):
        """Should try 6-digit CUSIP if 9-digit fails."""
        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            mock_resp = MagicMock()
            if call_count == 1:
                # 9-digit CUSIP returns empty
                mock_resp.status_code = 200
                mock_resp.json.return_value = []
            else:
                # 6-digit CUSIP succeeds
                mock_resp.status_code = 200
                mock_resp.json.return_value = [
                    {"Code": "AAPL", "Exchange": "US", "Name": "Apple Inc"}
                ]
            return mock_resp

        mock_get.side_effect = side_effect
        result = resolve_cusip_via_mapping("037833100")
        assert result is not None
        assert call_count == 2

    @patch("scrapers.eodhd_mapping.requests.get")
    def test_no_match(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = []
        mock_get.return_value = mock_resp

        result = resolve_cusip_via_mapping("000000000")
        assert result is None


class TestResolveCusipViaSearch:
    @patch("scrapers.eodhd_mapping.requests.get")
    def test_search_success(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = [
            {"Code": "AAPL", "Exchange": "US", "Name": "Apple Inc", "Type": "Common Stock"}
        ]
        mock_get.return_value = mock_resp

        result = resolve_cusip_via_search("APPLE INC")
        assert result is not None
        assert result["Code"] == "AAPL"

    def test_empty_name(self):
        result = resolve_cusip_via_search("")
        assert result is None

    @patch("scrapers.eodhd_mapping.requests.get")
    def test_prefers_us_exchange(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = [
            {"Code": "AAPL", "Exchange": "LSE", "Name": "Apple Inc"},
            {"Code": "AAPL", "Exchange": "US", "Name": "Apple Inc"},
        ]
        mock_get.return_value = mock_resp

        result = resolve_cusip_via_search("APPLE INC")
        assert result["Exchange"] == "US"


class TestCusipResolver:
    @patch("scrapers.eodhd_mapping.resolve_cusip_via_mapping")
    @patch("scrapers.eodhd_mapping.time.sleep")
    def test_resolves_unresolved_cusips(self, mock_sleep, mock_mapping, db_path):
        """Should update securities table with resolved ticker info."""
        init_db(db_path)
        conn = get_connection(db_path)
        conn.execute("INSERT INTO securities (cusip, name) VALUES (?, ?)", ("037833100", "APPLE INC"))
        conn.execute("INSERT INTO securities (cusip, name) VALUES (?, ?)", ("594918104", "MICROSOFT CORP"))
        conn.commit()
        conn.close()

        mock_mapping.side_effect = [
            {"Code": "AAPL", "Exchange": "US", "Name": "Apple Inc", "Type": "Common Stock"},
            {"Code": "MSFT", "Exchange": "US", "Name": "Microsoft Corp", "Type": "Common Stock"},
        ]

        with CusipResolver(db_path) as resolver:
            resolver.run(batch_size=10)

        conn = get_connection(db_path)
        aapl = query_one(conn, "SELECT * FROM securities WHERE cusip = ?", ("037833100",))
        assert aapl["ticker"] == "AAPL"
        assert aapl["eodhd_symbol"] == "AAPL.US"
        assert aapl["resolution_source"] == "eodhd_mapping"
        assert aapl["resolution_confidence"] == 1.0

        msft = query_one(conn, "SELECT * FROM securities WHERE cusip = ?", ("594918104",))
        assert msft["ticker"] == "MSFT"
        conn.close()

    @patch("scrapers.eodhd_mapping.resolve_cusip_via_mapping")
    @patch("scrapers.eodhd_mapping.resolve_cusip_via_search")
    @patch("scrapers.eodhd_mapping.time.sleep")
    def test_fallback_to_search(self, mock_sleep, mock_search, mock_mapping, db_path):
        """Should fall back to search when mapping fails."""
        init_db(db_path)
        conn = get_connection(db_path)
        conn.execute("INSERT INTO securities (cusip, name) VALUES (?, ?)", ("037833100", "APPLE INC"))
        conn.commit()
        conn.close()

        mock_mapping.return_value = None
        mock_search.return_value = {"Code": "AAPL", "Exchange": "US", "Name": "Apple Inc", "Type": "Common Stock"}

        with CusipResolver(db_path) as resolver:
            resolver.run(batch_size=10)

        conn = get_connection(db_path)
        row = query_one(conn, "SELECT * FROM securities WHERE cusip = ?", ("037833100",))
        assert row["ticker"] == "AAPL"
        assert row["resolution_source"] == "name_search"
        assert row["resolution_confidence"] == 0.7
        conn.close()

    @patch("scrapers.eodhd_mapping.resolve_cusip_via_mapping")
    @patch("scrapers.eodhd_mapping.resolve_cusip_via_search")
    @patch("scrapers.eodhd_mapping.time.sleep")
    def test_unresolved_marked(self, mock_sleep, mock_search, mock_mapping, db_path):
        """Unresolvable CUSIPs should be marked with confidence 0."""
        init_db(db_path)
        conn = get_connection(db_path)
        conn.execute("INSERT INTO securities (cusip, name) VALUES (?, ?)", ("000000000", "UNKNOWN"))
        conn.commit()
        conn.close()

        mock_mapping.return_value = None
        mock_search.return_value = None

        with CusipResolver(db_path) as resolver:
            resolver.run(batch_size=10)

        conn = get_connection(db_path)
        row = query_one(conn, "SELECT * FROM securities WHERE cusip = ?", ("000000000",))
        assert row["ticker"] is None
        assert row["resolution_source"] == "unresolved"
        assert row["resolution_confidence"] == 0.0
        conn.close()

    @patch("scrapers.eodhd_mapping.resolve_cusip_via_mapping")
    @patch("scrapers.eodhd_mapping.time.sleep")
    def test_skips_already_resolved(self, mock_sleep, mock_mapping, db_path):
        """Should skip CUSIPs that already have a ticker."""
        init_db(db_path)
        conn = get_connection(db_path)
        conn.execute(
            "INSERT INTO securities (cusip, name, ticker, resolution_source) VALUES (?, ?, ?, ?)",
            ("037833100", "APPLE INC", "AAPL", "eodhd_mapping"),
        )
        conn.commit()
        conn.close()

        with CusipResolver(db_path) as resolver:
            resolver.run(batch_size=10)

        mock_mapping.assert_not_called()
