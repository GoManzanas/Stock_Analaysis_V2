"""Tests for CUSIP to ticker resolution via EODHD Exchange Symbol List + search fallback."""

from unittest.mock import patch, MagicMock

import pytest

from db.database import get_connection, init_db, query_one, query_all
from scrapers.eodhd_mapping import (
    CusipResolver,
    _extract_ticker_info,
    download_exchange_symbols,
    extract_cusip_from_isin,
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


# --- extract_cusip_from_isin tests ---


class TestExtractCusipFromIsin:
    def test_valid_us_isin(self):
        assert extract_cusip_from_isin("US0378331005") == "037833100"

    def test_valid_us_isin_microsoft(self):
        assert extract_cusip_from_isin("US5949181045") == "594918104"

    def test_non_us_isin(self):
        # Non-US ISINs still have a valid 9-char middle section
        assert extract_cusip_from_isin("GB0002374006") == "000237400"

    def test_too_short(self):
        assert extract_cusip_from_isin("US037833") is None

    def test_too_long(self):
        assert extract_cusip_from_isin("US03783310050") is None

    def test_empty_string(self):
        assert extract_cusip_from_isin("") is None

    def test_none_input(self):
        assert extract_cusip_from_isin(None) is None

    def test_non_alpha_prefix(self):
        assert extract_cusip_from_isin("120378331005") is None

    def test_non_alnum_cusip(self):
        assert extract_cusip_from_isin("US037833-005") is None


# --- download_exchange_symbols tests ---


class TestDownloadExchangeSymbols:
    @patch("scrapers.eodhd_mapping.requests.get")
    def test_successful_download(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = [
            {"Code": "AAPL", "Name": "Apple Inc", "Country": "USA",
             "Exchange": "US", "Currency": "USD", "Isin": "US0378331005",
             "Type": "Common Stock"},
        ]
        mock_get.return_value = mock_resp

        result = download_exchange_symbols("US")
        assert len(result) == 1
        assert result[0]["Code"] == "AAPL"

    @patch("scrapers.eodhd_mapping.requests.get")
    def test_delisted_param(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = []
        mock_get.return_value = mock_resp

        download_exchange_symbols("US", delisted=True)
        # Verify delisted=1 was passed
        call_kwargs = mock_get.call_args
        assert call_kwargs[1]["params"]["delisted"] == "1"

    @patch("scrapers.eodhd_mapping.requests.get")
    def test_api_error(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_get.return_value = mock_resp

        result = download_exchange_symbols("US")
        assert result == []

    @patch("scrapers.eodhd_mapping.requests.get")
    def test_network_error(self, mock_get):
        import requests
        mock_get.side_effect = requests.ConnectionError("fail")

        result = download_exchange_symbols("US")
        assert result == []


# --- _extract_ticker_info tests ---


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


# --- resolve_cusip_via_mapping tests (backward compat) ---


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
    def test_no_match(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = []
        mock_get.return_value = mock_resp

        result = resolve_cusip_via_mapping("000000000")
        assert result is None


# --- resolve_cusip_via_search tests ---


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


# --- CusipResolver 3-phase integration tests ---


def _mock_symbol_list():
    """Return mock exchange symbol list with ISINs."""
    return [
        {"Code": "AAPL", "Name": "Apple Inc", "Country": "USA",
         "Exchange": "US", "Currency": "USD", "Isin": "US0378331005",
         "Type": "Common Stock"},
        {"Code": "MSFT", "Name": "Microsoft Corp", "Country": "USA",
         "Exchange": "US", "Currency": "USD", "Isin": "US5949181045",
         "Type": "Common Stock"},
        {"Code": "GOOGL", "Name": "Alphabet Inc", "Country": "USA",
         "Exchange": "US", "Currency": "USD", "Isin": "US02079K3059",
         "Type": "Common Stock"},
    ]


class TestCusipResolverPhase1:
    @patch("scrapers.eodhd_mapping.download_exchange_symbols")
    @patch("scrapers.eodhd_mapping.time.sleep")
    def test_downloads_and_stores_symbols(self, mock_sleep, mock_download, db_path):
        """Phase 1 should download active + delisted symbols and store in exchange_symbols."""
        init_db(db_path)

        mock_download.side_effect = [
            _mock_symbol_list(),  # active
            [],                   # delisted
        ]

        with CusipResolver(db_path) as resolver:
            resolver._phase1_download_symbols()

        conn = get_connection(db_path)
        count = conn.execute("SELECT COUNT(*) FROM exchange_symbols").fetchone()[0]
        assert count == 3

        aapl = query_one(conn, "SELECT * FROM exchange_symbols WHERE code = ?", ("AAPL",))
        assert aapl["cusip9"] == "037833100"
        assert aapl["isin"] == "US0378331005"
        assert aapl["is_delisted"] == 0
        conn.close()

    @patch("scrapers.eodhd_mapping.download_exchange_symbols")
    @patch("scrapers.eodhd_mapping.time.sleep")
    def test_stores_delisted_flag(self, mock_sleep, mock_download, db_path):
        """Delisted symbols should have is_delisted=1."""
        init_db(db_path)

        delisted_symbols = [
            {"Code": "DEAD", "Name": "Dead Corp", "Country": "USA",
             "Exchange": "US", "Currency": "USD", "Isin": "US1234567890",
             "Type": "Common Stock"},
        ]
        mock_download.side_effect = [
            [],              # active
            delisted_symbols,  # delisted
        ]

        with CusipResolver(db_path) as resolver:
            resolver._phase1_download_symbols()

        conn = get_connection(db_path)
        row = query_one(conn, "SELECT * FROM exchange_symbols WHERE code = ?", ("DEAD",))
        assert row["is_delisted"] == 1
        conn.close()

    @patch("scrapers.eodhd_mapping.download_exchange_symbols")
    @patch("scrapers.eodhd_mapping.time.sleep")
    def test_idempotent_redownload(self, mock_sleep, mock_download, db_path):
        """Running phase 1 twice should skip if already completed."""
        init_db(db_path)

        mock_download.side_effect = [
            _mock_symbol_list(),  # active
            [],                   # delisted
        ]

        with CusipResolver(db_path) as resolver:
            resolver._phase1_download_symbols()
            # Reset mock to verify it's not called again
            mock_download.reset_mock()
            mock_download.side_effect = None
            resolver._phase1_download_symbols()

        # Should not have been called the second time
        mock_download.assert_not_called()


class TestCusipResolverPhase2:
    def test_bulk_matches_cusips(self, db_path):
        """Phase 2 should match CUSIPs to tickers via SQL JOIN on cusip9."""
        init_db(db_path)
        conn = get_connection(db_path)

        # Insert exchange symbols
        conn.execute(
            "INSERT INTO exchange_symbols (code, name, exchange, isin, cusip9, type, is_delisted) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("AAPL", "Apple Inc", "US", "US0378331005", "037833100", "Common Stock", 0),
        )
        conn.execute(
            "INSERT INTO exchange_symbols (code, name, exchange, isin, cusip9, type, is_delisted) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("MSFT", "Microsoft Corp", "US", "US5949181045", "594918104", "Common Stock", 0),
        )
        # Insert unresolved securities with matching CUSIPs
        conn.execute("INSERT INTO securities (cusip, name) VALUES (?, ?)", ("037833100", "APPLE INC"))
        conn.execute("INSERT INTO securities (cusip, name) VALUES (?, ?)", ("594918104", "MICROSOFT CORP"))
        # Insert one that won't match
        conn.execute("INSERT INTO securities (cusip, name) VALUES (?, ?)", ("000000000", "UNKNOWN"))
        conn.commit()
        conn.close()

        with CusipResolver(db_path) as resolver:
            resolver._phase2_bulk_match()

        conn = get_connection(db_path)
        aapl = query_one(conn, "SELECT * FROM securities WHERE cusip = ?", ("037833100",))
        assert aapl["ticker"] == "AAPL"
        assert aapl["eodhd_symbol"] == "AAPL.US"
        assert aapl["resolution_source"] == "bulk_symbol_list"
        assert aapl["resolution_confidence"] == 0.95

        msft = query_one(conn, "SELECT * FROM securities WHERE cusip = ?", ("594918104",))
        assert msft["ticker"] == "MSFT"

        # Unmatched CUSIP should remain unresolved
        unknown = query_one(conn, "SELECT * FROM securities WHERE cusip = ?", ("000000000",))
        assert unknown["ticker"] is None
        conn.close()

    def test_prefers_active_over_delisted(self, db_path):
        """When same CUSIP has active and delisted tickers, prefer active."""
        init_db(db_path)
        conn = get_connection(db_path)

        # Same CUSIP, two tickers: one active, one delisted
        conn.execute(
            "INSERT INTO exchange_symbols (code, name, exchange, isin, cusip9, type, is_delisted) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("OLD", "Old Ticker", "US", "US0378331005", "037833100", "Common Stock", 1),
        )
        conn.execute(
            "INSERT INTO exchange_symbols (code, name, exchange, isin, cusip9, type, is_delisted) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("AAPL", "Apple Inc", "US", "US0378331005", "037833100", "Common Stock", 0),
        )
        conn.execute("INSERT INTO securities (cusip, name) VALUES (?, ?)", ("037833100", "APPLE INC"))
        conn.commit()
        conn.close()

        with CusipResolver(db_path) as resolver:
            resolver._phase2_bulk_match()

        conn = get_connection(db_path)
        row = query_one(conn, "SELECT * FROM securities WHERE cusip = ?", ("037833100",))
        assert row["ticker"] == "AAPL"
        assert row["is_active"] == 1
        conn.close()

    def test_skips_already_resolved(self, db_path):
        """Phase 2 should not overwrite CUSIPs that already have a ticker."""
        init_db(db_path)
        conn = get_connection(db_path)

        conn.execute(
            "INSERT INTO exchange_symbols (code, name, exchange, isin, cusip9, type, is_delisted) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("AAPL", "Apple Inc", "US", "US0378331005", "037833100", "Common Stock", 0),
        )
        conn.execute(
            "INSERT INTO securities (cusip, name, ticker, resolution_source) VALUES (?, ?, ?, ?)",
            ("037833100", "APPLE INC", "AAPL", "eodhd_mapping"),
        )
        conn.commit()
        conn.close()

        with CusipResolver(db_path) as resolver:
            resolver._phase2_bulk_match()

        conn = get_connection(db_path)
        row = query_one(conn, "SELECT * FROM securities WHERE cusip = ?", ("037833100",))
        # Should still be eodhd_mapping, not overwritten to bulk_symbol_list
        assert row["resolution_source"] == "eodhd_mapping"
        conn.close()


class TestCusipResolverPhase3:
    @patch("scrapers.eodhd_mapping.resolve_cusip_via_search")
    @patch("scrapers.eodhd_mapping.time.sleep")
    def test_name_search_fallback(self, mock_sleep, mock_search, db_path):
        """Phase 3 should search by name for CUSIPs not matched in Phase 2."""
        init_db(db_path)
        conn = get_connection(db_path)
        # Unresolved CUSIP with no resolution_source (not yet attempted)
        conn.execute("INSERT INTO securities (cusip, name) VALUES (?, ?)", ("000000000", "WEIRD CORP"))
        conn.commit()
        conn.close()

        mock_search.return_value = {
            "Code": "WRDC", "Exchange": "US", "Name": "Weird Corp", "Type": "Common Stock"
        }

        with CusipResolver(db_path) as resolver:
            resolver._phase3_name_search_fallback(batch_size=10)

        conn = get_connection(db_path)
        row = query_one(conn, "SELECT * FROM securities WHERE cusip = ?", ("000000000",))
        assert row["ticker"] == "WRDC"
        assert row["resolution_source"] == "name_search"
        assert row["resolution_confidence"] == 0.7
        conn.close()

    @patch("scrapers.eodhd_mapping.resolve_cusip_via_search")
    @patch("scrapers.eodhd_mapping.time.sleep")
    def test_unresolved_marked(self, mock_sleep, mock_search, db_path):
        """CUSIPs that fail name search should be marked unresolved."""
        init_db(db_path)
        conn = get_connection(db_path)
        conn.execute("INSERT INTO securities (cusip, name) VALUES (?, ?)", ("000000000", "UNKNOWN"))
        conn.commit()
        conn.close()

        mock_search.return_value = None

        with CusipResolver(db_path) as resolver:
            resolver._phase3_name_search_fallback(batch_size=10)

        conn = get_connection(db_path)
        row = query_one(conn, "SELECT * FROM securities WHERE cusip = ?", ("000000000",))
        assert row["ticker"] is None
        assert row["resolution_source"] == "unresolved"
        assert row["resolution_confidence"] == 0.0
        conn.close()

    @patch("scrapers.eodhd_mapping.resolve_cusip_via_search")
    @patch("scrapers.eodhd_mapping.time.sleep")
    def test_skips_already_attempted(self, mock_sleep, mock_search, db_path):
        """Phase 3 should skip CUSIPs that already have a resolution_source."""
        init_db(db_path)
        conn = get_connection(db_path)
        # Already marked as unresolved from a previous run
        conn.execute(
            "INSERT INTO securities (cusip, name, resolution_source) VALUES (?, ?, ?)",
            ("000000000", "UNKNOWN", "unresolved"),
        )
        conn.commit()
        conn.close()

        with CusipResolver(db_path) as resolver:
            resolver._phase3_name_search_fallback(batch_size=10)

        mock_search.assert_not_called()


class TestCusipResolverFullFlow:
    @patch("scrapers.eodhd_mapping.download_exchange_symbols")
    @patch("scrapers.eodhd_mapping.resolve_cusip_via_search")
    @patch("scrapers.eodhd_mapping.time.sleep")
    def test_full_3_phase_flow(self, mock_sleep, mock_search, mock_download, db_path):
        """Full resolver should match via symbol list, then fall back to name search."""
        init_db(db_path)
        conn = get_connection(db_path)
        # CUSIP that matches AAPL via ISIN
        conn.execute("INSERT INTO securities (cusip, name) VALUES (?, ?)", ("037833100", "APPLE INC"))
        # CUSIP that won't match any ISIN
        conn.execute("INSERT INTO securities (cusip, name) VALUES (?, ?)", ("999999999", "MYSTERY CORP"))
        conn.commit()
        conn.close()

        # Phase 1: symbol list has AAPL
        mock_download.side_effect = [
            [{"Code": "AAPL", "Name": "Apple Inc", "Country": "USA",
              "Exchange": "US", "Currency": "USD", "Isin": "US0378331005",
              "Type": "Common Stock"}],
            [],  # delisted
        ]

        # Phase 3: name search resolves MYSTERY CORP
        mock_search.return_value = {
            "Code": "MYST", "Exchange": "US", "Name": "Mystery Corp", "Type": "Common Stock"
        }

        with CusipResolver(db_path) as resolver:
            resolver.run(batch_size=10)

        conn = get_connection(db_path)

        # AAPL matched via bulk symbol list
        aapl = query_one(conn, "SELECT * FROM securities WHERE cusip = ?", ("037833100",))
        assert aapl["ticker"] == "AAPL"
        assert aapl["resolution_source"] == "bulk_symbol_list"

        # MYSTERY matched via name search fallback
        myst = query_one(conn, "SELECT * FROM securities WHERE cusip = ?", ("999999999",))
        assert myst["ticker"] == "MYST"
        assert myst["resolution_source"] == "name_search"

        conn.close()
