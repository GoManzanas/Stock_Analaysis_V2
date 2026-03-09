"""Tests for the /api/screener endpoints."""

from api.cache import refresh_cache
from tests.conftest import setup_fund


class TestScreener:
    """Tests for the GET /api/screener endpoint."""

    def test_basic_screen(self, db, client):
        """Apply min_return filter; only the good fund should pass."""
        conn, _ = db
        # Good fund: prices go up steadily
        setup_fund(conn, "111", "Good Fund", [100.0, 120.0, 140.0, 160.0, 180.0])
        # Bad fund: prices go down
        setup_fund(conn, "222", "Bad Fund", [100.0, 90.0, 80.0, 70.0, 60.0])
        refresh_cache(conn, ciks=["111", "222"])

        resp = client.get("/api/screener", params={"min_return": 0.1})
        assert resp.status_code == 200
        data = resp.json()
        ciks = [item["cik"] for item in data["items"]]
        assert "111" in ciks
        assert "222" not in ciks

    def test_pagination(self, db, client):
        """Page and page_size should control result windows."""
        conn, _ = db
        # Create 3 funds
        for i, cik in enumerate(["111", "222", "333"]):
            setup_fund(conn, cik, f"Fund {cik}", [100.0 + i * 10, 110.0 + i * 10, 120.0 + i * 10, 130.0 + i * 10, 140.0 + i * 10])
        refresh_cache(conn, ciks=["111", "222", "333"])

        resp = client.get("/api/screener", params={"page": 1, "page_size": 2})
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["items"]) == 2
        assert data["total"] == 3
        assert data["page"] == 1
        assert data["page_size"] == 2

        # Page 2
        resp2 = client.get("/api/screener", params={"page": 2, "page_size": 2})
        data2 = resp2.json()
        assert len(data2["items"]) == 1

    def test_sort(self, db, client):
        """sort_by and sort_dir should order results correctly."""
        conn, _ = db
        setup_fund(conn, "111", "Alpha Fund", [100.0, 120.0, 140.0, 160.0, 180.0])
        setup_fund(conn, "222", "Beta Fund", [100.0, 105.0, 110.0, 115.0, 120.0])
        refresh_cache(conn, ciks=["111", "222"])

        # Sort desc (default) — higher return first
        resp = client.get("/api/screener", params={"sort_by": "annualized_return", "sort_dir": "desc"})
        data = resp.json()
        assert data["items"][0]["cik"] == "111"

        # Sort asc — lower return first
        resp_asc = client.get("/api/screener", params={"sort_by": "annualized_return", "sort_dir": "asc"})
        data_asc = resp_asc.json()
        assert data_asc["items"][0]["cik"] == "222"

    def test_additional_filters(self, db, client):
        """New filters (min_turnover, max_top5) should narrow results."""
        conn, _ = db
        setup_fund(conn, "111", "Fund A", [100.0, 120.0, 140.0, 160.0, 180.0])
        setup_fund(conn, "222", "Fund B", [100.0, 105.0, 110.0, 115.0, 120.0])
        refresh_cache(conn, ciks=["111", "222"])

        # Apply a max_top5 filter that should limit results
        # Both funds have single positions so top5_concentration = 1.0
        resp = client.get("/api/screener", params={"max_top5": 0.5})
        data = resp.json()
        # Single-position funds have top5 = 1.0, so neither should pass
        assert data["total"] == 0

        # max_top5 = 1.0 should include both
        resp2 = client.get("/api/screener", params={"max_top5": 1.0})
        data2 = resp2.json()
        assert data2["total"] == 2


class TestScreenerPresets:
    """Tests for the /api/screener/presets endpoints."""

    def test_list_presets(self, client):
        """GET /api/screener/presets should return 4 presets."""
        resp = client.get("/api/screener/presets")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 4
        names = {p["name"] for p in data}
        assert names == {"top_performers", "contrarian", "concentrated", "long_track_record"}

    def test_preset_structure(self, client):
        """Each preset should have name, description, filters, sort_by."""
        resp = client.get("/api/screener/presets")
        data = resp.json()
        for preset in data:
            assert "name" in preset
            assert "description" in preset
            assert isinstance(preset["description"], str)
            assert len(preset["description"]) > 0
            assert "filters" in preset
            assert isinstance(preset["filters"], dict)
            assert "sort_by" in preset

    def test_unknown_preset(self, client):
        """GET /api/screener/presets/unknown should return 404."""
        resp = client.get("/api/screener/presets/unknown")
        assert resp.status_code == 404

    def test_run_preset_with_data(self, db, client):
        """Running a preset should apply its filters to cached data."""
        conn, _ = db
        # Fund with high return and many quarters -> should pass top_performers
        setup_fund(conn, "111", "Great Fund", [100.0, 120.0, 140.0, 160.0, 180.0])
        # Fund with declining returns -> should not pass top_performers
        setup_fund(conn, "222", "Bad Fund", [100.0, 90.0, 80.0, 70.0, 60.0])
        refresh_cache(conn, ciks=["111", "222"])

        # long_track_record requires 40+ quarters, neither fund has that
        resp = client.get("/api/screener/presets/long_track_record")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 0

        # Run with no filter preset that our funds could match
        # top_performers requires 20+ quarters and >15% CAGR, our funds have only 4 quarters
        resp2 = client.get("/api/screener/presets/top_performers")
        assert resp2.status_code == 200
        data2 = resp2.json()
        assert data2["total"] == 0  # not enough quarters
