"""Tests for BaseScraper job tracking, resume logic, and SIGINT handling."""

import json
import signal

import pytest

from db.database import init_db, get_connection
from scrapers.base import BaseScraper


class DummyScraper(BaseScraper):
    """Concrete scraper for testing."""

    job_type = "test_job"

    def __init__(self, db_path=None):
        super().__init__(db_path)
        self.ran = False
        self.items_processed = []

    def run(self, items=None, **kwargs):
        items = items or []
        for item in items:
            if self.is_interrupted:
                break
            self.items_processed.append(item)
        self.ran = True


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / "test.db"


@pytest.fixture
def scraper(db_path):
    s = DummyScraper(db_path)
    s.open()
    yield s
    s.close()


class TestJobLifecycle:
    def test_create_job(self, scraper):
        job_id = scraper.create_job("2024Q4")
        assert job_id is not None
        job = scraper.get_job("2024Q4")
        assert job["status"] == "running"
        assert job["job_type"] == "test_job"
        assert job["target"] == "2024Q4"

    def test_complete_job(self, scraper):
        job_id = scraper.create_job("2024Q4")
        scraper.complete_job(job_id)
        job = scraper.get_job("2024Q4")
        assert job["status"] == "completed"
        assert job["completed_at"] is not None

    def test_fail_job(self, scraper):
        job_id = scraper.create_job("2024Q4")
        scraper.fail_job(job_id, "connection timeout")
        job = scraper.get_job("2024Q4")
        assert job["status"] == "failed"
        assert job["error_message"] == "connection timeout"

    def test_update_progress(self, scraper):
        job_id = scraper.create_job("2024Q4")
        progress = {"current": 500, "total": 7000}
        scraper.update_progress(job_id, progress)
        scraper.conn.commit()
        job = scraper.get_job("2024Q4")
        assert job["progress"] == progress

    def test_interrupt_job(self, scraper):
        job_id = scraper.create_job("2024Q4")
        progress = {"completed_items": ["a", "b"]}
        scraper.interrupt_job(job_id, progress)
        job = scraper.get_job("2024Q4")
        assert job["status"] == "interrupted"
        assert job["progress"] == progress


class TestResumeLogic:
    def test_resume_interrupted_job(self, scraper):
        job_id = scraper.create_job("2024Q4")
        scraper.interrupt_job(job_id, {"done": ["a"]})

        resumed_id, progress = scraper.get_or_create_job("2024Q4")
        assert resumed_id == job_id
        assert progress == {"done": ["a"]}

        job = scraper.get_job("2024Q4")
        assert job["status"] == "running"
        assert job["resumed_count"] == 1

    def test_resume_running_job(self, scraper):
        """A job left in 'running' state (crash) should also be resumable."""
        job_id = scraper.create_job("2024Q4")
        scraper.update_progress(job_id, {"done": ["a"]})
        scraper.conn.commit()

        resumed_id, progress = scraper.get_or_create_job("2024Q4")
        assert resumed_id == job_id
        assert progress == {"done": ["a"]}

    def test_completed_job_returns_none_progress(self, scraper):
        job_id = scraper.create_job("2024Q4")
        scraper.complete_job(job_id)

        returned_id, progress = scraper.get_or_create_job("2024Q4")
        assert returned_id == job_id
        assert progress is None  # Already done

    def test_no_existing_job_creates_new(self, scraper):
        job_id, progress = scraper.get_or_create_job("2025Q1")
        assert job_id is not None
        assert progress is None
        job = scraper.get_job("2025Q1")
        assert job["status"] == "running"

    def test_resume_increments_count(self, scraper):
        job_id = scraper.create_job("2024Q4")
        scraper.interrupt_job(job_id, {})

        scraper.get_or_create_job("2024Q4")
        scraper.interrupt_job(job_id, {})
        scraper.get_or_create_job("2024Q4")

        job = scraper.get_job("2024Q4")
        assert job["resumed_count"] == 2


class TestSIGINTHandling:
    def test_interrupt_flag_set(self, scraper):
        assert not scraper.is_interrupted
        scraper._handle_sigint(signal.SIGINT, None)
        assert scraper.is_interrupted

    def test_scraper_stops_on_interrupt(self, scraper):
        """Scraper should stop processing when interrupted."""
        items = list(range(10))

        # Simulate interrupt after processing a few items
        class InterruptingScraper(DummyScraper):
            def run(self, items=None, **kwargs):
                items = items or []
                for item in items:
                    if self.is_interrupted:
                        break
                    self.items_processed.append(item)
                    if item == 2:
                        self._interrupted = True

        s = InterruptingScraper(scraper.db_path)
        s.open()
        s.run(items=items)
        assert s.items_processed == [0, 1, 2]
        s.close()

    def test_signal_handler_installed_and_restored(self, db_path):
        original_handler = signal.getsignal(signal.SIGINT)
        s = DummyScraper(db_path)
        s.open()
        # Handler should be installed
        current = signal.getsignal(signal.SIGINT)
        assert current == s._handle_sigint
        s.close()
        # Handler should be restored
        restored = signal.getsignal(signal.SIGINT)
        assert restored == original_handler


class TestContextManager:
    def test_context_manager(self, db_path):
        with DummyScraper(db_path) as s:
            assert s.conn is not None
            s.run(items=[1, 2, 3])
            assert s.items_processed == [1, 2, 3]
        assert s.conn is None

    def test_context_manager_restores_signal(self, db_path):
        original = signal.getsignal(signal.SIGINT)
        with DummyScraper(db_path) as s:
            pass
        assert signal.getsignal(signal.SIGINT) == original


class TestGetAllJobs:
    def test_returns_all_jobs(self, scraper):
        scraper.create_job("2024Q1")
        scraper.create_job("2024Q2")
        scraper.create_job("2024Q3")
        jobs = scraper.get_all_jobs()
        assert len(jobs) == 3
        assert [j["target"] for j in jobs] == ["2024Q1", "2024Q2", "2024Q3"]
