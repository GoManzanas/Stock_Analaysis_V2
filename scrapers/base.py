"""Base scraper with job tracking, SIGINT handling, and Rich progress display."""

import json
import logging
import signal
import sqlite3
from abc import ABC, abstractmethod
from datetime import datetime, timezone

from rich.console import Console
from rich.table import Table
from rich.live import Live

from db.database import get_connection, init_db, query_one, query_all

log = logging.getLogger(__name__)
console = Console()


class BaseScraper(ABC):
    """Base class for all scrapers with job tracking and graceful interruption."""

    job_type: str = ""  # Override in subclass

    def __init__(self, db_path=None):
        self.db_path = db_path
        self.conn: sqlite3.Connection | None = None
        self._interrupted = False
        self._original_sigint = None

    @property
    def is_interrupted(self) -> bool:
        return self._interrupted

    def _handle_sigint(self, signum, frame):
        """Set interrupted flag on first SIGINT; restore original handler for second."""
        if self._interrupted:
            # Second SIGINT — force quit
            if self._original_sigint:
                self._original_sigint(signum, frame)
            return
        self._interrupted = True
        console.print("\n[yellow]Interruption requested. Finishing current batch...[/yellow]")

    def _install_signal_handler(self):
        self._original_sigint = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, self._handle_sigint)

    def _restore_signal_handler(self):
        if self._original_sigint is not None:
            signal.signal(signal.SIGINT, self._original_sigint)
            self._original_sigint = None

    def open(self):
        """Open database connection and install signal handler."""
        init_db(self.db_path)
        self.conn = get_connection(self.db_path)
        self._install_signal_handler()

    def close(self):
        """Close database connection and restore signal handler."""
        self._restore_signal_handler()
        if self.conn:
            self.conn.close()
            self.conn = None

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    # --- Job tracking ---

    def create_job(self, target: str) -> int:
        """Create a new scrape job. Returns the job ID."""
        now = datetime.now(timezone.utc).isoformat()
        cursor = self.conn.execute(
            "INSERT INTO scrape_jobs (job_type, target, status, started_at) VALUES (?, ?, ?, ?)",
            (self.job_type, target, "running", now),
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_job(self, target: str) -> dict | None:
        """Get the most recent job for this type+target."""
        row = query_one(
            self.conn,
            "SELECT * FROM scrape_jobs WHERE job_type = ? AND target = ? ORDER BY id DESC LIMIT 1",
            (self.job_type, target),
        )
        if row is None:
            return None
        d = dict(row)
        if d["progress"]:
            d["progress"] = json.loads(d["progress"])
        return d

    def get_or_create_job(self, target: str) -> tuple[int, dict | None]:
        """Get existing incomplete job or create a new one.

        Returns (job_id, progress_dict_or_None).
        If resuming, increments resumed_count.
        """
        existing = self.get_job(target)
        if existing and existing["status"] in ("running", "interrupted"):
            # Resume
            self.conn.execute(
                "UPDATE scrape_jobs SET status = ?, resumed_count = resumed_count + 1 WHERE id = ?",
                ("running", existing["id"]),
            )
            self.conn.commit()
            log.info("Resuming job %d for %s (resume #%d)", existing["id"], target, existing["resumed_count"] + 1)
            return existing["id"], existing.get("progress")

        if existing and existing["status"] == "completed":
            return existing["id"], None  # Already done

        job_id = self.create_job(target)
        return job_id, None

    def update_progress(self, job_id: int, progress: dict):
        """Update job progress (JSON)."""
        self.conn.execute(
            "UPDATE scrape_jobs SET progress = ? WHERE id = ?",
            (json.dumps(progress), job_id),
        )
        # Don't commit here — caller manages transactions

    def complete_job(self, job_id: int):
        """Mark job as completed."""
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            "UPDATE scrape_jobs SET status = ?, completed_at = ? WHERE id = ?",
            ("completed", now, job_id),
        )
        self.conn.commit()

    def fail_job(self, job_id: int, error: str):
        """Mark job as failed with error message."""
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            "UPDATE scrape_jobs SET status = ?, error_message = ?, completed_at = ? WHERE id = ?",
            ("failed", error, now, job_id),
        )
        self.conn.commit()

    def interrupt_job(self, job_id: int, progress: dict):
        """Mark job as interrupted with current progress."""
        self.conn.execute(
            "UPDATE scrape_jobs SET status = ?, progress = ? WHERE id = ?",
            ("interrupted", json.dumps(progress), job_id),
        )
        self.conn.commit()

    def get_all_jobs(self) -> list[dict]:
        """Get all jobs of this type."""
        rows = query_all(
            self.conn,
            "SELECT * FROM scrape_jobs WHERE job_type = ? ORDER BY id",
            (self.job_type,),
        )
        result = []
        for row in rows:
            d = dict(row)
            if d["progress"]:
                d["progress"] = json.loads(d["progress"])
            result.append(d)
        return result

    # --- Abstract ---

    @abstractmethod
    def run(self, **kwargs):
        """Execute the scraper. Subclasses implement this."""
        ...
