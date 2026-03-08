"""Database connection, initialization, and helpers for SQLite with WAL mode."""

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from config.settings import DB_PATH

SCHEMA_PATH = Path(__file__).parent / "schema.sql"
VIEWS_PATH = Path(__file__).parent / "views.sql"
SCHEMA_VERSION = 1


def get_connection(db_path: Path | None = None) -> sqlite3.Connection:
    """Create a SQLite connection with WAL mode and foreign keys enabled."""
    path = db_path or DB_PATH
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: Path | None = None) -> None:
    """Initialize database schema if not already created."""
    conn = get_connection(db_path)
    try:
        schema_sql = SCHEMA_PATH.read_text()
        conn.executescript(schema_sql)

        views_sql = VIEWS_PATH.read_text()
        conn.executescript(views_sql)

        # Record schema version if not present
        existing = conn.execute(
            "SELECT version FROM schema_version WHERE version = ?",
            (SCHEMA_VERSION,),
        ).fetchone()
        if not existing:
            conn.execute(
                "INSERT INTO schema_version (version) VALUES (?)",
                (SCHEMA_VERSION,),
            )
            conn.commit()
    finally:
        conn.close()


@contextmanager
def transaction(conn: sqlite3.Connection):
    """Context manager for database transactions. Commits on success, rolls back on error."""
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def execute_many(
    conn: sqlite3.Connection, sql: str, rows: list[tuple | dict]
) -> int:
    """Bulk insert/update with executemany. Returns number of rows affected."""
    cursor = conn.executemany(sql, rows)
    return cursor.rowcount


def insert_or_ignore(
    conn: sqlite3.Connection, table: str, columns: list[str], rows: list[tuple]
) -> int:
    """INSERT OR IGNORE into a table. Returns number of rows inserted."""
    placeholders = ", ".join("?" for _ in columns)
    col_names = ", ".join(columns)
    sql = f"INSERT OR IGNORE INTO {table} ({col_names}) VALUES ({placeholders})"
    cursor = conn.executemany(sql, rows)
    return cursor.rowcount


def upsert(
    conn: sqlite3.Connection,
    table: str,
    columns: list[str],
    rows: list[tuple],
    conflict_columns: list[str],
    update_columns: list[str],
) -> int:
    """INSERT ... ON CONFLICT DO UPDATE for upsert operations."""
    placeholders = ", ".join("?" for _ in columns)
    col_names = ", ".join(columns)
    conflict = ", ".join(conflict_columns)
    updates = ", ".join(f"{c} = excluded.{c}" for c in update_columns)
    sql = (
        f"INSERT INTO {table} ({col_names}) VALUES ({placeholders}) "
        f"ON CONFLICT({conflict}) DO UPDATE SET {updates}"
    )
    cursor = conn.executemany(sql, rows)
    return cursor.rowcount


def query_one(
    conn: sqlite3.Connection, sql: str, params: tuple = ()
) -> sqlite3.Row | None:
    """Execute a query and return a single row or None."""
    return conn.execute(sql, params).fetchone()


def query_all(
    conn: sqlite3.Connection, sql: str, params: tuple = ()
) -> list[sqlite3.Row]:
    """Execute a query and return all rows."""
    return conn.execute(sql, params).fetchall()


def row_to_dict(row: sqlite3.Row) -> dict:
    """Convert a sqlite3.Row to a plain dict."""
    return dict(row)


def rows_to_dicts(rows: list[sqlite3.Row]) -> list[dict]:
    """Convert a list of sqlite3.Row objects to dicts."""
    return [dict(r) for r in rows]


def get_table_count(conn: sqlite3.Connection, table: str) -> int:
    """Get row count for a table."""
    result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    return result[0]
