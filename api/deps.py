"""FastAPI dependency injection for database connections."""

import sqlite3
from collections.abc import Generator

from db.database import get_connection


def get_db() -> Generator[sqlite3.Connection, None, None]:
    """Yield a database connection, closing it after the request."""
    conn = get_connection()
    try:
        yield conn
    finally:
        conn.close()
