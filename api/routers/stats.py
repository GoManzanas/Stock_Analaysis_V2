"""Stats endpoint: database summary counts."""

import sqlite3

from fastapi import APIRouter, Depends

from api.cache import get_cache_stats
from api.deps import get_db
from api.models import StatsResponse
from db.database import get_table_count

router = APIRouter(tags=["stats"])


@router.get("/stats", response_model=StatsResponse)
def get_stats(conn: sqlite3.Connection = Depends(get_db)):
    """Return database summary statistics."""
    resolved = conn.execute(
        "SELECT COUNT(*) FROM securities WHERE ticker IS NOT NULL"
    ).fetchone()[0]
    unresolved = conn.execute(
        "SELECT COUNT(*) FROM securities WHERE ticker IS NULL"
    ).fetchone()[0]

    return StatsResponse(
        total_filers=get_table_count(conn, "filers"),
        total_filings=get_table_count(conn, "filings"),
        total_holdings=get_table_count(conn, "holdings"),
        total_securities=get_table_count(conn, "securities"),
        resolved_securities=resolved,
        unresolved_securities=unresolved,
        total_prices=get_table_count(conn, "prices"),
        total_corporate_actions=get_table_count(conn, "corporate_actions"),
        total_audit_results=get_table_count(conn, "audit_results"),
        cache_stats=get_cache_stats(conn),
    )
