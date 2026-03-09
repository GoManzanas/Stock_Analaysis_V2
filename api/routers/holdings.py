"""Holdings endpoints: position history tracking."""

import sqlite3

from fastapi import APIRouter, Depends, Query

from api.deps import get_db
from api.models import PositionHistoryPoint
from db.database import query_all

router = APIRouter(tags=["holdings"])


@router.get("/holdings/position-history", response_model=list[PositionHistoryPoint])
def get_position_history(
    cik: str = Query(..., description="Fund CIK identifier"),
    cusip: str = Query(..., description="Security CUSIP identifier"),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Track a fund's position in a security over time."""
    rows = query_all(
        conn,
        """SELECT v.report_date,
                  v.report_year || 'Q' || v.report_quarter AS quarter,
                  v.shares, v.reported_value AS value, v.price_on_date AS price,
                  CASE WHEN pq.total_reported_value > 0
                       THEN v.reported_value * 1.0 / pq.total_reported_value
                       ELSE NULL
                  END AS weight
           FROM v_holding_values v
           JOIN v_portfolio_quarterly pq ON v.cik = pq.cik AND v.report_date = pq.report_date
           WHERE v.cik = ? AND v.cusip = ?
           ORDER BY v.report_date""",
        (cik, cusip),
    )
    return [PositionHistoryPoint(**dict(r)) for r in rows]
