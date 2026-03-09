"""Securities endpoints: search, detail, holders, holder history."""

import sqlite3

from fastapi import APIRouter, Depends, HTTPException, Query

from api.deps import get_db
from api.models import (
    SecurityHolder,
    SecurityHolderHistoryPoint,
    SecurityInfo,
    SecuritySearchResult,
)
from api.utils import quarter_to_date
from db.database import query_all, query_one

router = APIRouter(tags=["securities"])


@router.get("/securities/search", response_model=list[SecuritySearchResult])
def search_securities(
    q: str = Query(..., description="Search by ticker or name"),
    limit: int = Query(20, ge=1, le=100),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Search securities by ticker or issuer name."""
    rows = query_all(
        conn,
        """SELECT cusip, ticker, name, exchange
           FROM securities
           WHERE ticker LIKE ? OR name LIKE ?
           ORDER BY
               CASE WHEN UPPER(ticker) = ? THEN 0
                    WHEN UPPER(ticker) LIKE ? THEN 1
                    ELSE 2
               END, name
           LIMIT ?""",
        (f"%{q}%", f"%{q}%", q.upper(), f"{q.upper()}%", limit),
    )
    return [SecuritySearchResult(**dict(r)) for r in rows]


@router.get("/securities/{cusip}", response_model=SecurityInfo)
def get_security(cusip: str, conn: sqlite3.Connection = Depends(get_db)):
    """Get security info by CUSIP."""
    row = query_one(
        conn,
        "SELECT cusip, ticker, name, exchange, is_active, resolution_confidence FROM securities WHERE cusip = ?",
        (cusip,),
    )
    if not row:
        raise HTTPException(status_code=404, detail=f"Security with CUSIP {cusip} not found")
    return SecurityInfo(**dict(row))


@router.get("/securities/{cusip}/holders", response_model=list[SecurityHolder])
def get_security_holders(
    cusip: str,
    quarter: str | None = None,
    conn: sqlite3.Connection = Depends(get_db),
):
    """Get funds holding this security in a given quarter (default: latest)."""
    if quarter:
        report_date = quarter_to_date(quarter)
    else:
        row = query_one(
            conn,
            """SELECT MAX(f.report_date) AS latest
               FROM holdings h
               JOIN filings f ON h.filing_id = f.id
               WHERE h.cusip = ?""",
            (cusip,),
        )
        if not row or not row["latest"]:
            return []
        report_date = row["latest"]

    rows = query_all(
        conn,
        """SELECT f.cik, fl.name, h.shares, h.value
           FROM holdings h
           JOIN filings f ON h.filing_id = f.id
           JOIN filers fl ON f.cik = fl.cik
           WHERE h.cusip = ? AND f.report_date = ?
             AND (f.amendment_type IS NULL OR f.amendment_type = 'RESTATEMENT')
           ORDER BY h.value DESC""",
        (cusip, report_date),
    )

    total_value = sum(r["value"] or 0 for r in rows)
    items = []
    for r in rows:
        val = r["value"] or 0
        items.append(SecurityHolder(
            cik=r["cik"],
            name=r["name"],
            shares=r["shares"],
            value=r["value"],
            weight=val / total_value if total_value > 0 else 0,
        ))
    return items


@router.get("/securities/{cusip}/holders/history", response_model=list[SecurityHolderHistoryPoint])
def get_security_holder_history(
    cusip: str,
    conn: sqlite3.Connection = Depends(get_db),
):
    """Get holder count and totals over time for a security."""
    rows = query_all(
        conn,
        """SELECT f.report_date,
                  f.report_year || 'Q' || f.report_quarter AS quarter,
                  COUNT(DISTINCT f.cik) AS holder_count,
                  COALESCE(SUM(h.shares), 0) AS total_shares,
                  COALESCE(SUM(h.value), 0) AS total_value
           FROM holdings h
           JOIN filings f ON h.filing_id = f.id
           WHERE h.cusip = ?
             AND (f.amendment_type IS NULL OR f.amendment_type = 'RESTATEMENT')
           GROUP BY f.report_date
           ORDER BY f.report_date""",
        (cusip,),
    )
    return [SecurityHolderHistoryPoint(**dict(r)) for r in rows]
