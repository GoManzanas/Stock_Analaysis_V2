"""Fund endpoints: list, detail, returns, holdings, filings, compare."""

import sqlite3

from fastapi import APIRouter, Depends, HTTPException, Query

from analytics.returns import compute_cumulative_returns, compute_quarterly_returns
from analytics.screening import compute_fund_metrics
from api.deps import get_db
from api.models import (
    FilingItem,
    FundDetail,
    FundSummary,
    HoldingDiffItem,
    HoldingItem,
    PaginatedResponse,
    QuarterlyReturn,
)
from db.database import query_all, query_one, rows_to_dicts

router = APIRouter(tags=["funds"])

# Valid sort columns for fund list (must match fund_metrics_cache columns)
_VALID_SORT_COLUMNS = {
    "name", "annualized_return", "sharpe_ratio", "sp500_correlation",
    "max_drawdown", "hhi", "top5_concentration", "avg_turnover",
    "quarters_active", "latest_aum", "avg_confidence",
}

# Filter mapping: query param -> (column, comparison)
_FILTER_MAP = {
    "min_return": ("annualized_return", ">="),
    "max_return": ("annualized_return", "<="),
    "min_correlation": ("sp500_correlation", ">="),
    "max_correlation": ("sp500_correlation", "<="),
    "min_quarters": ("quarters_active", ">="),
    "min_aum": ("latest_aum", ">="),
    "min_sharpe": ("sharpe_ratio", ">="),
    "max_drawdown": ("max_drawdown", ">="),  # drawdown is negative, so >= threshold
    "min_confidence": ("avg_confidence", ">="),
    "min_hhi": ("hhi", ">="),
    "max_hhi": ("hhi", "<="),
}


def _quarter_to_date(q: str) -> str:
    """Convert quarter string (e.g. '2024Q4') to report_date ('2024-12-31')."""
    if len(q) < 5 or q[4] != "Q":
        raise HTTPException(status_code=400, detail=f"Invalid quarter format: {q}. Use YYYYQN (e.g. 2024Q4)")
    year = q[:4]
    quarter = q[5:]
    end_dates = {"1": "03-31", "2": "06-30", "3": "09-30", "4": "12-31"}
    if quarter not in end_dates:
        raise HTTPException(status_code=400, detail=f"Invalid quarter: {quarter}. Must be 1-4.")
    return f"{year}-{end_dates[quarter]}"


@router.get("/funds", response_model=PaginatedResponse[FundSummary])
def list_funds(
    search: str | None = None,
    sort_by: str = "annualized_return",
    sort_dir: str = "desc",
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    # Filters
    min_return: float | None = None,
    max_return: float | None = None,
    min_correlation: float | None = None,
    max_correlation: float | None = None,
    min_quarters: int | None = None,
    min_aum: float | None = None,
    min_sharpe: float | None = None,
    max_drawdown: float | None = None,
    min_confidence: float | None = None,
    min_hhi: float | None = None,
    max_hhi: float | None = None,
    conn: sqlite3.Connection = Depends(get_db),
):
    """List funds from the metrics cache with filtering, sorting, and pagination."""
    if sort_by not in _VALID_SORT_COLUMNS:
        raise HTTPException(status_code=400, detail=f"Invalid sort_by: {sort_by}")
    if sort_dir not in ("asc", "desc"):
        raise HTTPException(status_code=400, detail="sort_dir must be 'asc' or 'desc'")

    where_clauses = []
    params: list = []

    if search:
        where_clauses.append("name LIKE ?")
        params.append(f"%{search}%")

    # Apply filters
    filter_values = {
        "min_return": min_return, "max_return": max_return,
        "min_correlation": min_correlation, "max_correlation": max_correlation,
        "min_quarters": min_quarters, "min_aum": min_aum,
        "min_sharpe": min_sharpe, "max_drawdown": max_drawdown,
        "min_confidence": min_confidence, "min_hhi": min_hhi, "max_hhi": max_hhi,
    }
    for key, val in filter_values.items():
        if val is not None and key in _FILTER_MAP:
            col, op = _FILTER_MAP[key]
            where_clauses.append(f"{col} {op} ?")
            params.append(val)

    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

    # Count total
    total = query_one(
        conn,
        f"SELECT COUNT(*) AS cnt FROM fund_metrics_cache WHERE {where_sql}",
        tuple(params),
    )["cnt"]

    # Query page
    offset = (page - 1) * page_size
    order = "DESC" if sort_dir == "desc" else "ASC"
    null_sort = "NULLS LAST" if sort_dir == "desc" else "NULLS LAST"
    rows = query_all(
        conn,
        f"""SELECT * FROM fund_metrics_cache
            WHERE {where_sql}
            ORDER BY {sort_by} {order} {null_sort}
            LIMIT ? OFFSET ?""",
        tuple(params) + (page_size, offset),
    )

    items = [FundSummary(**dict(r)) for r in rows]
    return PaginatedResponse(items=items, total=total, page=page, page_size=page_size)


@router.get("/funds/{cik}", response_model=FundDetail)
def get_fund(cik: str, conn: sqlite3.Connection = Depends(get_db)):
    """Get full fund profile with metrics."""
    filer = query_one(conn, "SELECT * FROM filers WHERE cik = ?", (cik,))
    if not filer:
        raise HTTPException(status_code=404, detail=f"Fund with CIK {cik} not found")

    cache = query_one(conn, "SELECT * FROM fund_metrics_cache WHERE cik = ?", (cik,))
    metrics = dict(cache) if cache else compute_fund_metrics(conn, cik)

    return FundDetail(
        cik=cik,
        name=filer["name"],
        address=filer["address"],
        first_report_date=filer["first_report_date"],
        last_report_date=filer["last_report_date"],
        filing_count=filer["filing_count"],
        annualized_return=metrics.get("annualized_return"),
        sharpe_ratio=metrics.get("sharpe_ratio"),
        sp500_correlation=metrics.get("sp500_correlation"),
        max_drawdown=metrics.get("max_drawdown"),
        hhi=metrics.get("hhi"),
        top5_concentration=metrics.get("top5_concentration"),
        avg_turnover=metrics.get("avg_turnover"),
        quarters_active=metrics.get("quarters_active"),
        latest_aum=metrics.get("latest_aum"),
        avg_confidence=metrics.get("avg_confidence"),
    )


@router.get("/funds/{cik}/returns", response_model=list[QuarterlyReturn])
def get_fund_returns(
    cik: str,
    cumulative: bool = False,
    conn: sqlite3.Connection = Depends(get_db),
):
    """Get quarterly return time series for a fund."""
    _check_fund_exists(conn, cik)
    quarterly = compute_quarterly_returns(conn, cik)
    if not quarterly:
        return []

    if cumulative:
        data = compute_cumulative_returns(quarterly)
    else:
        data = quarterly

    return [QuarterlyReturn(**row) for row in data]


@router.get("/funds/{cik}/holdings", response_model=list[HoldingItem])
def get_fund_holdings(
    cik: str,
    quarter: str | None = None,
    conn: sqlite3.Connection = Depends(get_db),
):
    """Get holdings for a fund in a specific quarter (default: latest)."""
    _check_fund_exists(conn, cik)

    if quarter:
        report_date = _quarter_to_date(quarter)
    else:
        row = query_one(
            conn,
            "SELECT MAX(report_date) AS latest FROM filings WHERE cik = ?",
            (cik,),
        )
        if not row or not row["latest"]:
            return []
        report_date = row["latest"]

    rows = query_all(
        conn,
        """SELECT cusip, issuer_name, ticker, reported_value, shares, put_call, computed_value
           FROM v_holding_values
           WHERE cik = ? AND report_date = ?
           ORDER BY reported_value DESC""",
        (cik, report_date),
    )

    # Compute weights
    total = sum(r["reported_value"] or 0 for r in rows)
    items = []
    for r in rows:
        val = r["reported_value"] or 0
        items.append(HoldingItem(
            cusip=r["cusip"],
            issuer_name=r["issuer_name"],
            ticker=r["ticker"],
            value=r["reported_value"],
            shares=r["shares"],
            weight=val / total if total > 0 else 0,
            put_call=r["put_call"],
            computed_value=r["computed_value"],
        ))
    return items


@router.get("/funds/{cik}/holdings/diff", response_model=list[HoldingDiffItem])
def get_holdings_diff(
    cik: str,
    q1: str = Query(..., description="Earlier quarter (e.g. 2024Q3)"),
    q2: str = Query(..., description="Later quarter (e.g. 2024Q4)"),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Compare holdings between two quarters."""
    _check_fund_exists(conn, cik)
    date1 = _quarter_to_date(q1)
    date2 = _quarter_to_date(q2)

    def _get_holdings(report_date):
        rows = query_all(
            conn,
            """SELECT cusip, issuer_name, ticker, reported_value, shares
               FROM v_holding_values
               WHERE cik = ? AND report_date = ? AND is_option = 0""",
            (cik, report_date),
        )
        return {r["cusip"]: dict(r) for r in rows}

    h1 = _get_holdings(date1)
    h2 = _get_holdings(date2)

    all_cusips = sorted(set(h1.keys()) | set(h2.keys()))
    items = []
    for cusip in all_cusips:
        in1 = h1.get(cusip)
        in2 = h2.get(cusip)

        if in1 and not in2:
            status = "removed"
        elif in2 and not in1:
            status = "added"
        else:
            s1 = in1["shares"] or 0
            s2 = in2["shares"] or 0
            status = "unchanged" if s1 == s2 else "changed"

        items.append(HoldingDiffItem(
            cusip=cusip,
            issuer_name=(in2 or in1)["issuer_name"],
            ticker=(in2 or in1)["ticker"],
            status=status,
            q1_shares=in1["shares"] if in1 else None,
            q2_shares=in2["shares"] if in2 else None,
            q1_value=in1["reported_value"] if in1 else None,
            q2_value=in2["reported_value"] if in2 else None,
            shares_change=(
                (in2["shares"] or 0) - (in1["shares"] or 0)
                if in1 and in2 else None
            ),
            value_change=(
                (in2["reported_value"] or 0) - (in1["reported_value"] or 0)
                if in1 and in2 else None
            ),
        ))

    return items


@router.get("/funds/{cik}/filings", response_model=list[FilingItem])
def get_fund_filings(cik: str, conn: sqlite3.Connection = Depends(get_db)):
    """Get all filings for a fund."""
    _check_fund_exists(conn, cik)
    rows = query_all(
        conn,
        """SELECT id, cik, accession_number, filing_date, report_date,
                  report_year, report_quarter, form_type, amendment_type,
                  total_value, holding_count
           FROM filings WHERE cik = ? ORDER BY report_date DESC""",
        (cik,),
    )
    return [FilingItem(**dict(r)) for r in rows]


@router.get("/funds/{cik}/compare")
def compare_funds(
    cik: str,
    vs: str = Query(..., description="Comma-separated CIKs to compare against"),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Compare metrics across multiple funds."""
    all_ciks = [cik] + [c.strip() for c in vs.split(",") if c.strip()]
    results = []
    for c in all_ciks:
        filer = query_one(conn, "SELECT name FROM filers WHERE cik = ?", (c,))
        if not filer:
            continue
        cache = query_one(conn, "SELECT * FROM fund_metrics_cache WHERE cik = ?", (c,))
        metrics = dict(cache) if cache else compute_fund_metrics(conn, c)
        metrics["cik"] = c
        metrics["name"] = filer["name"]
        results.append(metrics)
    return results


def _check_fund_exists(conn: sqlite3.Connection, cik: str) -> None:
    """Raise 404 if fund doesn't exist."""
    filer = query_one(conn, "SELECT cik FROM filers WHERE cik = ?", (cik,))
    if not filer:
        raise HTTPException(status_code=404, detail=f"Fund with CIK {cik} not found")
