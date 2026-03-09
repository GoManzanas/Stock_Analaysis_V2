"""Price endpoints: time series and benchmark comparison."""

import sqlite3

from fastapi import APIRouter, Depends, Query

from api.deps import get_db
from api.models import BenchmarkComparison, PricePoint
from db.database import query_all

router = APIRouter(tags=["prices"])


@router.get("/prices/{ticker}", response_model=list[PricePoint])
def get_prices(
    ticker: str,
    start_date: str | None = None,
    end_date: str | None = None,
    conn: sqlite3.Connection = Depends(get_db),
):
    """Get price time series for a ticker."""
    sql = (
        "SELECT date, open, high, low, close, adj_close, volume "
        "FROM prices WHERE ticker = ?"
    )
    params: list = [ticker]

    if start_date is not None:
        sql += " AND date >= ?"
        params.append(start_date)
    if end_date is not None:
        sql += " AND date <= ?"
        params.append(end_date)

    sql += " ORDER BY date ASC"

    rows = query_all(conn, sql, tuple(params))
    return [PricePoint(**dict(r)) for r in rows]


@router.get("/prices/{ticker}/benchmark", response_model=list[BenchmarkComparison])
def get_benchmark_comparison(
    ticker: str,
    start_date: str | None = None,
    end_date: str | None = None,
    benchmark: str = Query("SPY"),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Compare a ticker's price series against a benchmark."""
    sql = """
        SELECT p.date, p.close AS ticker_close, p.adj_close AS ticker_adj_close,
               b.close AS benchmark_close, b.adj_close AS benchmark_adj_close
        FROM prices p
        LEFT JOIN benchmark_prices b ON p.date = b.date AND b.ticker = ?
        WHERE p.ticker = ?
          AND (? IS NULL OR p.date >= ?) AND (? IS NULL OR p.date <= ?)
        ORDER BY p.date
    """
    params = (benchmark, ticker, start_date, start_date, end_date, end_date)
    rows = query_all(conn, sql, params)
    return [BenchmarkComparison(**dict(r)) for r in rows]
