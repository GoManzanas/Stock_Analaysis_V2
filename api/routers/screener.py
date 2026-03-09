"""Screener endpoints: advanced multi-filter screening with preset support."""

import sqlite3

from fastapi import APIRouter, Depends, HTTPException, Query

from analytics.ranking import _PREBUILT_SCREENS
from api.deps import get_db
from api.models import FundSummary, PaginatedResponse, ScreenerPreset
from db.database import query_all, query_one

router = APIRouter(tags=["screener"])

# Valid sort columns (must match fund_metrics_cache columns)
_VALID_SORT_COLUMNS = {
    "name", "annualized_return", "sharpe_ratio", "sp500_correlation",
    "max_drawdown", "hhi", "top5_concentration", "avg_turnover",
    "quarters_active", "latest_aum", "avg_confidence",
}

# Filter mapping: query param -> (column, comparison operator)
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
    "min_turnover": ("avg_turnover", ">="),
    "max_turnover": ("avg_turnover", "<="),
    "min_top5": ("top5_concentration", ">="),
    "max_top5": ("top5_concentration", "<="),
}

# Human-readable descriptions for prebuilt screens
_PRESET_DESCRIPTIONS = {
    "top_performers": "Funds with >15% CAGR, 20+ quarters, high confidence",
    "contrarian": "Low S&P 500 correlation (<0.3), 20+ quarters",
    "concentrated": "High concentration (HHI >0.1), 10+ quarters",
    "long_track_record": "40+ quarters of filing history",
}

# Map prebuilt screen filter keys to our _FILTER_MAP keys
_PRESET_FILTER_KEY_MAP = {
    "min_annualized_return": "min_return",
    "max_annualized_return": "max_return",
    "min_sp500_correlation": "min_correlation",
    "max_sp500_correlation": "max_correlation",
    "min_quarters_active": "min_quarters",
    "min_latest_aum": "min_aum",
    "min_sharpe_ratio": "min_sharpe",
    "max_max_drawdown": "max_drawdown",
    "min_avg_confidence": "min_confidence",
    "min_hhi": "min_hhi",
    "max_hhi": "max_hhi",
    "min_avg_turnover": "min_turnover",
    "max_avg_turnover": "max_turnover",
    "min_top5_concentration": "min_top5",
    "max_top5_concentration": "max_top5",
}


def _build_where(filter_values: dict, search: str | None = None):
    """Build WHERE clauses and params from a filter dict."""
    where_clauses = []
    params: list = []

    if search:
        where_clauses.append("name LIKE ?")
        params.append(f"%{search}%")

    for key, val in filter_values.items():
        if val is not None and key in _FILTER_MAP:
            col, op = _FILTER_MAP[key]
            where_clauses.append(f"{col} {op} ?")
            params.append(val)

    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
    return where_sql, params


@router.get("/screener", response_model=PaginatedResponse[FundSummary])
def screen_funds(
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
    min_turnover: float | None = None,
    max_turnover: float | None = None,
    min_top5: float | None = None,
    max_top5: float | None = None,
    conn: sqlite3.Connection = Depends(get_db),
):
    """Advanced multi-filter screening against the fund metrics cache."""
    if sort_by not in _VALID_SORT_COLUMNS:
        raise HTTPException(status_code=400, detail=f"Invalid sort_by: {sort_by}")
    if sort_dir not in ("asc", "desc"):
        raise HTTPException(status_code=400, detail="sort_dir must be 'asc' or 'desc'")

    filter_values = {
        "min_return": min_return, "max_return": max_return,
        "min_correlation": min_correlation, "max_correlation": max_correlation,
        "min_quarters": min_quarters, "min_aum": min_aum,
        "min_sharpe": min_sharpe, "max_drawdown": max_drawdown,
        "min_confidence": min_confidence, "min_hhi": min_hhi, "max_hhi": max_hhi,
        "min_turnover": min_turnover, "max_turnover": max_turnover,
        "min_top5": min_top5, "max_top5": max_top5,
    }

    where_sql, params = _build_where(filter_values, search)

    # Count total
    total = query_one(
        conn,
        f"SELECT COUNT(*) AS cnt FROM fund_metrics_cache WHERE {where_sql}",
        tuple(params),
    )["cnt"]

    # Query page
    offset = (page - 1) * page_size
    order = "DESC" if sort_dir == "desc" else "ASC"
    null_sort = "NULLS LAST"
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


@router.get("/screener/presets", response_model=list[ScreenerPreset])
def list_presets():
    """List available preset screens with descriptions."""
    presets = []
    for name, config in _PREBUILT_SCREENS.items():
        presets.append(ScreenerPreset(
            name=name,
            description=_PRESET_DESCRIPTIONS.get(name, ""),
            filters=config["filters"],
            sort_by=config["sort_by"],
        ))
    return presets


@router.get("/screener/presets/{name}", response_model=PaginatedResponse[FundSummary])
def run_preset(
    name: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Run a specific preset screen against the fund metrics cache."""
    if name not in _PREBUILT_SCREENS:
        available = ", ".join(sorted(_PREBUILT_SCREENS.keys()))
        raise HTTPException(
            status_code=404,
            detail=f"Unknown preset '{name}'. Available: {available}",
        )

    config = _PREBUILT_SCREENS[name]

    # Translate preset filter keys to our _FILTER_MAP keys
    filter_values = {}
    for preset_key, val in config["filters"].items():
        mapped_key = _PRESET_FILTER_KEY_MAP.get(preset_key)
        if mapped_key:
            filter_values[mapped_key] = val

    sort_by = config["sort_by"]
    if sort_by not in _VALID_SORT_COLUMNS:
        raise HTTPException(status_code=400, detail=f"Invalid sort_by in preset: {sort_by}")
    sort_dir = "asc" if config.get("sort_ascending", False) else "desc"

    where_sql, params = _build_where(filter_values)

    # Count total
    total = query_one(
        conn,
        f"SELECT COUNT(*) AS cnt FROM fund_metrics_cache WHERE {where_sql}",
        tuple(params),
    )["cnt"]

    # Query page
    offset = (page - 1) * page_size
    order = "DESC" if sort_dir == "desc" else "ASC"
    null_sort = "NULLS LAST"
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
