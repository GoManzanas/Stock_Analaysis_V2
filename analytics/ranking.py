"""Fund screening and ranking: filter, sort, and rank funds by computed metrics."""

import logging
import sqlite3

from analytics.screening import compute_fund_metrics
from db.database import query_all

logger = logging.getLogger(__name__)

# All supported filter keys and the metric they map to
_FILTER_TO_METRIC = {
    "min_annualized_return": "annualized_return",
    "max_annualized_return": "annualized_return",
    "min_sp500_correlation": "sp500_correlation",
    "max_sp500_correlation": "sp500_correlation",
    "min_quarters_active": "quarters_active",
    "min_latest_aum": "latest_aum",
    "max_max_drawdown": "max_drawdown",
    "min_sharpe_ratio": "sharpe_ratio",
    "min_avg_confidence": "avg_confidence",
    "min_hhi": "hhi",
    "max_hhi": "hhi",
}


def screen_funds(
    conn: sqlite3.Connection,
    filters: dict,
    sort_by: str = "annualized_return",
    sort_ascending: bool = False,
    limit: int = 25,
) -> list[dict]:
    """Screen and rank funds by computed metrics.

    1. Pre-filter CIKs by quarters_active and latest_aum via cheap SQL queries
    2. Compute full metrics for remaining CIKs
    3. Apply all metric filters
    4. Sort and return top N results

    Args:
        conn: Database connection.
        filters: Dict of filter_name -> threshold (None = no filter).
        sort_by: Metric key to sort by.
        sort_ascending: If True, sort ascending; if False, descending.
        limit: Maximum number of results to return.

    Returns:
        List of dicts, each with all metric keys plus 'cik' and 'name'.
    """
    # 1. Pre-filter by quarters_active (cheap SQL)
    min_quarters = filters.get("min_quarters_active")
    # We need at least 2 filings to get 1 quarterly return, so pre-filter
    # by filing count (quarters_active = filings - 1)
    min_filings = (min_quarters + 1) if min_quarters else 2

    rows = query_all(
        conn,
        """
        SELECT f.cik, fl.name, COUNT(*) AS filing_count
        FROM filings f
        JOIN filers fl ON f.cik = fl.cik
        GROUP BY f.cik
        HAVING COUNT(*) >= ?
        """,
        (min_filings,),
    )

    candidates = [(r["cik"], r["name"]) for r in rows]

    # 2. Pre-filter by latest_aum if specified (cheap SQL on holdings)
    min_aum = filters.get("min_latest_aum")
    if min_aum is not None:
        aum_rows = query_all(
            conn,
            """
            SELECT cik, SUM(value) AS total_value
            FROM holdings h
            JOIN filings f ON h.filing_id = f.id
            WHERE f.report_date = (
                SELECT MAX(f2.report_date) FROM filings f2 WHERE f2.cik = f.cik
            )
            GROUP BY cik
            HAVING SUM(value) >= ?
            """,
            (min_aum,),
        )
        aum_ciks = {r["cik"] for r in aum_rows}
        candidates = [(cik, name) for cik, name in candidates if cik in aum_ciks]

    # 3. Compute full metrics for remaining CIKs
    results = []
    for cik, name in candidates:
        metrics = compute_fund_metrics(conn, cik)
        metrics["cik"] = cik
        metrics["name"] = name

        # 4. Apply all filters
        if _passes_filters(metrics, filters):
            results.append(metrics)

    # 5. Sort by requested metric
    results.sort(
        key=lambda m: (
            m.get(sort_by) if m.get(sort_by) is not None else float("-inf")
        ),
        reverse=not sort_ascending,
    )

    # 6. Return top N
    return results[:limit]


def _passes_filters(metrics: dict, filters: dict) -> bool:
    """Check if a fund's metrics pass all specified filters.

    For min filters: value must be >= threshold.
    For max filters: value must be <= threshold.
    If a metric is None and the filter is set, the fund is excluded.
    """
    for filter_key, threshold in filters.items():
        if threshold is None:
            continue
        if filter_key not in _FILTER_TO_METRIC:
            continue

        metric_key = _FILTER_TO_METRIC[filter_key]
        value = metrics.get(metric_key)

        # None metric with an active filter → exclude
        if value is None:
            return False

        if filter_key.startswith("min_"):
            if value < threshold:
                return False
        elif filter_key.startswith("max_"):
            if value > threshold:
                return False

    return True


# --- Prebuilt screens ---

_PREBUILT_SCREENS = {
    "top_performers": {
        "filters": {
            "min_annualized_return": 0.15,
            "min_quarters_active": 20,
            "min_avg_confidence": 0.8,
        },
        "sort_by": "annualized_return",
        "sort_ascending": False,
    },
    "contrarian": {
        "filters": {
            "max_sp500_correlation": 0.3,
            "min_quarters_active": 20,
        },
        "sort_by": "sp500_correlation",
        "sort_ascending": True,
    },
    "concentrated": {
        "filters": {
            "min_hhi": 0.1,
            "min_quarters_active": 10,
        },
        "sort_by": "hhi",
        "sort_ascending": False,
    },
    "long_track_record": {
        "filters": {
            "min_quarters_active": 40,
        },
        "sort_by": "quarters_active",
        "sort_ascending": False,
    },
}


def prebuilt_screen(
    conn: sqlite3.Connection, name: str, limit: int = 25
) -> list[dict]:
    """Run a named prebuilt screen.

    Available screens: top_performers, contrarian, concentrated, long_track_record.

    Args:
        conn: Database connection.
        name: Screen name.
        limit: Maximum results.

    Returns:
        List of fund metric dicts (same format as screen_funds).

    Raises:
        ValueError: If screen name is not recognized.
    """
    if name not in _PREBUILT_SCREENS:
        available = ", ".join(sorted(_PREBUILT_SCREENS.keys()))
        raise ValueError(
            f"Unknown screen '{name}'. Available: {available}"
        )

    config = _PREBUILT_SCREENS[name]
    return screen_funds(
        conn,
        filters=config["filters"],
        sort_by=config["sort_by"],
        sort_ascending=config["sort_ascending"],
        limit=limit,
    )
