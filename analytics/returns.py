"""Quarterly return computation for 13F institutional holdings."""

import logging
import sqlite3
from collections import defaultdict

from db.database import query_all, query_one

logger = logging.getLogger(__name__)


def get_nearest_price(conn: sqlite3.Connection, ticker: str, date: str) -> float | None:
    """Find the nearest prior trading day's adj_close, looking back up to 5 business days.

    Useful when report_date falls on a weekend or holiday.
    """
    row = query_one(
        conn,
        """
        SELECT adj_close FROM prices
        WHERE ticker = ? AND date <= ? AND date >= date(?, '-7 days')
        ORDER BY date DESC
        LIMIT 1
        """,
        (ticker, date, date),
    )
    return row["adj_close"] if row else None


def compute_quarterly_returns(
    conn: sqlite3.Connection, cik: str
) -> list[dict]:
    """Compute quarterly portfolio returns using quarter-end snapshot diffing.

    Algorithm:
    1. Query v_holding_values for the given CIK (non-option holdings only)
    2. Group by quarter into position snapshots: {cusip: {shares, computed_value}}
    3. For consecutive quarter pairs, compute weighted-average return:
       - Continuing positions: (value_curr - value_prev) / value_prev
       - New positions: 0% return (assumed bought at quarter-end)
       - Exited positions: 0% return (assumed sold at prev quarter-end)
    4. Confidence = priced_positions / equity_positions from v_portfolio_quarterly
    """
    # 1. Get all non-option holdings for this CIK
    holdings_rows = query_all(
        conn,
        """
        SELECT report_date, cusip, shares, computed_value, reported_value
        FROM v_holding_values
        WHERE cik = ? AND is_option = 0
        ORDER BY report_date
        """,
        (cik,),
    )

    if not holdings_rows:
        logger.info("No holdings found for CIK %s", cik)
        return []

    # 2. Group by quarter into snapshots
    snapshots: dict[str, dict[str, dict]] = defaultdict(dict)
    for row in holdings_rows:
        report_date = row["report_date"]
        cusip = row["cusip"]
        snapshots[report_date][cusip] = {
            "shares": row["shares"],
            "computed_value": row["computed_value"],
            "reported_value": row["reported_value"],
        }

    sorted_dates = sorted(snapshots.keys())

    if len(sorted_dates) < 2:
        logger.info("CIK %s has only %d quarter(s), need at least 2 for returns", cik, len(sorted_dates))
        return []

    # Get portfolio-level stats for confidence scores
    portfolio_rows = query_all(
        conn,
        """
        SELECT report_date, position_count, total_reported_value,
               total_computed_value, priced_positions, equity_positions,
               price_coverage
        FROM v_portfolio_quarterly
        WHERE cik = ?
        ORDER BY report_date
        """,
        (cik,),
    )
    portfolio_stats = {row["report_date"]: dict(row) for row in portfolio_rows}

    # 3. Compute returns for each consecutive quarter pair
    results = []
    for i in range(1, len(sorted_dates)):
        prev_date = sorted_dates[i - 1]
        curr_date = sorted_dates[i]

        prev_snap = snapshots[prev_date]
        curr_snap = snapshots[curr_date]

        prev_cusips = set(prev_snap.keys())
        curr_cusips = set(curr_snap.keys())

        continuing = prev_cusips & curr_cusips
        new_positions = curr_cusips - prev_cusips
        # exited = prev_cusips - curr_cusips  (contribute 0% return)

        weighted_return_sum = 0.0
        total_weight = 0.0

        # Continuing positions: return = (curr_value - prev_value) / prev_value
        for cusip in continuing:
            prev_val = prev_snap[cusip]["computed_value"]
            curr_val = curr_snap[cusip]["computed_value"]

            # Skip positions without computed values (unresolved CUSIPs)
            if prev_val is None or curr_val is None or prev_val == 0:
                continue

            position_return = (curr_val - prev_val) / prev_val
            weight = prev_val
            weighted_return_sum += position_return * weight
            total_weight += weight

        # New positions: 0% return, weighted by curr-quarter value
        for cusip in new_positions:
            curr_val = curr_snap[cusip]["computed_value"]
            if curr_val is None:
                continue
            # 0% return, so no addition to weighted_return_sum
            total_weight += curr_val

        # Exited positions: 0% return, no weight contribution needed
        # (they were sold at prev quarter-end, so they don't affect this quarter's return)

        quarterly_return = weighted_return_sum / total_weight if total_weight > 0 else 0.0

        # Get confidence and stats from portfolio view
        stats = portfolio_stats.get(curr_date, {})
        equity_pos = stats.get("equity_positions", 0)
        priced_pos = stats.get("priced_positions", 0)
        confidence = priced_pos / equity_pos if equity_pos > 0 else 0.0

        total_value = stats.get("total_reported_value", 0)
        position_count = stats.get("position_count", 0)

        results.append({
            "cik": cik,
            "report_date": curr_date,
            "quarterly_return": quarterly_return,
            "confidence": confidence,
            "position_count": position_count,
            "total_value": total_value,
        })

    return results


def compute_cumulative_returns(quarterly_returns: list[dict]) -> list[dict]:
    """Compound quarterly returns into cumulative growth-of-$1 series.

    Each entry gets a `cumulative_value` field representing the value of $1
    invested at the start.
    """
    if not quarterly_returns:
        return []

    result = []
    cumulative = 1.0

    for qr in quarterly_returns:
        cumulative *= (1.0 + qr["quarterly_return"])
        entry = dict(qr)
        entry["cumulative_value"] = cumulative
        result.append(entry)

    return result
