"""Fund screening metrics: CAGR, Sharpe, correlation, drawdown, concentration, turnover."""

import logging
import sqlite3
import statistics
from functools import reduce

from analytics.returns import compute_quarterly_returns, compute_cumulative_returns
from config.settings import MIN_QUARTERS_FOR_METRICS, RISK_FREE_RATE_QUARTERLY
from db.database import query_all, query_one

logger = logging.getLogger(__name__)


def compute_concentration_metrics(
    conn: sqlite3.Connection, cik: str, report_date: str | None = None
) -> dict:
    """Compute HHI, top-5 weight, and position count for a single quarter.

    Args:
        conn: Database connection.
        cik: Filer CIK.
        report_date: Quarter to analyze. Defaults to latest available.

    Returns:
        Dict with keys: hhi, top5_concentration, position_count.
        Returns empty dict if no holdings found.
    """
    if report_date is None:
        row = query_one(
            conn,
            """
            SELECT MAX(report_date) AS latest
            FROM v_holding_values
            WHERE cik = ? AND is_option = 0
            """,
            (cik,),
        )
        if not row or row["latest"] is None:
            return {}
        report_date = row["latest"]

    rows = query_all(
        conn,
        """
        SELECT cusip, reported_value
        FROM v_holding_values
        WHERE cik = ? AND report_date = ? AND is_option = 0
        """,
        (cik, report_date),
    )

    if not rows:
        return {}

    total_value = sum(r["reported_value"] for r in rows)
    if total_value == 0:
        return {"hhi": 0.0, "top5_concentration": 0.0, "position_count": len(rows)}

    weights = [r["reported_value"] / total_value for r in rows]
    hhi = sum(w ** 2 for w in weights)

    sorted_weights = sorted(weights, reverse=True)
    top5 = sum(sorted_weights[:5])

    return {
        "hhi": hhi,
        "top5_concentration": top5,
        "position_count": len(rows),
    }


def compute_turnover(conn: sqlite3.Connection, cik: str) -> float | None:
    """Average quarterly turnover across all consecutive quarter pairs.

    Turnover per quarter = |value of new positions + value of exited positions| / total portfolio value.
    Returns None if fewer than 2 quarters.
    """
    rows = query_all(
        conn,
        """
        SELECT report_date, cusip, reported_value
        FROM v_holding_values
        WHERE cik = ? AND is_option = 0
        ORDER BY report_date
        """,
        (cik,),
    )

    if not rows:
        return None

    # Group by quarter
    from collections import defaultdict
    snapshots: dict[str, dict[str, float]] = defaultdict(dict)
    quarter_totals: dict[str, float] = defaultdict(float)
    for r in rows:
        rd = r["report_date"]
        snapshots[rd][r["cusip"]] = r["reported_value"]
        quarter_totals[rd] += r["reported_value"]

    sorted_dates = sorted(snapshots.keys())
    if len(sorted_dates) < 2:
        return None

    turnovers = []
    for i in range(1, len(sorted_dates)):
        prev_date = sorted_dates[i - 1]
        curr_date = sorted_dates[i]

        prev_cusips = set(snapshots[prev_date].keys())
        curr_cusips = set(snapshots[curr_date].keys())

        new_positions = curr_cusips - prev_cusips
        exited_positions = prev_cusips - curr_cusips

        new_value = sum(snapshots[curr_date][c] for c in new_positions)
        exited_value = sum(snapshots[prev_date][c] for c in exited_positions)

        total = quarter_totals[curr_date]
        if total > 0:
            turnover = abs(new_value + exited_value) / total
            turnovers.append(turnover)

    if not turnovers:
        return None

    return statistics.mean(turnovers)


def compute_fund_metrics(conn: sqlite3.Connection, cik: str) -> dict:
    """Compute comprehensive fund metrics for screening.

    Returns dict with keys:
        annualized_return, sharpe_ratio, sp500_correlation, max_drawdown,
        hhi, top5_concentration, avg_turnover, quarters_active, latest_aum,
        avg_confidence.

    Metrics requiring >= MIN_QUARTERS_FOR_METRICS quarters return None if insufficient data.
    """
    quarterly_returns = compute_quarterly_returns(conn, cik)

    quarters_active = len(quarterly_returns)
    returns_list = [qr["quarterly_return"] for qr in quarterly_returns]
    has_enough = quarters_active >= MIN_QUARTERS_FOR_METRICS

    # --- CAGR ---
    if has_enough and returns_list:
        product = reduce(lambda acc, r: acc * (1 + r), returns_list, 1.0)
        n = len(returns_list)
        annualized_return = product ** (4.0 / n) - 1.0
    else:
        annualized_return = None

    # --- Sharpe Ratio ---
    if has_enough and len(returns_list) >= 2:
        mean_r = statistics.mean(returns_list)
        std_r = statistics.stdev(returns_list)
        if std_r > 0:
            sharpe_ratio = (mean_r - RISK_FREE_RATE_QUARTERLY) / std_r * (4 ** 0.5)
        else:
            sharpe_ratio = None
    else:
        sharpe_ratio = None

    # --- S&P 500 Correlation ---
    if has_enough:
        fund_dates = {qr["report_date"]: qr["quarterly_return"] for qr in quarterly_returns}
        benchmark_rows = query_all(
            conn,
            """
            SELECT report_date, quarterly_return
            FROM v_benchmark_quarterly
            WHERE quarterly_return IS NOT NULL
            """,
        )
        benchmark_map = {r["report_date"]: r["quarterly_return"] for r in benchmark_rows}

        # Align on common dates
        common_dates = sorted(set(fund_dates.keys()) & set(benchmark_map.keys()))
        if len(common_dates) >= MIN_QUARTERS_FOR_METRICS:
            fund_vals = [fund_dates[d] for d in common_dates]
            bench_vals = [benchmark_map[d] for d in common_dates]
            sp500_correlation = statistics.correlation(fund_vals, bench_vals)
        else:
            sp500_correlation = None
    else:
        sp500_correlation = None

    # --- Max Drawdown ---
    if has_enough:
        cumulative = compute_cumulative_returns(quarterly_returns)
        cum_values = [c["cumulative_value"] for c in cumulative]
        max_drawdown = _compute_max_drawdown(cum_values)
    else:
        max_drawdown = None

    # --- Concentration (HHI, top-5) from latest quarter ---
    concentration = compute_concentration_metrics(conn, cik)
    hhi = concentration.get("hhi")
    top5_concentration = concentration.get("top5_concentration")

    # --- Turnover ---
    avg_turnover = compute_turnover(conn, cik)

    # --- Latest AUM ---
    if quarterly_returns:
        latest_aum = quarterly_returns[-1]["total_value"]
    else:
        # Fall back to direct query
        row = query_one(
            conn,
            """
            SELECT total_reported_value
            FROM v_portfolio_quarterly
            WHERE cik = ?
            ORDER BY report_date DESC
            LIMIT 1
            """,
            (cik,),
        )
        latest_aum = row["total_reported_value"] if row else None

    # --- Avg Confidence ---
    if quarterly_returns:
        avg_confidence = statistics.mean(
            qr["confidence"] for qr in quarterly_returns
        )
    else:
        avg_confidence = None

    return {
        "annualized_return": annualized_return,
        "sharpe_ratio": sharpe_ratio,
        "sp500_correlation": sp500_correlation,
        "max_drawdown": max_drawdown,
        "hhi": hhi,
        "top5_concentration": top5_concentration,
        "avg_turnover": avg_turnover,
        "quarters_active": quarters_active,
        "latest_aum": latest_aum,
        "avg_confidence": avg_confidence,
    }


def _compute_max_drawdown(cumulative_values: list[float]) -> float:
    """Compute max peak-to-trough drawdown from a cumulative return series.

    Returns a negative number (e.g., -0.20 for a 20% drawdown), or 0.0 if
    the series never declines.
    """
    if not cumulative_values:
        return 0.0

    peak = cumulative_values[0]
    max_dd = 0.0
    for val in cumulative_values:
        if val > peak:
            peak = val
        drawdown = (val - peak) / peak if peak > 0 else 0.0
        if drawdown < max_dd:
            max_dd = drawdown
    return max_dd
