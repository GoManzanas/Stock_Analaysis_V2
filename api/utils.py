"""Shared API utilities."""

from fastapi import HTTPException


def quarter_to_date(q: str) -> str:
    """Convert quarter string (e.g. '2024Q4') to report_date ('2024-12-31')."""
    if len(q) < 5 or q[4] != "Q":
        raise HTTPException(status_code=400, detail=f"Invalid quarter format: {q}. Use YYYYQN (e.g. 2024Q4)")
    year = q[:4]
    quarter = q[5:]
    end_dates = {"1": "03-31", "2": "06-30", "3": "09-30", "4": "12-31"}
    if quarter not in end_dates:
        raise HTTPException(status_code=400, detail=f"Invalid quarter: {quarter}. Must be 1-4.")
    return f"{year}-{end_dates[quarter]}"
