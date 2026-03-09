"""Pydantic response schemas for the API."""

from typing import Generic, TypeVar

from pydantic import BaseModel

T = TypeVar("T")


class PaginatedResponse(BaseModel, Generic[T]):
    items: list[T]
    total: int
    page: int
    page_size: int


class StatsResponse(BaseModel):
    total_filers: int
    total_filings: int
    total_holdings: int
    total_securities: int
    resolved_securities: int
    unresolved_securities: int
    total_prices: int
    total_corporate_actions: int
    total_audit_results: int
    cache_stats: dict


class FundSummary(BaseModel):
    cik: str
    name: str | None = None
    annualized_return: float | None = None
    sharpe_ratio: float | None = None
    sp500_correlation: float | None = None
    max_drawdown: float | None = None
    hhi: float | None = None
    top5_concentration: float | None = None
    avg_turnover: float | None = None
    quarters_active: int | None = None
    latest_aum: float | None = None
    avg_confidence: float | None = None


class FundDetail(FundSummary):
    address: str | None = None
    first_report_date: str | None = None
    last_report_date: str | None = None
    filing_count: int | None = None
    position_count: int | None = None


class QuarterlyReturn(BaseModel):
    cik: str
    report_date: str
    quarterly_return: float
    confidence: float
    position_count: int
    total_value: float | None = None
    cumulative_value: float | None = None


class HoldingItem(BaseModel):
    cusip: str
    issuer_name: str | None = None
    ticker: str | None = None
    value: float | None = None
    shares: float | None = None
    weight: float | None = None
    put_call: str | None = None
    computed_value: float | None = None


class HoldingDiffItem(BaseModel):
    cusip: str
    issuer_name: str | None = None
    ticker: str | None = None
    status: str  # "added", "removed", "changed", "unchanged"
    q1_shares: float | None = None
    q2_shares: float | None = None
    q1_value: float | None = None
    q2_value: float | None = None
    shares_change: float | None = None
    value_change: float | None = None


class FilingItem(BaseModel):
    id: int
    cik: str
    accession_number: str
    filing_date: str | None = None
    report_date: str | None = None
    report_year: int | None = None
    report_quarter: int | None = None
    form_type: str | None = None
    amendment_type: str | None = None
    total_value: float | None = None
    holding_count: int | None = None


class SecurityInfo(BaseModel):
    cusip: str
    ticker: str | None = None
    name: str | None = None
    exchange: str | None = None
    is_active: bool | None = None
    resolution_confidence: float | None = None


class SecurityHolder(BaseModel):
    cik: str
    name: str | None = None
    shares: float | None = None
    value: float | None = None
    weight: float | None = None


class PricePoint(BaseModel):
    date: str
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    adj_close: float | None = None
    volume: int | None = None


class SecurityHolderHistoryPoint(BaseModel):
    report_date: str
    quarter: str
    holder_count: int
    total_shares: float
    total_value: float


class SecuritySearchResult(BaseModel):
    cusip: str
    ticker: str | None = None
    name: str | None = None
    exchange: str | None = None


class BenchmarkComparison(BaseModel):
    date: str
    ticker_close: float | None = None
    ticker_adj_close: float | None = None
    benchmark_close: float | None = None
    benchmark_adj_close: float | None = None


class PositionHistoryPoint(BaseModel):
    report_date: str
    quarter: str
    shares: float | None = None
    value: float | None = None
    weight: float | None = None
    price: float | None = None


class ScreenerPreset(BaseModel):
    name: str
    description: str
    filters: dict
    sort_by: str
