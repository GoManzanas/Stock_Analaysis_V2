"""Project-wide configuration. All paths, API keys, and constants."""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# --- Paths ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
SEC_BULK_DIR = DATA_DIR / "sec_bulk"
DB_PATH = DATA_DIR / "fund_analyst.db"

# Ensure data directories exist
DATA_DIR.mkdir(exist_ok=True)
SEC_BULK_DIR.mkdir(exist_ok=True)

# --- EODHD API ---
EODHD_API_KEY = os.getenv("EODHD_API_KEY", "")
EODHD_BASE_URL = "https://eodhd.com/api"

# --- SEC ---
SEC_BULK_BASE_URL = (
    "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/"
)
SEC_USER_AGENT = "13F-Analyst research@example.com"

# --- Value cutover ---
# ZIPs from 2023Q1 onward have values in actual dollars.
# ZIPs from 2022Q4 and earlier have values in thousands of dollars.
# The SEC changed from thousands to actual dollars for filings after Jan 3, 2023.
# In practice, the 2023Q1 ZIP is the first with all actual-dollar values.
VALUE_CUTOVER_QUARTER = "2023Q1"  # First quarter with actual dollar values

# --- Rate limiting ---
EODHD_CALLS_PER_SECOND = 5
SEC_CALLS_PER_SECOND = 8

# --- Analytics ---
RISK_FREE_RATE_QUARTERLY = 0.01  # ~4% annualized
MIN_QUARTERS_FOR_METRICS = 4     # Minimum quarters to compute CAGR, Sharpe, etc.
