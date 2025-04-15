# config.py
import os
from dotenv import load_dotenv

load_dotenv()

# --- Supabase ---
SUPABASE_URL: str = os.getenv("NEXT_PUBLIC_SUPABASE_URL", "")
SUPABASE_ANON_KEY: str = os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY", "")
SUPABASE_TABLE_CARDS: str = "cards" # Your table with tcgplayer_id and set_name
SUPABASE_COLUMN_ID: str = "tcgplayer_id"
SUPABASE_COLUMN_SET: str = "set"

# --- Target Sets ---
# Define the specific sets you want to monitor. Can be empty list [] to monitor all (NOT RECOMMENDED FOR SCALE)
TARGET_SETS: list[str] = [
    "dft",
    "dfc",
    # "Modern Horizons 3", # Add sets you care about
    # "Bloomburrow"
]

# --- TCGPlayer API ---
TCGPLAYER_API_URL_TEMPLATE: str = "https://mpapi.tcgplayer.com/v2/product/{}/latestsales"
# Consider reducing if API response time becomes an issue with many concurrent requests
TCGPLAYER_PAGE_LIMIT: int = 25 # How many sales per API page request

# --- Fetching Control ---
MAX_CONCURRENT_REQUESTS: int = 50 # Tune based on performance and proxy stability
MAX_RETRIES_PER_ID: int = 1      # Overall attempts for an ID across different proxies/errors
FETCH_TIMEOUT_SECONDS: int = 30 # Timeout for a single HTTP request

# --- Proxy Configuration ---
PROXY_FILE: str = "nordvpnserver.json"
PROXY_PORT: int = 89
PROXY_USERNAME: str | None = os.getenv("PROXY_USERNAME")
PROXY_PASSWORD: str | None = os.getenv("PROXY_PASSWORD")
USE_PROXIES: bool = True # Set to False to disable proxy usage

# --- Database ---
DB_TYPE: str = "sqlite" # Could be "postgres" in the future
SQLITE_DB_NAME: str = "market_data.db"
DB_TABLE_SALES: str = "sales_records" # Name for the structured sales table
DB_TABLE_ANALYSIS: str = "daily_card_metrics" # Table to store analysis results

# --- Analysis Parameters ---
# How many days back to calculate baseline average price/volume
ANALYSIS_BASELINE_DAYS: int = 14
# Volume Spike: current > (multiplier * baseline_avg) OR current > absolute_min
ANALYSIS_VOLUME_SPIKE_MULTIPLIER: float = 1.5 # e.g., 3x the average volume
ANALYSIS_VOLUME_SPIKE_MIN_ABSOLUTE: int = 7 # e.g., ignore spikes below 10 sales/day
# Price Spike: current_avg > (multiplier * baseline_avg)
ANALYSIS_PRICE_SPIKE_MULTIPLIER: float = 1.5 # e.g., 50% price increase over average
# Consider Median Price Spike too
ANALYSIS_PRICE_MEDIAN_SPIKE_MULTIPLIER: float = 1.4

# Add other necessary constants
PROCESSING_CHUNK_SIZE = 100
PROXY_FILE = "datacenter_proxy.csv"