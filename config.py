"""Central configuration for the Kenya City Weather Pipeline.

Everything that is a *setting* lives here so the other modules stay focused on
behaviour. The database password is NOT a setting: it comes from the
environment through `.env` and never appears in this file or in the logs.
"""

from datetime import date
from pathlib import Path
import os

from dotenv import load_dotenv

# --- Paths -----------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
SQL_DIR = BASE_DIR / "sql"
LOG_DIR = BASE_DIR / "logs"
LOG_FILE = LOG_DIR / "pipeline.log"

# Loads kenya_weather_pipeline/.env into os.environ (no-op if the file is absent).
load_dotenv(BASE_DIR / ".env")

# --- Credentials -----------------------------------------------------------
# Example: postgresql://avnadmin:<password>@<host>:<port>/defaultdb?sslmode=require
# Aiven requires sslmode=require. Validate it in load.get_connection().
DATABASE_URL = os.getenv("DATABASE_URL")

# --- Towns -----------------------------------------------------------------
TOWNS = ["Nairobi", "Mombasa", "Kisumu", "Nakuru", "Eldoret"]
COUNTRY_CODE = "KE"  # geocoding is filtered to Kenya

# --- Open-Meteo endpoints --------------------------------------------------
GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

# The archive returns data by column; transform.py zips these lists into rows.
DAILY_VARIABLES = [
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "rain_sum",
    "wind_speed_10m_max",
]
TIMEZONE = "Africa/Nairobi"

# --- Incremental window ----------------------------------------------------
# First run backfills from here; later runs start at watermark + 1 day.
START_DATE = date(2025, 1, 1)

# Historical weather is published with roughly a 5-day delay, so the last
# requestable day is always today - PUBLICATION_DELAY_DAYS.
PUBLICATION_DELAY_DAYS = 5

# --- HTTP ------------------------------------------------------------------
REQUEST_TIMEOUT = 30  # seconds per request
MAX_RETRIES = 3
BACKOFF_SECONDS = [1, 2, 4]  # waits between attempts

# --- Derived values --------------------------------------------------------
# Rounds temp_avg to the precision of the NUMERIC(4,1) columns in sql/.
DECIMALS = 1

# --- Data quality rules (mirrored by transform._reason) --------------------
TEMP_MIN_C = -5.0
TEMP_MAX_C = 50.0
RAINY_DAY_MM = 1.0  # is_rainy_day when precipitation >= 1.0 mm

# --- Seasons ---------------------------------------------------------------
LONG_RAINS_MONTHS = (3, 4, 5)
SHORT_RAINS_MONTHS = (10, 11, 12)
DRY_SEASON = "Dry season"
