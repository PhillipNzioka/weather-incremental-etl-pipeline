import os
from pathlib import Path
from dotenv import load_dotenv
from datetime import date

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent

DATABASE_URL = os.getenv("DATABASE_URL")

GEOCODE_URL = "https://geocoding-api.open-meteo.com/v1/search"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

BACKOFF_DELAYS = [1, 2, 4]

DAILY_VARIABLES = ["temperature_2m_max", "temperature_2m_min", "precipitation_sum", "rain_sum", "wind_speed_10m_max"]

START_DATE = date(2025,1,1)