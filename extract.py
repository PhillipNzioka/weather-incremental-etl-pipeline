"""Extract: Open-Meteo geocoding + archive API, with timeout and retries."""

import logging
import time
from datetime import date

import requests

import config

logger = logging.getLogger(__name__)


def get_json(url: str, params: dict | None = None, timeout: float | None = None) -> dict:
    """Fetch JSON with a timeout and exponential backoff.

    Tries config.MAX_RETRIES times (waits 1s, 2s, 4s...). Re-raises the last
    RequestException once every attempt has failed, so the caller can decide
    whether to skip the town or abort.
    """
    delays = config.BACKOFF_SECONDS
    timeout = config.REQUEST_TIMEOUT if timeout is None else timeout
    attempts = max(config.MAX_RETRIES, 1)
    last_error: Exception | None = None

    for attempt in range(attempts):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            last_error = exc
            if attempt < attempts - 1:
                delay = delays[min(attempt, len(delays) - 1)]
                logger.warning(
                    "attempt %d/%d failed: %s. Retrying in %ss",
                    attempt + 1, attempts, exc, delay,
                )
                time.sleep(delay)
            else:
                logger.error("attempt %d/%d failed: %s", attempt + 1, attempts, exc)

    raise last_error


def get_geocode(town: str) -> dict | None:
    """Look a town up; return a dim_city-ready dict, or None if there is no hit."""
    params = {
        "name": town,
        "countryCode": config.COUNTRY_CODE,
        "count": 1,
    }

    data = get_json(config.GEOCODING_URL, params)
    results = data.get("results")
    if not results:
        return None

    hit = results[0]
    return {
        "city_id": hit["id"],
        "name": hit["name"],
        "county": hit.get("admin1"),
        "latitude": hit["latitude"],
        "longitude": hit["longitude"],
        "elevation": hit.get("elevation"),
        "population": hit.get("population"),
        "timezone": hit.get("timezone"),
    }


def fetch_weather(latitude: float, longitude: float, start_date: date, end_date: date) -> dict:
    """Fetch the daily archive for one point and inclusive date window."""
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "daily": ",".join(config.DAILY_VARIABLES),
        "timezone": config.TIMEZONE,
    }
    return get_json(config.ARCHIVE_URL, params)
