"""Transform: reshape the API payload, validate every row, derive measures.

Pure functions only -- nothing here touches the network or the database, so it
is safe to call inside the load transaction and trivial to reason about.
"""

from datetime import date, timedelta
from collections import Counter
from decimal import ROUND_HALF_UP, Decimal

import config

REQUIRED = ("temp_max", "temp_min", "precipitation", "rain", "wind_max")


def _round(value):
    """Round half-up to config.DECIMALS places (not banker's rounding)."""
    quantum = Decimal(1).scaleb(-config.DECIMALS)
    return float(Decimal(str(value)).quantize(quantum, rounding=ROUND_HALF_UP))


def reshape(city_id, payload):
    """Turn the column-oriented `daily` block into one dict per day."""
    daily = payload.get("daily")
    if not isinstance(daily, dict) or not daily.get("time"):
        raise ValueError(f"City {city_id} has no daily data or missing 'time' key in payload.")

    # Keys must match config.DAILY_VARIABLES exactly, since extract.py asks the
    # API for those names.
    columns = {
        "temp_max": daily.get("temperature_2m_max") or [],
        "temp_min": daily.get("temperature_2m_min") or [],
        "precipitation": daily.get("precipitation_sum") or [],
        "rain": daily.get("rain_sum") or [],
        "wind_max": daily.get("wind_speed_10m_max") or [],
    }

    reshaped_data = []
    for index, date_str in enumerate(daily["time"]):
        row = {
            "city_id": city_id,
            "weather_date": date.fromisoformat(date_str),
        }
        for col_name, col_values in columns.items():
            if col_values is not None and index < len(col_values):
                row[col_name] = col_values[index]
            else:
                row[col_name] = None
        reshaped_data.append(row)
    return reshaped_data


def _reason(row):
    """Why a single row is bad, or None."""
    missing = [f for f in REQUIRED if row.get(f) is None]
    if missing:
        return f"missing required value: {', '.join(missing)}"
    if row["temp_max"] < row["temp_min"]:
        return f"max temperature {row['temp_max']} is below min temperature {row['temp_min']}"
    for field in ("temp_max", "temp_min"):
        if row[field] < config.TEMP_MIN_C or row[field] > config.TEMP_MAX_C:
            return (f"{field} {row[field]} is outside "
                    f"{config.TEMP_MIN_C} to {config.TEMP_MAX_C} C")
    if row["precipitation"] < 0:
        return f"negative precipitation {row['precipitation']}"
    return None


def validate(rows):
    """Split rows into (clean, rejected); every input row lands in exactly one list.

    If a (city, date) appears twice, both copies are rejected: neither can be trusted.
    """
    counts = Counter((r["city_id"], r["weather_date"]) for r in rows)

    clean, rejected = [], []
    for r in rows:
        reason = _reason(r)
        if reason is None and counts[(r["city_id"], r["weather_date"])] > 1:
            reason = "duplicate city and date in batch"
        if reason is None:
            clean.append(r)
        else:
            rejected.append({"city_id": r["city_id"], "weather_date": r["weather_date"],
                             "reason": reason, "payload": r})
    return clean, rejected


def derive(row):
    """Add the measures the fact table needs but the API does not return."""
    derived = dict(row)
    if row.get("temp_max") is not None and row.get("temp_min") is not None:
        derived["temp_avg"] = _round((row["temp_max"] + row["temp_min"]) / 2)

    derived["is_rainy_day"] = (
        row.get("precipitation") is not None
        and row["precipitation"] >= config.RAINY_DAY_MM
    )
    return derived


def season_for(month):
    if 3 <= month <= 5:
        return "Long rains"
    if 10 <= month <= 12:
        return "Short rains"
    return config.DRY_SEASON


def build_dim_date(start, end=None):
    """One dim_date row per day from `start`..`end` inclusive.

    With `end=None`, `start` is treated as an iterable of dates instead (handy
    for tests and ad-hoc backfills). Sorted and de-duplicated either way.
    """
    if end is None:
        days = [start] if isinstance(start, date) else sorted(set(start))
    else:
        days = [start + timedelta(days=offset) for offset in range((end - start).days + 1)]

    return [{
        "date": d,
        "year": d.year,
        "month": d.month,
        "month_name": d.strftime("%B"),
        "day_of_week": d.strftime("%A"),
        "is_weekend": d.weekday() >= 5,
        "season": season_for(d.month),
    } for d in days]


def transform_town(city_id, town, payload):
    """Full T stage for one town: (clean records, rejects ready for the log).

    Every input row lands in exactly one list, which is what makes the
    extracted == loaded + rejected reconciliation hold.
    """
    rows = reshape(city_id, payload)
    clean, rejected = validate(rows)

    records = [derive(row) for row in clean]
    rejects = [
        {
            "city_name": town,
            "weather_date": bad["weather_date"],
            "reason": bad["reason"],
            "payload": bad["payload"],
        }
        for bad in rejected
    ]
    return records, rejects
