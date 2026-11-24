"""Load: schema creation, UPSERTs, watermarks, run log and rejects.

Everything in here talks to PostgreSQL through psycopg2. The pipeline opens
one connection, runs the data load in a single transaction, and commits only
after the watermark update is part of that same transaction.

Commit policy, because it is easy to get wrong and impossible to see later:
  * create_tables()         commits  -> the schema must survive a failed load;
  * start_run()/finish_run() commits -> run evidence on the run-log connection;
  * every upsert_*()/set_watermark()/insert_rejected() does NOT commit -> they
    all ride in the one data transaction that pipeline.py ends with commit().
"""

import logging
from datetime import date
from typing import Any, Iterable

import psycopg2
from psycopg2 import extras
from psycopg2.extensions import connection as Connection

# Import the module, then read attributes at call time (config.DATABASE_URL).
# `from config import DATABASE_URL` would copy the value at import time, so a
# password added to .env after startup would never be seen.
import config

logger = logging.getLogger(__name__)

# Keep the column readable: one bad run should not bloat etl_run_log.
MAX_ERROR_CHARS = 2000


def get_connection() -> Connection:
    """Open a connection from DATABASE_URL.

    Fail with a clear message when DATABASE_URL is unset, so a missing .env
    looks like a configuration problem instead of a confusing driver error.
    `psycopg2.connect(DATABASE_URL)` is enough; sslmode travels in the URL.
    """
    if not config.DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL is not set. Check your .env file and the README for setup instructions."
        )

    try:
        conn = psycopg2.connect(config.DATABASE_URL)
    except psycopg2.OperationalError as exc:
        raise RuntimeError(f"could not connect to PostgreSQL: {exc}") from exc

    logger.debug("connected to PostgreSQL")
    return conn


def create_tables(conn: Connection) -> None:
    """Execute sql/create_tables.sql.

    Read the file once (`SQL_DIR / "create_tables.sql"`), execute it, and
    commit. The DDL is idempotent (CREATE TABLE IF NOT EXISTS), so this can
    run at the start of every pipeline run.
    """
    # Read the file every run rather than caching the SQL in Python: the
    # schema lives in sql/ and stays the single source of truth.
    ddl = (config.SQL_DIR / "create_tables.sql").read_text(encoding="utf-8")

    with conn.cursor() as cur:
        cur.execute(ddl)

    # Commit the DDL on its own. If a later data load rolls back, the
    # tables, and therefore the place to record the failure, still exist.
    conn.commit()
    logger.info("schema ready (create_tables.sql applied)")


def upsert_cities(conn: Connection, cities: Iterable[dict[str, Any]]) -> None:
    """UPSERT dim_city rows; a re-geocode may change population or county.

    ON CONFLICT (city_id) DO UPDATE ... so a town's stored coordinates and
    population follow the API instead of going stale.
    """
    # Turn the dicts into positional tuples once. execute_values works on
    # sequences, and dict key order must never decide column order.
    rows = [
        (
            city["city_id"],
            city["name"],
            city.get("county"),
            city["latitude"],
            city["longitude"],
            city.get("elevation"),
            city.get("population"),
            city.get("timezone"),
        )
        for city in cities
    ]
    if not rows:
        return

    # DO UPDATE: geocoding answers can change, and a city whose coordinates 
    # drifted would silently keep writing weather for the old point.
    # loaded_at = now() records when we last refreshed it.
    with conn.cursor() as cur:
        extras.execute_values(
            cur,
            """
            INSERT INTO dim_city
                (city_id, name, county, latitude, longitude, elevation,
                 population, timezone)
            VALUES %s
            ON CONFLICT (city_id) DO UPDATE SET
                name       = EXCLUDED.name,
                county     = EXCLUDED.county,
                latitude   = EXCLUDED.latitude,
                longitude  = EXCLUDED.longitude,
                elevation  = EXCLUDED.elevation,
                population = EXCLUDED.population,
                timezone   = EXCLUDED.timezone,
                loaded_at  = now()
            """,
            rows,
            page_size=len(rows),
        )
    # No commit: dims are part of the same transaction as the facts.
    logger.info("upserted %d dim_city rows", len(rows))


def upsert_dates(conn: Connection, dates: Iterable[dict[str, Any]]) -> None:
    """UPSERT dim_date rows. The calendar never changes, so DO NOTHING is fine."""
    rows = [
        (
            day["date"],
            day["year"],
            day["month"],
            day["month_name"],
            day["day_of_week"],
            day["is_weekend"],
            day["season"],
        )
        for day in dates
    ]
    if not rows:
        return

    # DO NOTHING because a date is immutable: 2026-10-04 is always a
    # Sunday. Overwriting it would only risk a wrong label beating a right
    # one. Rerunning the same window is therefore free.
    with conn.cursor() as cur:
        extras.execute_values(
            cur,
            """
            INSERT INTO dim_date
                (date, year, month, month_name, day_of_week, is_weekend, season)
            VALUES %s
            ON CONFLICT (date) DO NOTHING
            """,
            rows,
            page_size=len(rows),
        )
    logger.info("upserted %d dim_date rows", len(rows))


def upsert_facts(conn: Connection, facts: Iterable[dict[str, Any]]) -> int:
    """UPSERT fact_daily_weather on the composite key; return rows written.

    ON CONFLICT (city_id, weather_date) DO UPDATE SET ... every measure plus
    loaded_at. That is what makes a rerun safe: same day, same key, no new
    row -- and a late revision of an old day overwrites instead of failing.

    Use psycopg2.extras.execute_values for a batch insert; a loop of
    execute() calls is fine while you are learning but much slower.
    """
    # 1. Facts must already carry temp_avg and is_rainy_day: this module stores
    #    what transform produced, it does not compute.
    rows = [
        (
            fact["city_id"],
            fact["weather_date"],
            fact["temp_max"],
            fact["temp_min"],
            fact["temp_avg"],
            fact["precipitation"],
            fact.get("rain"),
            fact.get("wind_max"),
            fact["is_rainy_day"],
        )
        for fact in facts
    ]
    if not rows:
        return 0

    with conn.cursor() as cur:
        extras.execute_values(
            cur,
            """
            INSERT INTO fact_daily_weather
                (city_id, weather_date, temp_max, temp_min, temp_avg,
                 precipitation, rain, wind_max, is_rainy_day)
            VALUES %s
            ON CONFLICT (city_id, weather_date) DO UPDATE SET
                temp_max      = EXCLUDED.temp_max,
                temp_min      = EXCLUDED.temp_min,
                temp_avg      = EXCLUDED.temp_avg,
                precipitation = EXCLUDED.precipitation,
                rain          = EXCLUDED.rain,
                wind_max      = EXCLUDED.wind_max,
                is_rainy_day  = EXCLUDED.is_rainy_day,
                loaded_at     = now()
            """,
            rows,
            page_size=len(rows),
        )
        written = cur.rowcount

    # No commit -- the watermark update must land in the same transaction.
    logger.info("upserted %d fact_daily_weather rows", written)
    return written


def get_watermark(conn: Connection, city_id: int) -> date | None:
    """Return the town's last loaded date, or None on its very first run."""
    # Parameterised query: city_id is data, never string-formatted into SQL.
    with conn.cursor() as cur:
        cur.execute(
            "SELECT last_date FROM etl_watermark WHERE city_id = %s",
            (city_id,),
        )
        row = cur.fetchone()

    # No row means "never loaded", which pipeline.window_for() turns into the
    # full backfill window. None is the signal; 0 rows is not an error.
    return row[0] if row else None


def set_watermark(conn: Connection, city_id: int, last_date: date) -> None:
    """Move the watermark forward -- inside the data transaction.

    ON CONFLICT (city_id) DO UPDATE. 
    """
    # The INSERT branch covers a town's first successful run; the UPDATE
    # branch covers every run after it
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO etl_watermark (city_id, last_date)
            VALUES (%s, %s)
            ON CONFLICT (city_id) DO UPDATE SET
                last_date  = EXCLUDED.last_date,
                updated_at = now()
            """,
            (city_id, last_date),
        )
    logger.info("watermark for city_id=%s -> %s", city_id, last_date)


def insert_rejected(conn: Connection, rows: Iterable[dict[str, Any]]) -> int:
    """Insert quarantined rows into rejected_records; return how many.

    These are written in the same transaction as the good rows so the
    reconciliation (extracted = loaded + rejected) always adds up.
    """
    params = [
        (
            row.get("city_name"),
            row.get("weather_date"),
            row["reason"],
            extras.Json(row["payload"]) if row.get("payload") is not None else None,
            row.get("run_id"),
        )
        for row in rows
    ]
    if not params:
        return 0

    # Plain INSERT, not an upsert: every rejection is an event worth
    #  keeping, including the same bad record seen on two different runs
    with conn.cursor() as cur:
        extras.execute_values(
            cur,
            """
            INSERT INTO rejected_records
                (city_name, weather_date, reason, payload, run_id)
            VALUES %s
            """,
            params,
            page_size=len(params),
        )
        written = cur.rowcount

    logger.info("inserted %d rejected_records rows", written)
    return written


def start_run(conn: Connection) -> int:
    """Insert an etl_run_log row with status 'running' and return run_id.

    Do this on a *separate, already-committed* connection/transaction, so a
    later rollback of the data transaction does not erase the evidence of
    the run that failed. Same for finish_run().
    """
    # 1. RETURNING gives the generated key in the same round trip; no
    #    lastval()/currval() guesswork.
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO etl_run_log (status) VALUES ('running') RETURNING run_id"
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    logger.info("run %s started", run_id)
    return run_id


def finish_run(
    conn: Connection,
    run_id: int,
    status: str,
    extracted: int = 0,
    loaded: int = 0,
    rejected: int = 0,
    error_message: str | None = None,
) -> None:
    """Close the run row: finished_at, counts, status, error text.

    status is 'success' or 'failed' (the table has a CHECK constraint).
    Truncate very long error messages so one bad run cannot bloat the table.
    """
    # Truncate defensively. An empty string is stored as NULL so queries
    # can filter `WHERE error_message IS NOT NULL` and get real failures.
    message = (error_message or "").strip()[:MAX_ERROR_CHARS] or None

    # One UPDATE closes the row: it is already committed, so this survives
    # whether the data transaction committed or rolled back.
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE etl_run_log
               SET finished_at    = now(),
                   status         = %s,
                   rows_extracted = %s,
                   rows_loaded    = %s,
                   rows_rejected  = %s,
                   error_message  = %s
             WHERE run_id = %s
            """,
            (status, extracted, loaded, rejected, message, run_id),
        )

    conn.commit()
    logger.info(
        "run %s finished: status=%s extracted=%d loaded=%d rejected=%d",
        run_id,
        status,
        extracted,
        loaded,
        rejected,
    )
