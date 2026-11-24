"""
Entry point: E -> T -> L -> verify, one etl_run_log row per run.
"""

import logging
import sys
from datetime import date, timedelta

import requests

import config
import extract
import load
import transform

logger = logging.getLogger(__name__)


def configure_logging() -> None:
    """ Send INFO and above to logs/pipeline.log and to the console."""
    config.LOG_DIR.mkdir(parents=True, exist_ok=True)

    # One formatter shared by both handlers, so a line in the file and the
    # same line on the console are identical
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # the audit trail
    file_handler = logging.FileHandler(config.LOG_FILE, encoding="utf-8")
    file_handler.setFormatter(formatter)

    # the console is what you watch while running it by hand
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    # Configure the root logger once. basicConfig does nothing if handlers
    # already exist, which also keeps a second call from duplicating lines.
    logging.basicConfig(
        level=logging.INFO,
        handlers=[file_handler, console_handler],
    )


def window_for(last_date: date | None, today: date) -> tuple[date, date] | None:
    """Decide which dates to request for one town.

    end   = today - config.PUBLICATION_DELAY_DAYS   (never later)
    start = config.START_DATE            when last_date is None (backfill)
          = last_date + 1 day            otherwise

    Return None when start > end: there is nothing new yet, so skip the town
    and count zero rows rather than sending an inverted range to the API.
    Keeping this as a pure function makes it trivial to unit-test.
    """

    end = today - timedelta(days=config.PUBLICATION_DELAY_DAYS)

    """
    First run: no watermark, so backfill from the configured start.
    Later runs: the watermark already holds the last stored day, and
    "+1" is what makes this incremental instead of a re-download.
    """
    start = config.START_DATE if last_date is None else last_date + timedelta(days=1)

    if start > end:
        return None

    return start, end


def main(argv: list[str] | None = None) -> int:
    """Run the whole pipeline and return a process exit code (0 ok, 1 failed).
    """
    # argv exists so main() is callable from tests as main([]); there are no
    # flags to parse yet.
    del argv

    # 1. Logging first: everything below this line must be visible in the file
    #    even if the very next step fails.
    configure_logging()
    today = date.today()
    logger.info("pipeline start (today=%s)", today)

    try:
        conn = load.get_connection()
    except Exception:
        logger.exception("could not open a database connection; run not recorded")
        return 1

    run_id = None
    extracted = loaded = rejected = 0

    try:
        load.create_tables(conn)
        run_id = load.start_run(conn)
        cities: list[dict] = []
        for town in config.TOWNS:
            city = extract.get_geocode(town)
            if city is None:
                logger.warning("%s: no geocoding result, skipping this town", town)
                continue
            cities.append(city)

        if not cities:
            raise RuntimeError("no town could be geocoded; aborting the run")

        # Dims before facts, as the brief requires: the city keys every fact row points at.
        load.upsert_cities(conn, cities)

        #Per-town E -> T -> L, still inside the one data transaction.
        for city in cities:
            city_id = city["city_id"]
            town = city["name"]

            last_date = load.get_watermark(conn, city_id)
            window = window_for(last_date, today)
            if window is None:
                logger.info("%s: nothing new since %s, skipped", town, last_date)
                continue

            start, end = window
            logger.info("%s: fetching %s..%s", town, start, end)

            # Extract. get_json has already retried; a RequestException here
            # means every attempt failed (network, timeout, 5xx/429). That is a
            # source problem, not a bug: skip the town, keep the run.
            try:
                payload = extract.fetch_weather(
                    city["latitude"], city["longitude"], start, end
                )
            except requests.RequestException as exc:
                logger.warning("%s: fetch failed after retries, skipped: %s", town, exc)
                continue

            records, rejects = transform.transform_town(city_id, town, payload)

            # The run_id travels inside each row so insert_rejected keeps its
            # simple (conn, rows) signature.
            for row in rejects:
                row["run_id"] = run_id

            # dim_date must cover every fact date (FK), so build the calendar
            # for the window we just asked for. DO NOTHING makes this free on
            # the second run.
            load.upsert_dates(conn, transform.build_dim_date(start, end))
            written = load.upsert_facts(conn, records)
            load.insert_rejected(conn, rejects)

            # Advance the watermark only to a day we actually stored. If every
            # row was quarantined, leave it alone so the days are requested
            # again next run instead of being silently skipped forever.
            if records:
                last_loaded = max(record["weather_date"] for record in records)
                load.set_watermark(conn, city_id, last_loaded)

            # Reconcile rows per town
            town_extracted = len(records) + len(rejects)
            if town_extracted != written + len(rejects):
                logger.error(
                    "%s: reconciliation mismatch extracted=%d loaded=%d rejected=%d",
                    town,
                    town_extracted,
                    written,
                    len(rejects),
                )
            else:
                logger.info(
                    "%s: extracted=%d loaded=%d rejected=%d",
                    town,
                    town_extracted,
                    written,
                    len(rejects),
                )

            extracted += town_extracted
            loaded += written
            rejected += len(rejects)

        #  One commit for all towns. If anything above raised, none of this
        #  is written and the watermarks stay where they were, so the next
        #  run re-fetches exactly the same window.
        conn.commit()
        load.finish_run(conn, run_id, "success", extracted, loaded, rejected)
        logger.info(
            "pipeline finished: extracted=%d loaded=%d rejected=%d",
            extracted,
            loaded,
            rejected,
        )
        return 0

    except Exception as exc:
        # Undo the data transaction, keep the evidence, then report failure.
        try:
            conn.rollback()
        except Exception:
            logger.exception("rollback failed; the connection is probably gone")
        logger.exception("pipeline failed")

        if run_id is not None:
            try:
                load.finish_run(
                    conn,
                    run_id,
                    "failed",
                    extracted,
                    loaded,
                    rejected,
                    f"{type(exc).__name__}: {exc}",
                )
            except Exception:
                # Never mask the original error with a bookkeeping error.
                logger.exception("could not record run %s as failed", run_id)
        return 1

    finally:
        try:
            conn.close()
        except Exception:
            logger.debug("closing the connection failed", exc_info=True)


if __name__ == "__main__":
    sys.exit(main())
