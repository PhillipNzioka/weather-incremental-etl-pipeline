# weather-incremental-etl-pipeline

An incremental ETL pipeline that pulls daily historical weather for five Kenyan
cities from the [Open-Meteo](https://open-meteo.com/) API and loads it into a
PostgreSQL star schema.

Each run:

- geocodes the towns (id, coordinates) and refreshes `dim_city`;
- asks only for the days that are missing, using a per-city watermark;
- validates every row and quarantines bad ones in `rejected_records`;
- derives the measures Open-Meteo does not return (`temp_avg`, `is_rainy_day`);
- loads facts and the calendar, then advances the watermark;
- writes one `etl_run_log` row with counts and status.

The first run backfills everything since `config.START_DATE`. Every later run
fetches only the new days.

---

## How it works

### Layout

```
weather_etl/
├── pipeline.py           # entry point: orchestration, windowing, logging
├── extract.py            # Open-Meteo geocoding + archive calls (retries)
├── transform.py          # pure functions: reshape, validate, derive, dim_date
├── load.py               # psycopg2: DDL, UPSERTs, watermarks, run log, rejects
├── config.py             # all settings (towns, dates, thresholds, HTTP tuning)
├── sql/create_tables.sql # idempotent star schema + control tables
├── logs/pipeline.log     # append-only run log (created on first run)
├── requirements.txt
└── .env                  # DATABASE_URL (not committed)
```

### The stages

#### 1. Startup and schema (`pipeline.py`, `load.create_tables`)

Logging goes to both `logs/pipeline.log` and stdout. The pipeline opens a
connection and applies `sql/create_tables.sql`, which is idempotent
(`CREATE TABLE IF NOT EXISTS`), and commits it on its own. So the schema exists
before the pipeline touches `etl_run_log`, and it survives a failed load.

#### 2. Run row (`load.start_run`)

The pipeline inserts an `etl_run_log` row with `status = 'running'` and commits
it at once, so a failed data transaction still leaves a record of the attempt.

#### 3. Extract (`extract.py`)

- `get_geocode(town)` searches Open-Meteo geocoding, filtered to
  `config.COUNTRY_CODE`, and returns a `dim_city`-shaped dict. It returns `None`
  when the town has no hit, and the pipeline skips that town with a warning.
- `fetch_weather(lat, lon, start, end)` requests the daily archive for an
  inclusive date window and the variables in `config.DAILY_VARIABLES`.
- `get_json()` wraps every request. Each attempt waits up to
  `config.REQUEST_TIMEOUT`, and the function makes up to `config.MAX_RETRIES`
  attempts with `config.BACKOFF_SECONDS` between them. It re-raises the last
  `RequestException` so the caller can skip a town instead of aborting the run.

#### 4. Incremental window (`pipeline.window_for`)

For each city:

```
end   = today - config.PUBLICATION_DELAY_DAYS      # archive lag, never later
start = config.START_DATE     if no watermark      # first run: backfill
      = watermark + 1 day     otherwise            # later runs: only new days
```

If `start > end`, nothing is new yet, and the pipeline skips the town without
calling the API. The delay exists because the archive does not publish the last
few days.

#### 5. Transform (`transform.py`, pure: no network, no DB)

- `reshape()` zips the column-oriented `daily` block into one dict per day.
- `validate()` splits rows into clean and rejected, with a reason for each:
  a missing required value, `temp_max < temp_min`, a temperature outside
  `TEMP_MIN_C..TEMP_MAX_C`, negative precipitation, or a duplicate
  `(city, date)` inside the batch (it rejects both copies).
- `derive()` adds `temp_avg` (half-up rounded to `config.DECIMALS`) and
  `is_rainy_day` (`precipitation >= config.RAINY_DAY_MM`).
- `build_dim_date(start, end)` builds the calendar rows for the requested
  window, so every fact date has a `dim_date` parent (foreign key).
- `transform_town()` runs all of the above and returns `(records, rejects)`.
  Every input row lands in exactly one list, which keeps the
  `extracted = loaded + rejected` reconciliation true.

Bad rows never abort a run. The pipeline writes them to `rejected_records` with
the original payload as JSONB for debugging.

#### 6. Load (`load.py`)

- `upsert_cities`: `ON CONFLICT (city_id) DO UPDATE`, because re-geocoding can
  change coordinates or population.
- `upsert_dates`: `ON CONFLICT DO NOTHING`, because a calendar date never
  changes.
- `upsert_facts`: `ON CONFLICT (city_id, weather_date) DO UPDATE`, so reruns
  and late revisions are safe and idempotent.
- `insert_rejected`: a plain insert, because every rejection is a kept event.
- `set_watermark`: moves forward only to the newest day it stored. If the
  pipeline quarantined every row, the watermark stays put and the next run
  requests those days again.

#### 7. Commit policy

Dimension and fact rows for all towns go in one data transaction, committed
once after the loops. If anything raises, the whole transaction rolls back and
the watermarks stay put, so the next run re-fetches exactly the same window.
`create_tables`, `start_run` and `finish_run` commit separately, so the
bookkeeping survives that rollback.

#### 8. Finish

On success, the pipeline closes the run row with `status = 'success'` and the
totals. On failure, it rolls back the data transaction, writes the error
(truncated to 2000 chars) to the run row as `failed`, and exits with code `1`.

### Schema

| Table | Purpose |
| --- | --- |
| `dim_city` | One row per town: geocoding id, name, county, lat/lon, elevation, population, timezone |
| `dim_date` | Calendar: year, month, month name, weekday, weekend flag, season (long rains / short rains / dry) |
| `fact_daily_weather` | Grain: one row per city per day. `temp_max`, `temp_min`, `temp_avg`, `precipitation`, `rain`, `wind_max`, `is_rainy_day` |
| `etl_run_log` | One row per run: status, timings, extracted/loaded/rejected, error message |
| `etl_watermark` | `last_date` loaded per city. This is what makes the run incremental |
| `rejected_records` | Quarantined rows with reason, original payload (JSONB) and run id |

---

## Running it

### Prerequisites

- Python 3.10 or newer (the code uses `X | None` type hints)
- A PostgreSQL database reachable over the network. The project was developed
  against [Aiven](https://aiven.io/) PostgreSQL, which requires
  `sslmode=require` in the connection URL.

### 1. Create a virtual environment and install dependencies

Windows (PowerShell / cmd):

```bat
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

macOS / Linux:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

`requirements.txt` lists three packages: `requests`, `psycopg2-binary`,
`python-dotenv`.

### 2. Configure the database URL

Create `.env` in the project root, next to `config.py`:

```
DATABASE_URL=postgresql://<user>:<password>@<host>:<port>/<database>?sslmode=require
```

`config.py` loads this file with `python-dotenv`. `load.get_connection()` reads
`DATABASE_URL` at call time and raises a clear error if it is missing. The
password never appears in code or logs. Keep `.env` out of version control, and
URL-encode special characters in the password.

### 3. Run the pipeline

```bash
python pipeline.py
```

Windows without activating the venv:

```bat
.venv\Scripts\python.exe pipeline.py
```

Exit codes: `0` for success, `1` for failure (a connection error, or an
exception during the run). Details are in `logs/pipeline.log` and
`etl_run_log.error_message`.

Console and log output look like this:

```
2026-11-24 12:41:03 INFO     __main__: pipeline start (today=2026-11-24)
2026-11-24 12:41:03 INFO     load: schema ready (create_tables.sql applied)
2026-11-24 12:41:04 INFO     load: run 1 started
2026-11-24 12:41:05 INFO     load: upserted 5 dim_city rows
2026-11-24 12:41:06 INFO     __main__: Nairobi: fetching 2025-01-01..2026-11-19
2026-11-24 12:41:09 INFO     __main__: Nairobi: extracted=688 loaded=688 rejected=0
...
2026-11-24 12:41:20 INFO     load: run 1 finished: status=success extracted=3440 loaded=3440 rejected=0
2026-11-24 12:41:20 INFO     __main__: pipeline finished: extracted=3440 loaded=3440 rejected=0
```

### 4. Rerun it

You can run it again at any time. Each town starts at `watermark + 1`, and if
nothing is newer than `today - PUBLICATION_DELAY_DAYS`, the pipeline logs:

```
Nairobi: nothing new since 2026-11-19, skipped
```

To schedule it, point any scheduler (cron, Task Scheduler, a CI job) at
`python pipeline.py` once a day, after the archive publishes.

---

## Configuration (`config.py`)

| Setting | Meaning |
| --- | --- |
| `TOWNS`, `COUNTRY_CODE` | Cities to load; geocoding filtered by country |
| `START_DATE` | Backfill start for the very first run |
| `PUBLICATION_DELAY_DAYS` | Archive lag; last requestable day is `today - this` |
| `DAILY_VARIABLES` | Open-Meteo daily fields requested |
| `TIMEZONE` | Requested timezone (`Africa/Nairobi`) |
| `REQUEST_TIMEOUT`, `MAX_RETRIES`, `BACKOFF_SECONDS` | HTTP patience per request |
| `DECIMALS` | Rounding used for `temp_avg` |
| `TEMP_MIN_C`, `TEMP_MAX_C` | Valid temperature range for the quality gate |
| `RAINY_DAY_MM` | Precipitation threshold for `is_rainy_day` |
| `LONG_RAINS_MONTHS`, `SHORT_RAINS_MONTHS`, `DRY_SEASON` | Season labels in `dim_date` |

To add a town, add one line to `TOWNS`. The next run geocodes and backfills it.

---

## Troubleshooting

`DATABASE_URL is not set`: `.env` is missing or in the wrong directory. It must
sit next to `config.py`.

Connection or SSL errors: check the host and port, and check that
`sslmode=require` is in the URL (Aiven rejects connections without it).

`no town could be geocoded; aborting the run`: the geocoding API was
unreachable or returned nothing for every town. The pipeline records the run as
`failed` and writes no data.

A request fails after retries: the pipeline skips only that town, with a
`fetch failed after retries` warning, and the other towns still load. The
skipped town's watermark does not move, so the next run picks up the same
window.

Rows landed in `rejected_records`: read `reason` there. The original API row is
in `payload`.

Some useful queries:

```sql
SELECT run_id, started_at, finished_at, status, rows_loaded, rows_rejected
FROM etl_run_log ORDER BY run_id DESC LIMIT 5;

SELECT * FROM rejected_records ORDER BY rejected_at DESC LIMIT 20;

SELECT d.year, d.month, c.name,
       round(avg(f.temp_avg)::numeric, 1) AS avg_temp,
       count(*) FILTER (WHERE f.is_rainy_day) AS rainy_days
FROM fact_daily_weather f
JOIN dim_city c USING (city_id)
JOIN dim_date d ON d.date = f.weather_date
GROUP BY d.year, d.month, c.name
ORDER BY d.year, d.month, c.name;

SELECT c.name, w.last_date
FROM etl_watermark w JOIN dim_city c USING (city_id)
ORDER BY c.name;
```

---

## Design notes

- Every step is idempotent. DDL is `IF NOT EXISTS`, facts upsert on
  `(city_id, weather_date)`, and calendar rows are `DO NOTHING`. You can
  re-request any window without duplicating data.
- The watermark moves only when rows are stored, so a batch that was entirely
  rejected gets retried instead of silently skipped.
- Each run uses one transaction. A run never half-finishes: either every town's
  data and watermark advance, or none does.
- The schema has no `CHECK` constraints on temperatures. One bad row would
  abort the whole transaction, so the quality gate in `transform.py` quarantines
  it instead.
- Extract, transform and orchestration stay separable. `transform.py` is pure,
  so you can test it without a database or network. `pipeline.window_for` is
  also pure, so you can test the incremental logic on its own.

## Data source

Weather data comes from the [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api)
(archive) and the [Geocoding API](https://open-meteo.com/en/docs/geocoding-api).
Both are free for non-commercial use. Check their terms before deploying
commercially.