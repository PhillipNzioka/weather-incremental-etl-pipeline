-- Kenya City Weather Pipeline -- star schema + control tables.
-- Idempotent: safe to run on every pipeline start.
-- Note: quality rules live in transform.py (bad rows go to rejected_records).
-- Deliberately no CHECK constraints on temperatures here: a CHECK would abort
-- the whole transaction instead of quarantining the row.

-- ---------------------------------------------------------------------------
-- Dimensions
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dim_city (
    city_id     INTEGER PRIMARY KEY,           -- Open-Meteo geocoding id
    name        TEXT NOT NULL UNIQUE,
    county      TEXT,                          -- geocoding admin1
    latitude    DOUBLE PRECISION NOT NULL,
    longitude   DOUBLE PRECISION NOT NULL,
    elevation   DOUBLE PRECISION,
    population  BIGINT,
    timezone    TEXT,
    loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dim_date (
    date         DATE PRIMARY KEY,
    year         SMALLINT NOT NULL,
    month        SMALLINT NOT NULL,
    month_name   TEXT NOT NULL,
    day_of_week  TEXT NOT NULL,
    is_weekend   BOOLEAN NOT NULL,
    season       TEXT NOT NULL                 -- Long rains / Short rains / Dry season
);

-- ---------------------------------------------------------------------------
-- Fact
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fact_daily_weather (
    city_id        INTEGER NOT NULL REFERENCES dim_city (city_id),
    weather_date   DATE    NOT NULL REFERENCES dim_date (date),
    temp_max       NUMERIC(4,1) NOT NULL,
    temp_min       NUMERIC(4,1) NOT NULL,
    temp_avg       NUMERIC(4,1) NOT NULL,
    precipitation  NUMERIC(6,1) NOT NULL,
    rain           NUMERIC(6,1),
    wind_max       NUMERIC(5,1),
    is_rainy_day   BOOLEAN NOT NULL,
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (city_id, weather_date)        -- one row per town per day
);

CREATE INDEX IF NOT EXISTS idx_fact_weather_date
    ON fact_daily_weather (weather_date);

-- ---------------------------------------------------------------------------
-- Control tables (created before rejected_records, which references them)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS etl_run_log (
    run_id          BIGSERIAL PRIMARY KEY,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    status          TEXT NOT NULL DEFAULT 'running'
                    CHECK (status IN ('running', 'success', 'failed')),
    rows_extracted  INTEGER NOT NULL DEFAULT 0,
    rows_loaded     INTEGER NOT NULL DEFAULT 0,
    rows_rejected   INTEGER NOT NULL DEFAULT 0,
    error_message   TEXT
);

CREATE TABLE IF NOT EXISTS etl_watermark (
    city_id     INTEGER PRIMARY KEY REFERENCES dim_city (city_id),
    last_date   DATE NOT NULL,                 -- last day successfully loaded
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS rejected_records (
    rejected_id   BIGSERIAL PRIMARY KEY,
    city_name     TEXT,
    weather_date  DATE,
    reason        TEXT NOT NULL,
    payload       JSONB,                       -- the original record, for debugging
    run_id        BIGINT REFERENCES etl_run_log (run_id),
    rejected_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
