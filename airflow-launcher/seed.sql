-- Creates and seeds the energy_consumption table with 2 years of hourly data
-- across 10 simulated households. Runs automatically on first container start.

CREATE TABLE IF NOT EXISTS energy_consumption (
    id          SERIAL PRIMARY KEY,
    timestamp   TIMESTAMPTZ NOT NULL,
    household   INTEGER NOT NULL,       -- household id 1-10
    consumption FLOAT   NOT NULL,       -- kWh
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_energy_timestamp  ON energy_consumption(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_energy_household  ON energy_consumption(household);
CREATE INDEX IF NOT EXISTS idx_energy_updated_at ON energy_consumption(updated_at DESC);

-- Seed 2 years of hourly data (17520 hours × 10 households = 175200 rows)
INSERT INTO energy_consumption (timestamp, household, consumption)
SELECT
    ts,
    h,
    -- Base load + time-of-day peak + seasonal variation + noise
    ROUND((
        1.5                                                          -- base kWh
        + 1.2 * SIN(EXTRACT(HOUR FROM ts) * PI() / 12.0 - 1.5)     -- daily cycle, peaks ~14:00
        + 0.8 * SIN(EXTRACT(DOY  FROM ts) * 2 * PI() / 365.0)      -- seasonal cycle
        + 0.3 * RANDOM()                                             -- noise
        + 0.1 * h                                                    -- per-household offset
    )::NUMERIC, 4) AS consumption
FROM
    GENERATE_SERIES(
        NOW() - INTERVAL '2 years',
        NOW(),
        INTERVAL '1 hour'
    ) AS ts,
    GENERATE_SERIES(1, 10) AS h
ON CONFLICT DO NOTHING;