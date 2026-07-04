-- Positions table: "GPS" points associated with a trip.
-- The `stopped` field marks the points inserted while the user is flagged as
-- stationary (see server/src/trips/mod.rs::STOP_THRESHOLD_SAMPLES).

CREATE TABLE IF NOT EXISTS positions (
    trip_id INTEGER NOT NULL REFERENCES trips(id) ON DELETE CASCADE,
    ts      INTEGER NOT NULL,
    lat     REAL    NOT NULL,
    lon     REAL    NOT NULL,
    stopped  BOOLEAN    NOT NULL,
    PRIMARY KEY (trip_id, ts)
);

CREATE INDEX IF NOT EXISTS idx_positions_trip_ts
    ON positions(trip_id, ts);
