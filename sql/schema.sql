CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS readings (
  id        BIGSERIAL PRIMARY KEY,
  source    TEXT,
  sensor_id TEXT,
  pollutant TEXT,
  value     DOUBLE PRECISION,
  unit      TEXT,
  lat       DOUBLE PRECISION,
  lon       DOUBLE PRECISION,
  ts        TIMESTAMPTZ,
  geom      GEOMETRY(Point, 4326)
            GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(lon, lat), 4326)) STORED
);

CREATE TABLE IF NOT EXISTS alerts (
  id          BIGSERIAL PRIMARY KEY,
  source      TEXT,
  sensor_id   TEXT,
  pollutant   TEXT,
  value       DOUBLE PRECISION,
  level       TEXT,
  lat         DOUBLE PRECISION,
  lon         DOUBLE PRECISION,
  ts          TIMESTAMPTZ,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  geom      GEOMETRY(Point, 4326)
            GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(lon, lat), 4326)) STORED
);

CREATE TABLE IF NOT EXISTS trends (
  sensor_id TEXT,
  pollutant TEXT,
  month     TIMESTAMPTZ,
  avg_value DOUBLE PRECISION,
  max_value DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS readings_geom_idx ON readings USING GIST (geom);
CREATE INDEX IF NOT EXISTS alerts_geom_idx   ON alerts   USING GIST (geom);
