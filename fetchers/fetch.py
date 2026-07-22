#!/usr/bin/env python3
"""Fetch air quality readings from public sources and write them to a sink.

Sinks:
  kafka     stream readings for the real time lane
  postgres  seed the readings table for the batch lane
  stdout    inspect output while developing
"""
import argparse
import json
import os
import random
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone


# OpenAQ v3 serves latest readings per parameter, there is no global /latest.
# 2 is pm25, confirm with GET /v3/parameters.
OPENAQ_PM25 = 2


def openaq_latest(limit, pages=1):
    key = os.getenv("OPENAQ_KEY")
    if not key:
        # Fall back to synthetic readings so local dev needs no key or network.
        print("openaq key missing (set OPENAQ_KEY); using synthetic data", file=sys.stderr)
        yield from (None for _ in range(limit))
        return
    for page in range(1, pages + 1):
        request = urllib.request.Request(
            f"https://api.openaq.org/v3/parameters/{OPENAQ_PM25}/latest?limit={limit}&page={page}",
            headers={"X-API-Key": key},
        )
        try:
            results = json.load(urllib.request.urlopen(request, timeout=15)).get("results", [])
        except Exception as error:
            print(f"openaq unavailable ({error}); using synthetic data", file=sys.stderr)
            if page == 1:
                yield from (None for _ in range(limit))
            return
        yield from results
        if len(results) < limit:
            return


def to_reading(raw):
    if raw:
        coords = raw.get("coordinates") or {}
        # v3 latest carries no parameter or unit field, both are fixed by the endpoint.
        return {
            "source": "openaq",
            "sensor_id": str(raw.get("sensorsId", "unknown")),
            "pollutant": "pm25",
            "value": float(raw.get("value", 0) or 0),
            "unit": "ug/m3",
            "lat": coords.get("latitude"),
            "lon": coords.get("longitude"),
            "ts": time.time(),
        }
    return {
        "source": "synthetic",
        "sensor_id": f"s{random.randint(1, 20)}",
        "pollutant": "pm25",
        "value": round(random.uniform(5, 300), 1),
        "unit": "ug/m3",
        "lat": 40 + random.random(),
        "lon": -74 - random.random(),
        "ts": time.time(),
    }


def airnow_latest(limit, pages=1):
    key = os.getenv("AIRNOW_KEY")
    if not key:
        print("airnow key missing (set AIRNOW_KEY); skipping", file=sys.stderr)
        return
    # PM2.5 across the continental US. AirNow publishes with a lag, so ask for a
    # two hour window, a single current hour usually comes back empty.
    now = datetime.now(timezone.utc)
    end = now.strftime("%Y-%m-%dT%H")
    start = (now - timedelta(hours=2)).strftime("%Y-%m-%dT%H")
    request = urllib.request.Request(
        "https://www.airnowapi.org/aq/data/"
        f"?startDate={start}&endDate={end}"
        "&parameters=PM25&BBOX=-125,24,-66,50"
        "&dataType=B&format=application/json&verbose=1"
        f"&API_KEY={key}"
    )
    try:
        payload = json.load(urllib.request.urlopen(request, timeout=10))
        yield from payload[:limit]
    except Exception as error:
        print(f"airnow unavailable ({error}); skipping", file=sys.stderr)
        return


def airnow_reading(raw):
    return {
        "source": "airnow",
        "sensor_id": str(raw.get("FullAQSCode") or raw.get("SiteName", "unknown")),
        "pollutant": "pm25",
        "value": float(raw.get("Value", 0) or 0),
        "unit": raw.get("Unit", "ug/m3"),
        "lat": raw.get("Latitude"),
        "lon": raw.get("Longitude"),
        "ts": time.time(),
    }


def purpleair_latest(limit, pages=1):
    key = os.getenv("PURPLEAIR_KEY")
    if not key:
        print("purpleair key missing (set PURPLEAIR_KEY); skipping", file=sys.stderr)
        return
    request = urllib.request.Request(
        "https://api.purpleair.com/v1/sensors"
        "?fields=pm2.5,latitude,longitude&max_age=3600",
        headers={"X-API-Key": key},
    )
    try:
        payload = json.load(urllib.request.urlopen(request, timeout=10))
        # Data rows are positional, so pair them with the field names.
        fields = payload.get("fields", [])
        for row in payload.get("data", [])[:limit]:
            yield dict(zip(fields, row))
    except Exception as error:
        print(f"purpleair unavailable ({error}); skipping", file=sys.stderr)
        return


def purpleair_reading(raw):
    return {
        "source": "purpleair",
        "sensor_id": str(raw.get("sensor_index", "unknown")),
        "pollutant": "pm25",
        "value": float(raw.get("pm2.5", 0) or 0),
        "unit": "ug/m3",
        "lat": raw.get("latitude"),
        "lon": raw.get("longitude"),
        "ts": time.time(),
    }


# Each source pairs a fetcher that yields raw dicts with its reading mapper.
SOURCES = {
    "openaq": (openaq_latest, to_reading),
    "airnow": (airnow_latest, airnow_reading),
    "purpleair": (purpleair_latest, purpleair_reading),
}


def write_kafka(readings, bootstrap, topic):
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda value: json.dumps(value).encode(),
    )
    for reading in readings:
        producer.send(topic, reading)
    producer.flush()


def write_postgres(readings, dsn):
    import psycopg2
    from psycopg2.extras import execute_batch

    rows = [{**r, "ts": datetime.fromtimestamp(r["ts"], tz=timezone.utc)} for r in readings]
    query = (
        "INSERT INTO readings "
        "(source, sensor_id, pollutant, value, unit, lat, lon, ts) VALUES "
        "(%(source)s, %(sensor_id)s, %(pollutant)s, %(value)s, %(unit)s, "
        "%(lat)s, %(lon)s, %(ts)s)"
    )
    with psycopg2.connect(dsn) as connection, connection.cursor() as cursor:
        execute_batch(cursor, query, rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sink", choices=["kafka", "postgres", "stdout"], default="stdout")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="readings")
    parser.add_argument("--pg", default="postgresql://air:air@localhost:5432/air")
    parser.add_argument("--sources", default="openaq")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--pages", type=int, default=1,
                        help="pages to pull per source, raise it for wider coverage (openaq)")
    parser.add_argument("--bbox", default="",
                        help="minlon,minlat,maxlon,maxlat filter, US is -125,24,-66,50")
    parser.add_argument("--loops", type=int, default=1)
    parser.add_argument("--interval", type=float, default=60,
                        help="seconds between loops, lower it to simulate a fast live feed")
    args = parser.parse_args()

    sources = [name for name in args.sources.split(",") if name]
    box = [float(v) for v in args.bbox.split(",")] if args.bbox else None

    def keep(reading):
        # Drop sensor sentinels (-999) and impossible spikes so bad data never
        # reaches the map or skews the trend averages.
        value = reading["value"]
        if reading["pollutant"] == "pm25" and not (0 <= value <= 1000):
            return False
        if box is not None:
            lat, lon = reading["lat"], reading["lon"]
            if lat is None or lon is None or not (box[0] <= lon <= box[2] and box[1] <= lat <= box[3]):
                return False
        return True

    for iteration in range(args.loops):
        readings = []
        for name in sources:
            if name not in SOURCES:
                print(f"unknown source {name}; skipping", file=sys.stderr)
                continue
            fetch, to_dict = SOURCES[name]
            readings.extend(r for r in (to_dict(raw) for raw in fetch(args.limit, args.pages)) if keep(r))
        if args.sink == "kafka":
            write_kafka(readings, args.bootstrap, args.topic)
        elif args.sink == "postgres":
            write_postgres(readings, args.pg)
        else:
            for reading in readings:
                print(json.dumps(reading))
        if iteration + 1 < args.loops:
            time.sleep(args.interval)


if __name__ == "__main__":
    main()
