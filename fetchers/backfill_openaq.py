#!/usr/bin/env python3
"""Batch lane backfill: pull real OpenAQ archive history from S3 into readings.

Public archive, no AWS credentials needed:
https://docs.openaq.org/aws/about
"""
import argparse
import csv
import gzip
import io
import sys
import urllib.request
from datetime import datetime, timedelta, timezone

ARCHIVE_URL = (
    "https://openaq-data-archive.s3.amazonaws.com/records/csv.gz/"
    "locationid={loc}/year={year}/month={month:02d}/location-{loc}-{date}.csv.gz"
)
HTTP_TIMEOUT = 25


def day_range(start, end):
    day = start
    while day <= end:
        yield day
        day += timedelta(days=1)


def fetch_day(location_id, day):
    url = ARCHIVE_URL.format(loc=location_id, year=day.year, month=day.month, date=day.strftime("%Y%m%d"))
    try:
        with urllib.request.urlopen(url, timeout=HTTP_TIMEOUT) as response:
            raw = gzip.decompress(response.read())
    except Exception as error:
        print(f"backfill {location_id} {day:%Y-%m-%d} unavailable ({error}); skipping", file=sys.stderr)
        return []

    readings = []
    for row in csv.DictReader(io.StringIO(raw.decode())):
        if row.get("parameter") != "pm25":
            continue
        try:
            ts = datetime.fromisoformat(row["datetime"]).astimezone(timezone.utc).timestamp()
            value = float(row["value"])
            lat = float(row["lat"])
            lon = float(row["lon"])
        except (KeyError, ValueError):
            continue
        readings.append({
            "source": "openaq_archive",
            "sensor_id": row.get("location_id", str(location_id)),
            "pollutant": "pm25",
            "value": value,
            "unit": "ug/m3",
            "lat": lat,
            "lon": lon,
            "ts": ts,
        })
    return readings


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--locations", required=True, help="comma separated OpenAQ location ids")
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    parser.add_argument("--pg", default="postgresql://air:air@localhost:5432/air")
    args = parser.parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end = datetime.strptime(args.end, "%Y-%m-%d").date()
    locations = [loc.strip() for loc in args.locations.split(",") if loc.strip()]

    readings = []
    for location_id in locations:
        for day in day_range(start, end):
            readings.extend(fetch_day(location_id, day))

    if not readings:
        print("no readings fetched; nothing to write", file=sys.stderr)
        return

    from fetch import write_postgres  # reuse the existing writer, same readings schema
    write_postgres(readings, args.pg)
    print(f"wrote {len(readings)} backfilled readings")


if __name__ == "__main__":
    main()