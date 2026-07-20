# Local evidence, captured 2026-07-20

Produced by `./local.sh stream` then `./local.sh batch` on the containerized
local stack (Spark 3.5.1 on the Airflow image, PostGIS, Kafka). Full query
output is in results.txt, the map data is in alerts.geojson.

## Streaming lane, detection latency
- 200 synthetic readings produced to Kafka, drained once by Spark Structured Streaming.
- 180 AQI breaches written to the alerts table (alert cutoff AQI 101).
- By severity: Unhealthy 67, Very Unhealthy 67, Hazardous 27, Unhealthy for Sensitive Groups 19.
- Detection latency (reading timestamp to alert row): avg 19.9 s, max 19.9 s.
  This is one cold start micro batch, so all alerts share the batch time. The
  point holds: the streaming lane detects within one micro batch, tens of
  seconds, versus the hourly batch baseline that detects a spike up to 60
  minutes late.

## Batch lane
- Spark batch rolled the readings history into monthly avg and max per sensor (trends table).

## Map
- PostGIS exported 180 alert points as a GeoJSON FeatureCollection for the Leaflet map.

## How to reproduce
    ./local.sh up
    ./local.sh stream
    ./local.sh batch
    ./local.sh results
    ./local.sh map
