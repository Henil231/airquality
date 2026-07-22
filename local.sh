#!/usr/bin/env bash
# Control the local stack. Spark and the fetchers run inside the Airflow image,
# so the host Python and Java versions do not matter.
#
# Most devs only need these:
#   ./local.sh demo [real]   start everything and stream live, watch the dashboard
#   ./local.sh stop          stop the live feed and stream
#   ./local.sh results       print alert counts, latency, and trends
#   ./local.sh down          stop everything
# Add "real" to pull the live APIs (needs keys in .env), otherwise it uses the
# free synthetic feed. More: up, stream [real], batch [real], load [N], map, airflow.
set -euo pipefail
cd "$(dirname "$0")"

PROJ="/opt/airflow/project"
PG_JDBC="jdbc:postgresql://db:5432/air?user=air&password=air"
PG_DSN="postgresql://air:air@db:5432/air"
KAFKA="kafka:9092"
PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3"
RUN="docker compose run --rm --no-deps -w /tmp --entrypoint"

# "real" pulls the live APIs, anything else stays on the free synthetic feed.
sources() { [ "${1:-}" = "real" ] && echo "openaq,airnow,purpleair" || echo "openaq"; }

topic() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --create --if-not-exists --topic readings --bootstrap-server localhost:9092 >/dev/null
}

fresh_topic() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --delete --topic readings --bootstrap-server localhost:9092 >/dev/null 2>&1 || true
  sleep 2
  topic
  docker compose exec -T db psql -U air -d air -c "TRUNCATE alerts;" >/dev/null
}

start_infra() {
  docker compose up -d --build db kafka web
  echo "waiting for postgres"
  until docker compose exec -T db psql -U air -d air -c 'select 1' >/dev/null 2>&1; do sleep 1; done
  docker compose exec -T db psql -U air -d air -q < sql/schema.sql >/dev/null 2>&1 || true
  topic
}

start_live() {
  local src; src=$(sources "${1:-}")
  fresh_topic
  docker rm -f air-stream air-feed >/dev/null 2>&1 || true
  docker compose run -d --name air-stream --no-deps -w /tmp --entrypoint spark-submit airflow \
    --packages "$PACKAGES" "$PROJ/spark/stream_alerts.py" \
    --bootstrap "$KAFKA" --topic readings --pg "$PG_JDBC" --from-start >/dev/null
  echo "stream warming up..."
  sleep 25
  docker compose run -d --name air-feed --no-deps -w /tmp --entrypoint python airflow \
    "$PROJ/fetchers/fetch.py" --sink kafka --bootstrap "$KAFKA" --topic readings \
    --sources "$src" --limit 50 --loops 600 --interval 1 >/dev/null
}

case "${1:-}" in
  up)
    start_infra
    echo "up. dashboard: http://localhost:8050"
    ;;
  demo)
    start_infra
    start_live "${2:-}"
    echo "live feed running. open the dashboard: http://localhost:8050"
    echo "stop the feed with ./local.sh stop, tear it all down with ./local.sh down"
    ;;
  stream)
    src=$(sources "${2:-}")
    $RUN python airflow "$PROJ/fetchers/fetch.py" \
      --sink kafka --bootstrap "$KAFKA" --topic readings --sources "$src" --limit 200
    $RUN spark-submit airflow --packages "$PACKAGES" "$PROJ/spark/stream_alerts.py" \
      --bootstrap "$KAFKA" --topic readings --pg "$PG_JDBC" --once
    ;;
  batch)
    src=$(sources "${2:-}")
    $RUN python airflow "$PROJ/fetchers/fetch.py" --sink postgres --pg "$PG_DSN" --sources "$src"
    $RUN spark-submit airflow --packages "$PACKAGES" "$PROJ/spark/batch_trends.py" --pg "$PG_JDBC"
    ;;
  load)
    total="${2:-5000}"; chunk=200; loops=$(( total / chunk ))
    echo "load test: $total readings, $chunk every 0.2s (a simulated live feed)"
    fresh_topic
    t0=$(date +%s)
    $RUN python airflow "$PROJ/fetchers/fetch.py" \
      --sink kafka --bootstrap "$KAFKA" --topic readings --limit "$chunk" --loops "$loops" --interval 0.2
    t1=$(date +%s)
    $RUN spark-submit airflow --packages "$PACKAGES" "$PROJ/spark/stream_alerts.py" \
      --bootstrap "$KAFKA" --topic readings --pg "$PG_JDBC" --once
    t2=$(date +%s)
    echo "produced $total readings in $((t1 - t0))s, drained and alerted in $((t2 - t1))s"
    docker compose exec -T db psql -U air -d air -c \
      "SELECT count(*) AS alerts_written, round(avg(extract(epoch FROM ingested_at - ts))::numeric,1) AS avg_latency_s FROM alerts;"
    ;;
  stop)
    docker rm -f air-feed air-stream >/dev/null 2>&1 || true
    echo "live feed and stream stopped"
    ;;
  results)
    docker compose exec -T db psql -U air -d air < sql/results.sql
    ;;
  map)
    docker compose exec -T db psql -U air -d air -tA < map/export_geojson.sql > map/alerts.geojson
    echo "wrote map/alerts.geojson"
    ;;
  usmap)
    # Seed the readings table with live PM2.5 across the continental US so the
    # dashboard shows every station colored by AQI. Needs OPENAQ_KEY, AIRNOW_KEY.
    docker compose exec -T db psql -U air -d air -c "TRUNCATE readings;" >/dev/null
    $RUN python airflow "$PROJ/fetchers/fetch.py" --sink postgres --pg "$PG_DSN" \
      --sources openaq,airnow --limit 1000 --pages 6 --bbox=-125,24,-66,50
    echo "seeded US-wide readings. open the dashboard: http://localhost:8050"
    ;;
  airflow)
    docker compose up -d --build airflow
    echo "airflow UI: http://localhost:8080  user admin"
    echo "password:  docker compose logs airflow | grep -i password"
    ;;
  down)
    docker rm -f air-feed air-stream >/dev/null 2>&1 || true
    docker compose down
    ;;
  *)
    echo "usage: ./local.sh {demo [real]|stop|up|stream [real]|batch [real]|load [N]|results|map|usmap|airflow|down}"
    exit 1
    ;;
esac
