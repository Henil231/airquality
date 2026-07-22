#!/usr/bin/env python3
"""Live dashboard server. Queries PostGIS on each request and serves JSON."""
import json
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import psycopg2

DSN = os.getenv("PG_DSN", "postgresql://air:air@db:5432/air")
HERE = os.path.dirname(os.path.abspath(__file__))

# Reuse the one EPA AQI implementation from the streaming job, do not re-derive it.
sys.path.insert(0, os.path.dirname(HERE))
from spark.stream_alerts import pm25_aqi


def query(sql):
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def stats():
    by_level = [
        {"level": lvl, "count": cnt}
        for lvl, cnt in query(
            "SELECT level, count(*) FROM alerts GROUP BY level ORDER BY count(*) DESC"
        )
    ]
    total, avg_lat, max_lat = query(
        "SELECT count(*), "
        "round(avg(extract(epoch FROM ingested_at - ts))::numeric, 1), "
        "round(max(extract(epoch FROM ingested_at - ts))::numeric, 1) FROM alerts"
    )[0]
    recent = [
        {"sensor_id": s, "level": lvl, "value": float(v), "ts": t.isoformat()}
        for s, lvl, v, t in query(
            "SELECT sensor_id, level, value, ts FROM alerts ORDER BY ts DESC LIMIT 15"
        )
    ]
    return {
        "total": total,
        "avg_latency": float(avg_lat or 0),
        "max_latency": float(max_lat or 0),
        "by_level": by_level,
        "recent": recent,
    }


def geojson():
    rows = query(
        "SELECT json_build_object('type', 'FeatureCollection', 'features', "
        "COALESCE(json_agg(json_build_object("
        "'type', 'Feature', 'geometry', ST_AsGeoJSON(geom)::json, "
        "'properties', json_build_object("
        "'level', level, 'value', value, 'sensor_id', sensor_id))), '[]'::json)) "
        "FROM (SELECT * FROM alerts WHERE geom IS NOT NULL ORDER BY ts DESC LIMIT 500) r"
    )
    return rows[0][0]


def readings_geojson():
    """Every recent station reading as a point, colored by its EPA AQI category."""
    rows = query(
        "SELECT source, sensor_id, value, ST_X(geom), ST_Y(geom) FROM readings "
        "WHERE geom IS NOT NULL AND pollutant = 'pm25' ORDER BY ts DESC LIMIT 5000"
    )
    seen, features = set(), []
    for source, sensor_id, value, lon, lat in rows:
        if sensor_id in seen:
            continue
        seen.add(sensor_id)
        aqi = pm25_aqi(value)
        if not aqi:
            continue
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {
                "source": source, "sensor_id": sensor_id,
                "value": round(value, 1), "aqi": aqi[0], "category": aqi[1],
            },
        })
    return {"type": "FeatureCollection", "features": features}


def coverage():
    """Headline counts for the station map."""
    total, stations, sources = query(
        "SELECT count(*), count(DISTINCT sensor_id), count(DISTINCT source) "
        "FROM readings WHERE pollutant = 'pm25'"
    )[0]
    return {"readings": total, "stations": stations, "sources": sources}


class Handler(BaseHTTPRequestHandler):
    def _send(self, body, ctype):
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        try:
            if self.path == "/":
                with open(os.path.join(HERE, "dashboard.html"), "rb") as page:
                    self._send(page.read(), "text/html")
            elif self.path.startswith("/api/stats"):
                self._send(json.dumps(stats()).encode(), "application/json")
            elif self.path.startswith("/api/geojson"):
                self._send(json.dumps(geojson()).encode(), "application/json")
            elif self.path.startswith("/api/readings"):
                self._send(json.dumps(readings_geojson()).encode(), "application/json")
            elif self.path.startswith("/api/coverage"):
                self._send(json.dumps(coverage()).encode(), "application/json")
            else:
                self.send_error(404)
        except Exception as error:
            self.send_error(500, str(error))

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8050), Handler).serve_forever()
