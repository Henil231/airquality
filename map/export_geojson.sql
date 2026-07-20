-- Export recent alerts as one GeoJSON FeatureCollection for the Leaflet map.
-- local.sh map runs this and writes the output to map/alerts.geojson.
SELECT json_build_object(
  'type', 'FeatureCollection',
  'features', COALESCE(json_agg(feature), '[]'::json)
) AS geojson
FROM (
  SELECT json_build_object(
    'type', 'Feature',
    'geometry', ST_AsGeoJSON(geom)::json,
    'properties', json_build_object(
      'level', level,
      'pollutant', pollutant,
      'value', value,
      'sensor_id', sensor_id,
      'ts', ts
    )
  ) AS feature
  FROM (
    SELECT level, pollutant, value, sensor_id, ts, geom
    FROM alerts
    WHERE geom IS NOT NULL
    ORDER BY ts DESC
    LIMIT 500
  ) recent
) features;
