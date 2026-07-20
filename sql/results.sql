-- Experiment evidence. Run after a stream or batch run, screenshot the output.

-- Alert volume by severity
SELECT level, count(*) AS alerts
FROM alerts
GROUP BY level;

-- Detection latency in seconds: reading timestamp to alert row arrival
SELECT round(avg(extract(epoch FROM ingested_at - ts))::numeric, 1) AS avg_seconds,
       round(max(extract(epoch FROM ingested_at - ts))::numeric, 1) AS max_seconds
FROM alerts;

-- Most recent alerts
SELECT sensor_id, pollutant, value, level, ts
FROM alerts
ORDER BY ts DESC
LIMIT 10;

-- Monthly trend sample from the batch lane
SELECT sensor_id, pollutant, month, round(avg_value::numeric, 1) AS avg_value,
       round(max_value::numeric, 1) AS max_value
FROM trends
ORDER BY month DESC, sensor_id
LIMIT 10;
