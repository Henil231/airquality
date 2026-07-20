# Work plan

Remaining work, one task per section. Each task is self contained: read README.md
and CLAUDE.md first, follow the repo rules, keep diffs minimal. Tasks 1 to 3 are
code, 4 to 6 are runs that produce evidence, 7 and 8 are the deliverables.

Deadlines: presentation July 24, 2026. Final report August 7, 2026.
Order: 1, 2, 3 in any order, then 4, then 5, then 6, then 7. Task 8 after 7.

## Status

- Task 1 AirNow and PurpleAir fetchers: done and verified (keyless skip, default source unchanged). Live API mapping not tested, needs keys.
- Task 2 EPA AQI breakpoints: done and verified. Ran live in the containerized stream, wrote 180 alerts.
- Task 3 Leaflet hotspot map: done and verified. Exported 180 GeoJSON features from PostGIS.
- Task 4 local evidence run: numbers captured in evidence/local/ (latency avg 19.9 s, 180 alerts, trends). Screenshots of the Airflow UI, Leaflet map, and streaming logs still to be taken by a person.
- Tasks 5 to 8: not started. GCP rehearsal, dashboard, deck, report. Need GCP, a browser, and screenshots.

## 1. AirNow and PurpleAir fetchers

- Goal: the deck claims three data sources, the code currently fetches only
  OpenAQ. Add the other two.
- Files: `fetchers/fetch.py`
- Steps: add `airnow_latest()` and `purpleair_latest()` beside `openaq_latest()`,
  same shape: yield raw dicts, map to the reading dict in `to_reading` style.
  Keys come from `AIRNOW_KEY` and `PURPLEAIR_KEY` env vars. On missing key or
  network failure, print a notice and yield nothing, do not crash. Add a
  `--sources` arg defaulting to `openaq` so existing calls do not change.
- Done when: `python fetchers/fetch.py --sources openaq,airnow,purpleair` prints
  readings with no key set (openaq falls back to synthetic, the others skip),
  and real readings with keys set.

## 2. EPA AQI breakpoints

- Goal: replace the flat PM2.5 threshold in the streaming job with the EPA AQI
  breakpoint table so alert levels match the official scale.
- Files: `spark/stream_alerts.py`
- Steps: implement the piecewise linear AQI formula for PM2.5 as a small pure
  function, register the breakpoint table as a list of tuples, compute AQI and
  category (Good, Moderate, Unhealthy for Sensitive Groups, Unhealthy, Very
  Unhealthy, Hazardous), alert on Unhealthy and above. Keep `flag_breaches`
  as the single place this logic lives.
- Done when: a unit style check in the file or a small test proves known values,
  for example PM2.5 of 35.5 maps to Unhealthy for Sensitive Groups at AQI 101,
  and `./local.sh stream` still writes alerts.

## 3. Leaflet hotspot map

- Goal: the deck promises a Leaflet hotspot map backed by PostGIS.
- Files: new `map/export_geojson.sql`, new `map/index.html`, one new `local.sh`
  subcommand `map`.
- Steps: a SQL query that selects recent alerts as GeoJSON using PostGIS
  functions, `local.sh map` runs it and writes `map/alerts.geojson`, and
  `map/index.html` is a static Leaflet page that loads that file and renders
  color coded circle markers by level. No server, no framework, open the file
  in a browser.
- Done when: after `./local.sh stream` has produced alerts, `./local.sh map`
  writes the geojson and opening `map/index.html` shows markers.

## 4. Local end to end run, evidence capture

- Goal: produce the Experiments and Results evidence for both deliverables.
- Prereq: Docker only. Spark and the fetchers run inside the Airflow image, so
  the host needs no Python, Java, or pyspark.
- Numbers already captured in evidence/local/ (results.txt, alerts.geojson,
  NOTES.md): latency avg 19.9 s, 180 alerts by severity, monthly trends.
- Remaining, needs a person: `./local.sh up`, `./local.sh stream`,
  `./local.sh batch`, then take screenshots. Airflow UI via `./local.sh airflow`
  (one manual DAG run). View the map with `cd map && python3 -m http.server`
  then open http://localhost:8000, since browsers block a file:// fetch of the
  geojson. Save the screenshots into evidence/local/ next to the captured data.
- Done when: `evidence/local/` holds the results text, at least four
  screenshots, and a one paragraph note stating the measured average and max
  detection latency.

## 5. Cloud rehearsal on GCP, evidence capture

- Goal: prove the pipeline runs on GCP and capture cloud screenshots. One
  cycle only, then everything comes down.
- Prereq: `.env` filled in per `.env.example`, billing account with free
  credit. Cloud SQL connectivity is already wired: the cluster runs the Auth
  Proxy, `PGURL` stays on localhost.
- Steps: `./gcp.sh provision`, load the schema, `./gcp.sh up`, seed readings
  (run the fetcher with `--sink postgres` against Cloud SQL), `./gcp.sh batch`,
  `./gcp.sh stream` briefly, screenshot the Dataproc cluster page, the job
  page, and the Cloud SQL instance page, then `./gcp.sh down` and confirm the
  verification output shows nothing running. Save screenshots and the down
  output to `evidence/gcp/`.
- Done when: `evidence/gcp/` holds the screenshots and the down verification,
  and `./gcp.sh status` shows no cluster and the instance STOPPED.

## 6. Looker Studio dashboard

- Goal: the client dashboard deliverable.
- Steps: during the task 5 window while Cloud SQL is up, connect Looker Studio
  to the Cloud SQL Postgres instance, build one page: alert count by level,
  alerts over time, a table of recent alerts, a trends chart. Screenshot to
  `evidence/gcp/`. If the Cloud SQL connector is blocked, fall back to
  exporting `results.sql` output as CSV and charting that, and say so in the
  report.
- Done when: one dashboard screenshot exists in `evidence/gcp/`.

## 7. Presentation, due July 24

- Goal: the 50 point ppt.
- Steps: run Prompt 1 from the Assignment prompts section of README.md against
  this repo, insert the evidence from `evidence/`, review every claim against
  the code, keep the regeneration prompt as the final appendix slide.
- Done when: a .pptx exists, every section of the assignment is present, and
  no slide claims something the repo does not do.

## 8. Final report, due August 7

- Goal: the 150 point docx.
- Steps: run Prompt 2 from the Assignment prompts section of README.md, insert
  evidence and the measured numbers, verify the seven plus references are real
  and properly cited, all team members submit.
- Done when: a .docx exists covering every required section with at least
  seven references.
