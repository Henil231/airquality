# Real Time Air Quality Monitoring

A two lane big data pipeline that ingests live air quality readings, alerts on
dangerous spikes in about one second, and mines history for trends.

## Collaborators

- Olha Shevchuk
- Chung Hyun Kim
- Henil Dineshbhai Patel

## Quickstart, run it locally

Make sure Docker is running, then:

    cp .env.example .env
    ./local.sh demo

Open http://localhost:8050 and watch live air quality alerts appear on the map.
When you are done:

    ./local.sh stop    # stop the live feed
    ./local.sh down    # stop everything

The first run pulls Docker images and Spark jars, so give it a couple of minutes.
No API keys are needed, it uses a built in synthetic feed. See Data sources below
to switch to real data.

## Prerequisites

- Docker and Docker Compose, installed and running. This is all you need for the local stack.
- A Google Cloud account and the Google Cloud SDK (`gcloud`), only if you also run the cloud demo.

Spark and the fetchers run inside a container (Java 17, Python 3.11), so your own
machine does not need Python, Java, or pyspark.

## Start here

This README is the single reference for the project: what the system does, how it
is structured, why each choice was made, and how to run it. `PLAN.md` holds the
remaining work as self contained tasks you can delegate. `CLAUDE.md` holds the
coding rules. To understand or extend this repo with a Claude Code agent, point
it at this file first.

All experiment results are measured on the free local stack. The GCP run is a
short rehearsal that only captures screenshots to prove the same jobs run on
Dataproc, it produces no new measurements.

## Architecture

Two lanes feed one PostGIS database, then a dashboard and a map serve it.

- Real time lane: fetch, Kafka, Spark Structured Streaming, alerts table. Detects an AQI breach in about one second.
- Batch lane: fetch, readings table, Spark batch, trends table, scheduled by Airflow. Reveals monthly and seasonal patterns.
- Serving: PostGIS for geospatial queries, a live web dashboard (Leaflet map plus stat tiles) at http://localhost:8050, and Looker Studio on the cloud.

Which command drives which part of the flow:

    fetch + Kafka + stream    ->  ./local.sh demo     (live) or stream (one pass)
    fetch + batch + Airflow   ->  ./local.sh batch    or the Airflow DAG
    high rate throughput      ->  ./local.sh load N
    read the results          ->  ./local.sh results  and the dashboard

## Layout

| Path | Purpose |
| --- | --- |
| `PLAN.md` | Remaining work, one delegable task per section |
| `fetchers/fetch.py` | Pull readings from OpenAQ, AirNow, PurpleAir; write to Kafka or Postgres |
| `spark/stream_alerts.py` | Streaming job: Kafka to alerts, with the EPA AQI check |
| `spark/batch_trends.py` | Batch job: readings to monthly trends |
| `airflow/dags/pipeline.py` | Airflow DAG for the batch lane |
| `web/server.py`, `web/dashboard.html` | Live web dashboard, queries PostGIS |
| `map/export_geojson.sql`, `map/index.html` | Static Leaflet hotspot map |
| `sql/schema.sql`, `sql/results.sql` | PostGIS tables and the evidence queries |
| `evidence/local/` | Captured run results and notes |
| `docker-compose.yml` | Local Postgres, Kafka, Airflow, dashboard |
| `local.sh` | Control the local stack, one command per action |
| `gcp.sh` | Control the GCP demo stack |

## Local development (free)

Copy the env file first:

    cp .env.example .env

Fastest path, one command brings up everything and streams live:

    ./local.sh demo          # free synthetic feed, or
    ./local.sh demo real     # live OpenAQ, AirNow, PurpleAir (needs keys in .env)

Then open the dashboard at http://localhost:8050 and watch the alert count and
map climb. Stop the feed with `./local.sh stop`, tear it all down with `./local.sh down`.

| Command | When to run |
| --- | --- |
| `./local.sh demo [real]` | Start everything, dashboard, and a live feed in one command |
| `./local.sh stop` | Stop the live feed and stream, leave the infra up |
| `./local.sh results` | Print evidence: alert counts, latency, trends |
| `./local.sh down` | Stop everything |
| `./local.sh up` | Start Postgres, Kafka, and the dashboard only |
| `./local.sh stream [real]` | Fast lane once: produce, drain, write alerts |
| `./local.sh batch [real]` | Batch lane: seed readings, build monthly trends |
| `./local.sh load [N]` | Throughput test: push N readings fast, measure the drain (default 5000) |
| `./local.sh us` | One command for the full US live map: infra, station layer, and alerts (needs keys) |
| `./local.sh map` | Export recent alerts to map/alerts.geojson |
| `./local.sh usmap` | Seed live PM2.5 across the continental US, fills the station map (needs keys) |
| `./local.sh airflow` | Start the Airflow UI at http://localhost:8080 |

Add `real` to `demo`, `stream`, or `batch` to pull the live APIs instead of the
synthetic feed. With the continuous stream, detection latency holds near one
second at about 50 readings per second. The dashboard needs internet for map tiles.

Airflow login is `admin`. Get the password with:

    docker compose logs airflow | grep -i password

Do all development and testing here. It touches no cloud resources and costs nothing.

## Data sources, synthetic and real

Live readings come from three free public APIs:

- OpenAQ, global baseline and history
- EPA AirNow, trusted US stations
- PurpleAir, dense low cost sensors, the fastest updates

By default the fetcher runs on a synthetic feed (realistic PM2.5 values), so the
whole stack works offline and free with no keys. That is what plain `./local.sh demo`
uses. To run against real data:

1. Get free keys from openaq.org, airnowapi.org, and purpleair.com.
2. Put them in `.env`:

       OPENAQ_KEY=...
       AIRNOW_KEY=...
       PURPLEAIR_KEY=...

3. Add `real` to any run command:

       ./local.sh demo real       # live feed on the dashboard
       ./local.sh stream real      # one fast lane pass on real data
       ./local.sh batch real       # batch lane on real data

Same pipeline and same dashboard, real sensor readings instead of synthetic. The
fetchers follow each API's documented schema. If a source has no key or is
unreachable it prints a notice and is skipped, the run does not fail.

## GCP demo (Dataproc)

### Production setup, one time

Do this section once, before any `gcp.sh` command:

1. A Google Cloud account with billing enabled. The free trial credit covers this.
2. Install the Google Cloud SDK, then `gcloud auth login` and `gcloud config set project YOUR_PROJECT`.
3. Copy `.env.example` to `.env` and fill in PROJECT, REGION, BUCKET, SQL_INSTANCE, CLUSTER, PGPASS, PGURL.
4. Create the durable resources and load the schema:

       ./gcp.sh provision
       gcloud sql connect air-sql --user=postgres --database=air < sql/schema.sql

`provision` enables the APIs (Dataproc, Cloud SQL, Storage, Compute) and creates
the bucket and the Cloud SQL instance. Run it only once.

### Run a demo, then bring it down

    ./gcp.sh up          # start Cloud SQL and an ephemeral Dataproc cluster
    ./gcp.sh batch       # submit the batch trends job
    ./gcp.sh stream      # submit the streaming alert job
    ./gcp.sh down        # delete the cluster, stop Cloud SQL, verify nothing runs

| Command | When to run |
| --- | --- |
| `./gcp.sh provision` | One time: enable APIs, create the bucket and Cloud SQL |
| `./gcp.sh up` | Before a demo: start Cloud SQL and an ephemeral Dataproc cluster with Kafka |
| `./gcp.sh batch` | Submit the batch trends job |
| `./gcp.sh stream` | Submit the streaming alert job |
| `./gcp.sh down` | After a demo: delete the cluster, stop Cloud SQL |
| `./gcp.sh destroy` | Tear everything down permanently, after the course |
| `./gcp.sh status` | Show what is running and billing |

Run `./gcp.sh down` the moment the demo ends.

### Cost control

Only two things bill while idle: Cloud SQL and a running Dataproc cluster.
`./gcp.sh down` stops both. Cloud Storage and stopped resources cost cents. We do
not use Cloud Composer, it runs all day, so Airflow runs locally instead. The
$300 free trial credit is far more than an intermittent demo needs.

### Cloud SQL connectivity

The cluster runs the Cloud SQL Auth Proxy through a Dataproc initialization
action, wired in `gcp.sh up`. Cloud jobs reach the database at
`localhost:5432`, so `PGURL` in `.env` keeps a localhost host. Nothing else to
configure.

## Experiments

The claim under test: the streaming lane detects dangerous spikes in seconds to
minutes, while the old batch only approach detects them hours later.

1. Run `./local.sh up` then `./local.sh stream`, let it process a few fetch loops.
2. Run `./local.sh batch` to build trends.
3. Run `./local.sh results` and screenshot the output. It shows alert counts by
   severity, detection latency (reading timestamp to alert row arrival), recent
   alerts, and monthly trends.
4. Screenshot the Airflow DAG run (`./local.sh airflow`), the Looker Studio
   dashboard, and the Dataproc job page from the cloud demo.

That set of evidence covers the Experiments and Results sections of both the
presentation and the final report.

Framing that keeps the results honest: latency is measured locally, and the
cloud rehearsal proves the same jobs run on Dataproc. The batch only baseline
is bounded by its schedule, the hourly DAG detects a spike up to an hour late,
which is the before number against the streaming lane's seconds.

## AI platform investigation

Required analysis in both deliverables. The position, kept factual to this repo:

- Partially replaceable: a no code platform such as n8n or Zapier could poll the
  same APIs on a schedule and send threshold alerts. That covers the alert path
  for a small number of sensors.
- Not replaceable: Spark scale batch analytics over years of history, streaming
  semantics such as windowing and micro batches, PostGIS geospatial hotspot
  queries, and infrastructure we can start, stop, and cost control ourselves.
- Platform advantages: no code, minutes to set up, fully managed.
- Platform disadvantages: per task pricing that grows with sensor count, no
  distributed compute, weak geospatial support, vendor lock in, and no
  architecture to present in a big data architectures course.

## Deadlines

- Presentation, ppt or pptx, due July 24, 2026
- Final report, doc or docx, due August 7, 2026

## Technology stack

The same Python and SQL jobs run in both places. Only the infrastructure around
them changes: Docker locally, managed GCP services in the cloud.

### Local stack

| Technology | Used for | Why |
| --- | --- | --- |
| Python and SQL | The fetchers, Spark jobs, and queries | Taught in the course, standard for data work |
| Apache Kafka (single broker) | Streaming ingestion bus | A real streaming layer, spun up only for a demo, no managed bill |
| Apache Spark Structured Streaming | Real time AQI alerts | Course engine, one tool for both stream and batch |
| Apache Spark batch | Monthly and seasonal trends | Same engine mining the history |
| Apache Airflow | Orchestrates the batch lane DAG | Shows real orchestration, free to run locally |
| PostgreSQL with PostGIS | Stores alerts, readings, trends, and the geospatial points | Serves results and powers the hotspot map |
| Python http.server with Leaflet | Live web dashboard at :8050 | See data arrive in real time, no cloud needed |
| Docker Compose and `local.sh` | Run the whole stack with one command | Reproducible and host independent |

### Production stack (GCP)

| Technology | Used for | Why |
| --- | --- | --- |
| Dataproc | Managed Spark cluster, with Kafka installed | Distributed Spark for scale, ephemeral so it bills only while up |
| Cloud SQL for PostgreSQL with PostGIS | Managed database | The same PostGIS, now managed and durable |
| Cloud Storage | Raw and finished data | The Amazon S3 replacement, native to GCP |
| Cloud SQL Auth Proxy | Secure database access from Dataproc | No public IP, jobs reach the DB on localhost |
| Looker Studio | Client dashboards | Free and native to GCP, replaces Power BI |
| `gcloud` and `gcp.sh` | Full lifecycle and cost control | One script to bring the demo up and down |

We do not use Cloud Composer (managed Airflow) because it runs all day and would
drain the free credit. Airflow runs locally instead.

## Assignment prompts

Two prompts, one per deliverable. Paste into Claude when the deliverable is due
or whenever the stack changes. Both read the repo, so they always reflect what
is actually built.

### Prompt 1: presentation, due July 24, 2026

```
Read this repo: README.md, CLAUDE.md, docker-compose.yml, local.sh, gcp.sh, and
the fetchers, spark, airflow, and sql folders. From the current state of the
code, generate a PowerPoint (.pptx) for the Real Time Air Quality Monitoring
project, CISC 525 Big Data Architectures. Required sections, in order:
1. Motivation
2. Problem statement and project objective
3. Data information: OpenAQ, EPA AirNow, PurpleAir, what each provides
4. Proposed architecture, data flow, and workflow as before and after: before
   is batch only monitoring that detects spikes hours late, after is the two
   lane design with a real time Kafka and Spark Streaming lane plus a Spark
   batch lane orchestrated by Airflow, both feeding PostGIS
5. Tools: each layer and the GCP service used for it, and why each was chosen
6. Experiments: what we tested and how, per the Experiments section of the
   README, including detection latency measurement
7. Results and deliverables: alert latency numbers, alert and trend outputs,
   dashboards, and the cost controlled pipeline
8. AI platform investigation: whether this project could be replaced by Google
   Opal, AWS App Studio, Microsoft Power Platform, n8n, or Zapier, using the
   AI platform investigation section of the README, with advantages and
   disadvantages both ways
9. References
Final slide: an appendix titled Regeneration Prompt containing this exact
prompt verbatim, so the deck can always be rebuilt from the repo.
Keep every claim factual to the repo. Use plain confident language, no filler.
Output a downloadable .pptx.
```

### Prompt 2: final report, due August 7, 2026

```
Read this repo: README.md, CLAUDE.md, docker-compose.yml, local.sh, gcp.sh, and
the fetchers, spark, airflow, and sql folders. From the current state of the
code, write the final report for the Real Time Air Quality Monitoring project,
CISC 525 Big Data Architectures, as a downloadable .docx. Required sections:
1. Introduction: the public health motivation and the project objective
2. Literature review and technical background, minimum 7 references: Lambda
   architecture, Apache Kafka, Apache Spark and Structured Streaming, Apache
   Airflow, PostgreSQL with PostGIS, the WHO air pollution statistics, and the
   OpenAQ, AirNow, and PurpleAir data platforms
3. System implementation: the two lane architecture, each component and the
   GCP service behind it, the local Docker environment versus the Dataproc
   cloud demo, and how local.sh and gcp.sh control the full lifecycle and cost
4. Experiments: the procedure from the Experiments section of the README
5. Results: detection latency, alert and trend outputs, dashboards, and cost
   figures from the demo runs
6. AI platform investigation: whether this project could be replaced by Google
   Opal, AWS App Studio, Microsoft Power Platform, n8n, or Zapier, using the
   AI platform investigation section of the README, with advantages and
   disadvantages both ways
7. Conclusion and future works: multi broker Kafka, Cloud Composer, the full
   EPA AQI formula, AirNow and PurpleAir fetchers, and always on production
   hosting
Write in our own words, cite all references properly, keep every claim factual
to the repo, and use clear plain English. Output a downloadable .docx.
```
