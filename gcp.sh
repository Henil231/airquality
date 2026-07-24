#!/usr/bin/env bash
# Control the GCP demo stack. Full lifecycle in one script.
#   provision  one time: enable APIs, make bucket and Cloud SQL
#   up         before a demo: start Cloud SQL and an ephemeral Dataproc cluster
#   stream     submit the streaming alert job
#   batch      submit the batch trends job
#   down       after a demo: delete the cluster, stop Cloud SQL (near zero idle cost)
#   destroy    tear everything down permanently
#   status     show what is running and billing
set -euo pipefail
cd "$(dirname "$0")"
[ -f .env ] || { echo "copy .env.example to .env first"; exit 1; }
set -a; source .env; set +a
gcloud config set project "$PROJECT" >/dev/null

PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3"
INIT="gs://goog-dataproc-initialization-actions-${REGION}/kafka/kafka.sh"
INIT="$INIT,gs://goog-dataproc-initialization-actions-${REGION}/cloud-sql-proxy/cloud-sql-proxy.sh"

case "${1:-}" in
  provision)
    gcloud services enable dataproc.googleapis.com sqladmin.googleapis.com \
      storage.googleapis.com compute.googleapis.com
    gcloud storage buckets create "gs://$BUCKET" --location="$REGION" || true
    gcloud sql instances create "$SQL_INSTANCE" --database-version=POSTGRES_16 \
      --tier=db-g1-small --region="$REGION" --storage-size=10 --edition=ENTERPRISE || true
    gcloud sql users set-password postgres --instance="$SQL_INSTANCE" --password="$PGPASS"
    gcloud sql databases create air --instance="$SQL_INSTANCE" || true
    echo "load the schema once:"
    echo "  gcloud sql connect $SQL_INSTANCE --user=postgres --database=air < sql/schema.sql"
    ;;
  up)
    gcloud sql instances patch "$SQL_INSTANCE" --activation-policy=ALWAYS -q
    gcloud dataproc clusters create "$CLUSTER" --region="$REGION" --single-node \
      ${ZONE:+--zone="$ZONE"} ${MACHINE:+--master-machine-type="$MACHINE"} ${NO_ADDRESS:+--no-address} \
      --image-version=2.2-debian12 --optional-components=ZOOKEEPER \
      --initialization-actions="$INIT" \
      --scopes=cloud-platform \
      --metadata="additional-cloud-sql-instances=${PROJECT}:${REGION}:${SQL_INSTANCE}=tcp:5432,enable-cloud-sql-hive-metastore=false"
    echo "up. broker: ${CLUSTER}-m:9092  cloud sql via proxy on localhost:5432"
    echo "next: ./gcp.sh batch"
    ;;
  stream)
    gcloud dataproc jobs submit pyspark spark/stream_alerts.py \
      --cluster="$CLUSTER" --region="$REGION" --properties="^#^spark.jars.packages=$PACKAGES" \
      -- --bootstrap "${CLUSTER}-m:9092" --topic readings --pg "$PGURL"
    ;;
  batch)
    gcloud dataproc jobs submit pyspark spark/batch_trends.py \
      --cluster="$CLUSTER" --region="$REGION" --properties="^#^spark.jars.packages=$PACKAGES" \
      -- --pg "$PGURL"
    ;;
  down)
    gcloud dataproc clusters delete "$CLUSTER" --region="$REGION" -q || true
    gcloud sql instances patch "$SQL_INSTANCE" --activation-policy=NEVER -q
    echo "verifying nothing is left running:"
    gcloud dataproc clusters list --region="$REGION"
    gcloud sql instances list --format="table(name, state)"
    echo "expect no clusters and the sql instance STOPPED"
    ;;
  destroy)
    gcloud dataproc clusters delete "$CLUSTER" --region="$REGION" -q || true
    gcloud sql instances delete "$SQL_INSTANCE" -q || true
    gcloud storage rm -r "gs://$BUCKET" || true
    ;;
  status)
    gcloud dataproc clusters list --region="$REGION" || true
    gcloud sql instances list || true
    ;;
  *)
    echo "usage: ./gcp.sh {provision|up|stream|batch|down|destroy|status}"
    exit 1
    ;;
esac
