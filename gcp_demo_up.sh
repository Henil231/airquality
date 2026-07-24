#!/usr/bin/env bash
# One command to bring the GCP demo live for the presentation.
# Recreates Cloud NAT, starts Cloud SQL, creates the Dataproc cluster (trying
# several zones so a capacity outage cannot block you), and runs a batch job so
# there is a fresh Succeeded job to show. Run ./gcp_demo_down.sh when done.
set -euo pipefail
cd "$(dirname "$0")"
export PATH="/opt/homebrew/share/google-cloud-sdk/bin:/opt/homebrew/opt/libpq/bin:/opt/homebrew/bin:$PATH"

[ -f .env ] || { echo "no .env found"; exit 1; }
set -a; source .env; set +a
gcloud config set project "$PROJECT" >/dev/null

PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3"
INIT="gs://goog-dataproc-initialization-actions-${REGION}/kafka/kafka.sh"
INIT="$INIT,gs://goog-dataproc-initialization-actions-${REGION}/cloud-sql-proxy/cloud-sql-proxy.sh"
META="additional-cloud-sql-instances=${PROJECT}:${REGION}:${SQL_INSTANCE}=tcp:5432,enable-cloud-sql-hive-metastore=false"

echo "[1/4] Cloud NAT for private cluster egress"
gcloud compute routers describe air-nat-router --region="$REGION" >/dev/null 2>&1 || \
  gcloud compute routers create air-nat-router --network=default --region="$REGION"
gcloud compute routers nats describe air-nat --router=air-nat-router --region="$REGION" >/dev/null 2>&1 || \
  gcloud compute routers nats create air-nat --router=air-nat-router --region="$REGION" \
    --auto-allocate-nat-external-ips --nat-all-subnet-ip-ranges

echo "[2/4] Start Cloud SQL"
gcloud sql instances patch "$SQL_INSTANCE" --activation-policy=ALWAYS -q

echo "[3/4] Create Dataproc cluster (e2 machine, ZooKeeper, private IP)"
gcloud dataproc clusters delete "$CLUSTER" --region="$REGION" -q >/dev/null 2>&1 || true
created=0
for z in us-central1-a us-central1-c us-central1-f us-central1-b; do
  echo "  trying zone $z"
  if gcloud dataproc clusters create "$CLUSTER" --region="$REGION" --single-node \
      --zone="$z" --master-machine-type=e2-standard-4 --no-address \
      --image-version=2.2-debian12 --optional-components=ZOOKEEPER \
      --initialization-actions="$INIT" --scopes=cloud-platform --metadata="$META"; then
    created=1; break
  fi
  echo "  zone $z unavailable, cleaning up and trying the next one"
  gcloud dataproc clusters delete "$CLUSTER" --region="$REGION" -q >/dev/null 2>&1 || true
done
[ "$created" = 1 ] || { echo "could not create the cluster in any zone, try again in a few minutes"; exit 1; }

echo "[4/4] Submit the batch trends job"
gcloud dataproc jobs submit pyspark spark/batch_trends.py --cluster="$CLUSTER" --region="$REGION" \
  --properties="^#^spark.jars.packages=$PACKAGES" -- --pg "$PGURL"

echo
echo "==================== READY ===================="
echo "Sign in to the console as the project owner account, then show:"
echo "  Cluster:   https://console.cloud.google.com/dataproc/clusters/${CLUSTER}?region=${REGION}&project=${PROJECT}"
echo "  Jobs:      https://console.cloud.google.com/dataproc/jobs?region=${REGION}&project=${PROJECT}"
echo "  Cloud SQL: https://console.cloud.google.com/sql/instances/${SQL_INSTANCE}/overview?project=${PROJECT}"
echo
echo "To submit another job live during the demo:"
echo "  ./gcp.sh batch      (or)     ./gcp.sh stream"
echo "When finished:  ./gcp_demo_down.sh"
