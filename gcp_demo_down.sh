#!/usr/bin/env bash
# Tear the GCP demo down after the presentation so nothing bills.
# Deletes the cluster, stops Cloud SQL, removes Cloud NAT. Cloud SQL and the
# bucket stay (stopped, pennies) so you can spin up again. Use ./gcp.sh destroy
# to delete those too, once the course is over.
set -euo pipefail
cd "$(dirname "$0")"
export PATH="/opt/homebrew/share/google-cloud-sdk/bin:/opt/homebrew/bin:$PATH"

[ -f .env ] || { echo "no .env found"; exit 1; }
set -a; source .env; set +a
gcloud config set project "$PROJECT" >/dev/null

echo "[1/3] Delete the Dataproc cluster"
gcloud dataproc clusters delete "$CLUSTER" --region="$REGION" -q || true

echo "[2/3] Stop Cloud SQL"
gcloud sql instances patch "$SQL_INSTANCE" --activation-policy=NEVER -q

echo "[3/3] Remove Cloud NAT"
gcloud compute routers nats delete air-nat --router=air-nat-router --region="$REGION" -q >/dev/null 2>&1 || true
gcloud compute routers delete air-nat-router --region="$REGION" -q >/dev/null 2>&1 || true

echo
echo "==================== TORN DOWN ===================="
echo "verifying nothing is left running:"
gcloud dataproc clusters list --region="$REGION"
gcloud sql instances list --format="table(name,state)"
echo "expect no clusters and the sql instance STOPPED"
