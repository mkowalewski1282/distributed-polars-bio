#!/usr/bin/env bash
# Wariant dla SAILA: klaster GKE. To jedyna droga do prawdziwej, wieloprocesowej
# dystrybucji Saila (tryb KubernetesCluster) — lokalnie okazala sie nieosiagalna
# przy 3.5 GB RAM.
#
# Koszt: pierwszy klaster ZONALNY jest darmowy w zakresie oplaty za warstwe
# sterowania (kredyt ok. 74,40 USD/mies. na billing account) — placi sie tylko
# za wezly. Klaster REGIONALNY tego kredytu NIE dostaje, wiec swiadomie
# uzywamy --zone, nie --region.
set -euo pipefail

PROJECT="${PROJECT:?ustaw PROJECT}"
REGION="${REGION:-europe-central2}"
ZONE="${ZONE:-${REGION}-a}"
CLUSTER="${CLUSTER:-sail-genomics}"
NODES="${NODES:-2}"
MACHINE="${MACHINE:-e2-standard-2}"   # 2 vCPU / 8 GB na wezel

gcloud container clusters create "$CLUSTER" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --num-nodes="$NODES" \
    --machine-type="$MACHINE" \
    --disk-size=50 \
    --enable-autoscaling --min-nodes=1 --max-nodes=4 \
    --no-enable-basic-auth --no-issue-client-certificate

gcloud container clusters get-credentials "$CLUSTER" --zone="$ZONE" --project="$PROJECT"

echo
echo "Klaster gotowy. Wdrozenie Saila:"
echo "  # 1. zbuduj i wypchnij obraz"
echo "  docker build -t ${REGION}-docker.pkg.dev/${PROJECT}/genomics/sail-genomics:latest \\"
echo "               -f deploy/sail/Dockerfile ."
echo "  docker push ${REGION}-docker.pkg.dev/${PROJECT}/genomics/sail-genomics:latest"
echo "  # 2. podmien 'image:' w deploy/sail/driver.yaml na powyzszy adres"
echo "  # 3. wdroz"
echo "  kubectl apply -k deploy/sail/"
echo "  kubectl logs -f sail-driver"
echo
echo "SPRZATANIE (wezly kosztuja, dopoki klaster istnieje):"
echo "  gcloud container clusters delete $CLUSTER --zone=$ZONE"
