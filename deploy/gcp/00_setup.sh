#!/usr/bin/env bash
# Jednorazowa konfiguracja projektu GCP. Uruchamiac z terminala WSL — nie
# potrzeba zadnego IDE od Google, wystarczy `gcloud`.
#
# Instalacja gcloud w WSL (raz):
#   curl https://sdk.cloud.google.com | bash && exec -l $SHELL
#   gcloud init
#
set -euo pipefail

PROJECT="${PROJECT:?ustaw PROJECT=<id-projektu-gcp>}"
REGION="${REGION:-europe-central2}"        # Warszawa — najblizej, najnizsze opoznienia
ZONE="${ZONE:-${REGION}-a}"
BUCKET="${BUCKET:-${PROJECT}-genomics}"

gcloud config set project "$PROJECT"
gcloud config set compute/region "$REGION"
gcloud config set compute/zone "$ZONE"

# API wlaczamy raz; bez nich kolejne komendy odbijaja sie bledem uprawnien.
gcloud services enable \
    compute.googleapis.com \
    container.googleapis.com \
    artifactregistry.googleapis.com \
    storage.googleapis.com

# Bucket na dane wejsciowe (BED/VCF) i wyniki. polars-bio czyta gs:// przez
# OpenDAL, wiec nie trzeba niczego kopiowac na maszyny.
gcloud storage buckets create "gs://${BUCKET}" --location="$REGION" || \
    echo "bucket juz istnieje — pomijam"

# Rejestr obrazow Dockera.
gcloud artifacts repositories create genomics \
    --repository-format=docker --location="$REGION" || \
    echo "repozytorium juz istnieje — pomijam"

gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet

echo
echo "Gotowe."
echo "  PROJECT = $PROJECT"
echo "  REGION  = $REGION"
echo "  BUCKET  = gs://$BUCKET"
echo "  OBRAZY  = ${REGION}-docker.pkg.dev/${PROJECT}/genomics"
