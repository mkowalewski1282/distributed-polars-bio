#!/usr/bin/env bash
# Wariant NAJPROSTSZY: jedna maszyna wirtualna, na niej klaster Ballista przez
# docker compose. Rekomendowany na pierwsze wyjscie poza localhost — nie wymaga
# Kubernetesa, a daje prawdziwe procesy i prawdziwa siec.
#
# Bonus: ta sama maszyna rozwiazuje problem 3.5 GB RAM lokalnej maszyny —
# mozna sie do niej podlaczyc z VS Code przez Remote-SSH i pracowac tak samo
# jak dzis w WSL, tylko z 16 GB RAM (patrz 30_vscode_remote.md).
set -euo pipefail

PROJECT="${PROJECT:?ustaw PROJECT}"
ZONE="${ZONE:-europe-central2-a}"
VM="${VM:-ballista-dev}"
# e2-standard-4 = 4 vCPU / 16 GB. Ok. 0,15 USD/h on-demand.
# Do samych benchmarkow warto rozwazyc --provisioning-model=SPOT (~60-70% taniej,
# ryzyko: maszyna moze zostac wywlaszczona — dla powtarzalnych testow akceptowalne).
MACHINE="${MACHINE:-e2-standard-4}"

gcloud compute instances create "$VM" \
    --project="$PROJECT" \
    --zone="$ZONE" \
    --machine-type="$MACHINE" \
    --image-family=ubuntu-2204-lts \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size=100GB \
    --boot-disk-type=pd-balanced \
    --scopes=storage-rw,logging-write,monitoring-write \
    --metadata=startup-script='#!/bin/bash
set -e
apt-get update
apt-get install -y docker.io docker-compose-plugin git
usermod -aG docker ubuntu
systemctl enable --now docker
'

echo
echo "Maszyna $VM tworzy sie. Polaczenie:"
echo "  gcloud compute ssh $VM --zone=$ZONE"
echo
echo "PAMIETAJ o zatrzymaniu, gdy nie jest uzywana (placi sie za czas dzialania):"
echo "  gcloud compute instances stop $VM --zone=$ZONE"
echo "  gcloud compute instances delete $VM --zone=$ZONE   # calkowite usuniecie"
