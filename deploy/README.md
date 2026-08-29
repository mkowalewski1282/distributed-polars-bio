# Wdrożenie: od localhost do GCP

Artefakty przygotowane w Fazie I. **Nic tu jeszcze nie zostało uruchomione na
GCP** — to gotowy punkt startu na moment, gdy będzie dostęp do projektu/budżetu
(kwestia do ustalenia na dalszym etapie).

## Odpowiedź na pytanie „czy trzeba IDE od Google"

Nie. Pełne omówienie czterech dróg (VS Code + `gcloud`, rozszerzenie Cloud Code,
VS Code Remote-SSH do maszyny w chmurze, Cloud Shell) wraz z pułapką dotyczącą
kluczy SSH w WSL: [`gcp/30_vscode_remote.md`](gcp/30_vscode_remote.md).

Rekomendacja dla tego projektu: **VS Code Remote-SSH do maszyny GCE**. To ten
sam model pracy co dziś z WSL, ale rozwiązuje przy okazji ograniczenie 3,5 GB
RAM, które przez całą pracę wymuszało `CARGO_BUILD_JOBS=1` i uniemożliwiło
lokalny eksperyment z Kubernetesem.

## Dlaczego dwa różne modele wdrożenia

Nie z wygody — wynika to z architektur obu silników:

| | Ballista | Sail |
|---|---|---|
| Model | scheduler + executory | driver + workery |
| Wystarczy | maszyny GCE + Docker Compose | **wymagany Kubernetes** |
| Dlaczego | Ballista ma udokumentowane wdrożenia Docker/Docker Compose/Kubernetes | Sail ma tylko dwie implementacje `WorkerManager`: `LocalWorkerManager` (workery jako aktory w JEDNYM procesie — używana zarówno przez tryb `local`, jak i `local-cluster`) oraz `KubernetesWorkerManager` |

Ustalenie o Sailu jest potwierdzone empirycznie, nie tylko z lektury źródeł:
zrzut procesów w trakcie działania trybu `local-cluster` pokazał, że wszystkie
cztery role (driver, worker 1, worker 2, serwer Spark Connect) należą do tego
samego PID-u.

## Kolejność uruchamiania

### Krok 0 — lokalnie, przed jakimikolwiek kosztami

```bash
docker compose -f deploy/ballista/docker-compose.yml up --build
```

Łapie błędy pakowania i konfiguracji sieci za darmo. Na maszynie 3,5 GB dodaj
`--build-arg CARGO_JOBS=1`, inaczej kompilacja Rusta wyczerpie pamięć.

> **Do zrobienia przed tym krokiem:** obecny kod tworzy klaster przez
> `SessionContext::standalone_with_state()` (scheduler i executor w jednym
> procesie). Dla trybu wielokontenerowego trzeba dodać wariant „połącz się
> z istniejącym schedulerem" (`remote_with_state("df://scheduler:50050", state)`).
> Kodeki rejestruje się identycznie — zmiana dotyczy wyłącznie sposobu tworzenia
> sesji w `runner::run()`.

### Krok 1 — konfiguracja projektu GCP (raz)

```bash
export PROJECT=<id-projektu>
bash deploy/gcp/00_setup.sh
```

Włącza potrzebne API, tworzy bucket GCS na dane i rejestr obrazów Dockera.

### Krok 2a — Ballista na maszynie GCE

```bash
PROJECT=<id> bash deploy/gcp/10_vm_ballista.sh
gcloud compute ssh ballista-dev --zone=europe-central2-a
```

### Krok 2b — Sail na GKE

```bash
PROJECT=<id> bash deploy/gcp/20_gke_sail.sh
# zbuduj i wypchnij obraz, podmień 'image:' w deploy/sail/driver.yaml
kubectl apply -k deploy/sail/
kubectl logs -f sail-driver
```

## Dane

polars-bio czyta `gs://` przez OpenDAL, więc pliki wgrywa się raz do bucketa
i podaje ścieżkę `gs://...` zamiast lokalnej — bez kopiowania na maszyny.

## Koszty — dwa nawyki, które robią największą różnicę

1. **Zatrzymuj maszyny po pracy** (`gcloud compute instances stop`) — płaci się
   za czas działania, nie za samo istnienie.
2. **Maszyny spot do benchmarków** (`--provisioning-model=SPOT`) — 60–70% taniej.

Nowe konto dostaje 300 USD kredytów na 90 dni, a pierwszy **zonalny** klaster
GKE ma darmową warstwę sterowania (dlatego skrypt używa `--zone`, nie `--region`
— klaster regionalny tego kredytu nie dostaje). Szczegółowa tabelka kosztów:
[`gcp/30_vscode_remote.md`](gcp/30_vscode_remote.md).

Warto od razu ustawić budżet z alertem mailowym: Billing → Budgets & alerts.
