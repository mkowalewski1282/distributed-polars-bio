# Praca z GCP z poziomu VS Code (bez zadnego IDE od Google)

Krotka odpowiedz na pytanie „czy trzeba korzystac z jakiegos IDE od Googla":
**nie trzeba**. Sa cztery drogi; wszystkie pozwalaja zostac przy VS Code.

## 1. VS Code lokalnie + `gcloud` z terminala (najprostsze)

Kodzisz dokladnie jak dzis w WSL, a wdrazasz komenda. Nic nowego do nauczenia
poza samym `gcloud`. To wystarcza do wszystkiego, co jest w `deploy/`.

```bash
curl https://sdk.cloud.google.com | bash && exec -l $SHELL
gcloud init
```

## 2. Rozszerzenie Cloud Code (oficjalne, darmowe)

Rozszerzenie Google do VS Code. Wciaga do IDE obsluge GKE i Cloud Run, Skaffold,
`kubectl` oraz uwierzytelnianie. Przydatne, gdy duzo pracujesz z Kubernetesem
(czyli przy Sailu) — podglad podow i logow bez przelaczania sie do terminala.
Dalej jest to zwykly VS Code.

## 3. VS Code Remote-SSH do maszyny GCE (rekomendowane dla tego projektu)

Ten sam model pracy co dzis z WSL, tylko „maszyna" stoi w chmurze.
**Rozwiazuje problem 3.5 GB RAM** — na `e2-standard-4` (16 GB) kompilacja Rusta
idzie rownolegle zamiast `CARGO_BUILD_JOBS=1`, a kilkudziesieciominutowe buildy
schodza do kilku minut.

```bash
# raz, generuje klucze i dopisuje wpisy do ~/.ssh/config
gcloud compute config-ssh
```

Potem w VS Code: `Ctrl+Shift+P` -> `Remote-SSH: Connect to Host` -> wybierz
`ballista-dev.<zona>.<projekt>`.

**Pulapka specyficzna dla WSL** (warto o niej wiedziec z gory): `gcloud` uruchomiony
w WSL zapisuje klucze do systemu plikow WSL (`~/.ssh/google_compute_engine`),
natomiast rozszerzenie Remote-SSH w VS Code dla Windows czyta
`C:\Users\<user>\.ssh\`. Jesli VS Code nie widzi hosta, skopiuj klucze:

```bash
mkdir -p /mnt/c/Users/<user>/.ssh
cp ~/.ssh/google_compute_engine* /mnt/c/Users/<user>/.ssh/
cp ~/.ssh/config /mnt/c/Users/<user>/.ssh/config    # lub dopisz recznie
```

Alternatywa bez kopiowania: uruchamiaj VS Code z WSL (`code .` w WSL) i tam
korzystaj z Remote-SSH — wtedy uzywana jest konfiguracja SSH z WSL.

## 4. Cloud Shell / Cloud Workstations (opcjonalne)

Srodowiska hostowane przez Google, dostepne z przegladarki. Do tego projektu
niepotrzebne — wymieniam dla kompletnosci, zeby bylo jasne, ze to WYBOR, a nie
wymog.

## Dane: bucket GCS zamiast kopiowania plikow

polars-bio czyta `gs://` przez OpenDAL, wiec pliki BED/VCF wystarczy wgrac raz:

```bash
gcloud storage cp data/*.csv gs://<PROJEKT>-genomics/input/
gcloud storage ls gs://<PROJEKT>-genomics/
```

Zamiast sciezki lokalnej podaje sie wtedy `gs://<bucket>/input/...`.

## Orientacyjne koszty (stan na sierpien 2026, region europe-central2)

| Zasob | Koszt |
|---|---|
| `e2-medium` (2 vCPU / 4 GB) | ok. 0,055 USD/h on-demand, ok. 0,033 USD/h spot |
| `e2-standard-4` (4 vCPU / 16 GB) | ok. 0,15 USD/h on-demand |
| Warstwa sterowania GKE | 0,10 USD/h za klaster, ale **pierwszy klaster zonalny darmowy** (kredyt ok. 74,40 USD/mies.) |
| GCS | ok. 0,02 USD za GB/mies. — przy danych testowych pomijalne |
| Nowe konto | 300 USD kredytow na 90 dni |

Dwa nawyki, ktore najbardziej obnizaja rachunek:
1. **Zatrzymuj maszyny po pracy** (`gcloud compute instances stop ...`) — placi sie
   za czas dzialania, nie za samo istnienie.
2. **Uzywaj maszyn spot do benchmarkow** (`--provisioning-model=SPOT`) — 60-70%
   taniej; ryzyko wywlaszczenia jest przy powtarzalnych testach akceptowalne.

Do biezacej kontroli: `gcloud billing accounts list` oraz budzet z alertem
mailowym ustawiony w konsoli (Billing -> Budgets & alerts).
