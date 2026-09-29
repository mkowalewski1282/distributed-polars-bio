# P0 — Ballista w osobnych procesach: plan implementacji

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Udowodnić czterema niezależnymi dowodami, że operacje genomiczne w Ballistcie wykonują się naprawdę rozproszone — scheduler i dwa executory jako osobne procesy — bez żadnych pomiarów czasu.

**Architecture:** Nowa binarka `ballista_node` uruchamia scheduler albo executor Ballisty 53 z koderami operacji genomicznych i bio-owym stanem sesji (w trybie standalone ten stan przechodził niejawnie od klienta; osobny proces musi go zbudować sam). Klient `dist_ops` dostaje tryb zdalny (zmienna `BALLISTA_SCHEDULER_URL`). Test pytest stawia klaster z procesów natywnych (bez Dockera), uruchamia pięć operacji i sprawdza: osobne PID-y, zgodność z wyrocznią polars-bio, pliki etapów w katalogach roboczych OBU executorów, a osobno — błąd zapytania, gdy executor nie ma koderów.

**Tech Stack:** Rust 2024, Ballista 53.0.0 (`ballista-scheduler`, `ballista-executor`, `ballista-core`), DataFusion 53, `tracing-subscriber` 0.3; Python 3.10, pytest, pandas, polars-bio 0.28 (tylko wyrocznie).

**Spec:** `docs/superpowers/specs/2026-09-29-metodyka-benchmarkow-design.md` — sekcja 9.1 (P0). Uzasadnienia decyzji: `raporty/podsumowanie_brainstorming.md`, sekcje 2.1 i 4.

## Global Constraints

- Budowanie ZAWSZE z `CARGO_BUILD_JOBS=1` (maszyna ma 3,5 GB RAM; bez tego linkowanie wywraca WSL).
- Nie uruchamiać Pythona z polars-bio równolegle z budowaniem Rusta.
- Ballista `53.0.0`, DataFusion `=53.0.0` — bez forkowania i modyfikowania Ballisty/Saila; zvendorowana biblioteka `datafusion-bio-function-ranges` bez zmian (tylko istniejące łatki widoczności).
- P0 **bez pomiarów czasu** — wyłącznie dowód rozproszenia.
- Procesy natywne, **bez Dockera**.
- 2 executory × **2 sloty zadań**.
- Wszystkie procesy klastra i klient działają z katalogu roboczego `ballista_genomics/` — ładunki planu przenoszą **ścieżki względne** do plików danych.
- Nowe zależności Rusta wyłącznie w wersjach już obecnych w `Cargo.lock` (bez pobierania nowych crate'ów).
- Komentarze w kodzie i dokumentacja po polsku (konwencja repozytorium); komunikaty commitów po polsku bez znaków diakrytycznych, zakończone linią `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- W repozytorium nic o komunikacji z promotorem (sformułowania neutralne: „założenia”, „do ustalenia”).

## Review Focus

Wejścia i warunki, których główne dowody nie sprawdzają, a które najłatwiej ugryzą osobę używającą tych narzędzi:

1. **Klient wskazuje scheduler, który nie działa** — oczekiwany błąd w rozsądnym czasie, nie zawieszenie. Test: `test_client_fails_fast_when_scheduler_is_down` (Zadanie 2).
2. **Pusta zmienna `BALLISTA_SCHEDULER_URL=""`** — ma znaczyć „brak zmiennej” (tryb standalone), nie próbę połączenia z pustym adresem. Test: `test_empty_scheduler_url_means_standalone` (Zadanie 2).
3. **Literówka lub brak flagi przy `ballista_node`** — kod wyjścia 2 i instrukcja użycia, nigdy start z wartościami domyślnymi. Test: `test_ballista_node_rejects_bad_arguments` (Zadanie 1).
4. **Węzeł pada przy starcie klastra** — pomocnik ma wskazać, który proces padł i gdzie są logi, zamiast czekać w nieskończoność. Test: `test_start_cluster_reports_crashed_process` (Zadanie 1).
5. **Osierocone procesy po teście** — zatrzymanie klastra ma zakończyć wszystkie procesy (inaczej kolejne uruchomienia walczą o pamięć i porty). Test: `test_cluster_stop_terminates_all_processes` (Zadanie 1).

## Miejsce w całości prac

To pierwszy z kilku planów realizujących specyfikację. Kolejne powstaną po zakończeniu poprzedniego, bo każdy korzysta z jego wniosków:

1. **P0 — Ballista w osobnych procesach** (ten plan).
2. Dane: pobranie `databio-8p`, przejście z CSV na Parquet w obu ścieżkach.
3. Narzędzie pomiarowe: metryki `/proc`, suma kontrolna (Python + Rust), runnery, orkiestrator, konfiguracje YAML, smoke; limity gRPC; instrumentacja Saila; parametr `algorithm`.
4. Próba Saila na Kubernetesie (ograniczona czasowo; wymaga zmiany `.wslconfig`).

## Struktura plików

| Plik | Rola |
|---|---|
| `ballista_genomics/Cargo.toml` | bezpośrednie zależności na `ballista-core`, `ballista-scheduler`, `ballista-executor`, `tracing-subscriber` (wersje z `Cargo.lock`) |
| `ballista_genomics/src/cluster.rs` (nowy) | jedno źródło konfiguracji węzłów: kodery, konfiguracja sesji z koderami, bio-owy stan sesji |
| `ballista_genomics/src/lib.rs` | rejestracja modułu `cluster` |
| `ballista_genomics/src/runner.rs` | korzysta z `cluster`; tryb zdalny przez `BALLISTA_SCHEDULER_URL` |
| `ballista_genomics/src/bin/ballista_node.rs` (nowy) | proces węzła: rola `scheduler` albo `executor`, ścisłe parsowanie argumentów, logi na stderr |
| `tests/test_ballista_multiprocess.py` (nowy) | pomocniki klastra + testy czterech dowodów i przypadków brzegowych |
| `ballista_genomics/OPIS.md` | sekcja P0: jak uruchomić ręcznie, co dowodzą testy, tabela dowodów |
| `deploy/ballista/docker-compose.yml` | aktualizacja nieaktualnego komentarza „DO ZROBIENIA” |

Testy są czarnoskrzynkowe w Pythonie (konwencja repozytorium): osobny harness testów Rusta oznaczałby dodatkowe, kosztowne linkowanie na tej maszynie.

---

### Task 1: Binarka `ballista_node` — scheduler i executor jako osobne procesy

**Files:**
- Modify: `ballista_genomics/Cargo.toml` (sekcja `[dependencies]`)
- Create: `ballista_genomics/src/cluster.rs`
- Modify: `ballista_genomics/src/lib.rs`
- Modify: `ballista_genomics/src/runner.rs:1-20` (importy) i `ballista_genomics/src/runner.rs:130-146` (budowa stanu sesji w `run`)
- Create: `ballista_genomics/src/bin/ballista_node.rs`
- Test: `tests/test_ballista_multiprocess.py` (nowy)

**Interfaces:**
- Consumes: `runner::bio_session_config() -> SessionConfig`; `logical_codec::BioDistLogicalCodec`; `bio_phys_codec::BioRangesPhysicalCodec`; `datafusion_bio_function_ranges::BioSessionExt::new_with_bio`.
- Produces:
  - `ballista_genomics::cluster::bio_logical_codec() -> Arc<dyn LogicalExtensionCodec>`
  - `ballista_genomics::cluster::bio_physical_codec() -> Arc<dyn PhysicalExtensionCodec>`
  - `ballista_genomics::cluster::bio_ballista_config() -> SessionConfig` (bio + oba kodery)
  - `ballista_genomics::cluster::bio_session_state(config: SessionConfig) -> datafusion::error::Result<SessionState>`
  - CLI: `ballista_node scheduler --port <P>` oraz `ballista_node executor --scheduler-port <P> --port <F> --grpc-port <G> --work-dir <DIR> --concurrent-tasks <N>`; błąd argumentów → kod 2 i „użycie” na stderr; błąd działania → kod 1.
  - Python (`tests/test_ballista_multiprocess.py`): `_require(binary)`, `_free_port() -> int`, `Cluster` (pola `scheduler_port`, `log_dir`, `procs`, `work_dirs`; `url`, `pids()`, `stop()`), `_registered_executors(port) -> list[dict]`, `_start_cluster(log_dir, executors: list[tuple[str, list[str]]]) -> Cluster`, fixture `cluster` (scope `module`, executory `executor_1`, `executor_2`).

- [ ] **Step 1: Napisz testy klastra (najpierw test)**

Utwórz `tests/test_ballista_multiprocess.py`:

```python
"""
Zadanie P0: dowód, że Ballista wykonuje operacje genomiczne w OSOBNYCH
procesach. Scheduler i dwa executory startują z binarki `ballista_node`,
klient `dist_ops` łączy się z nimi przez zmienną BALLISTA_SCHEDULER_URL.

Cztery dowody (specyfikacja metodyki, sekcja 9.1):
  1. osobne procesy — różne PID-y schedulera, executorów i klienta, a scheduler
     widzi dwa zarejestrowane executory (REST: /api/executors);
  2. wynik zgodny z wyrocznią polars-bio (te same wyrocznie co testy standalone);
  3. praca na OBU executorach — executor zapisuje dane każdego policzonego etapu
     w SWOIM katalogu roboczym (work_dir/job_id/stage_id/...), więc katalogi
     pokazują wprost, kto co liczył;
  4. kontrola negatywna — executor bez koderów (--no-codecs) nie potrafi
     zdekodować planu, więc zapytanie musi zakończyć się błędem.

Bez pomiarów czasu — P0 pyta „czy rozproszone”, nie „jak szybko”.

Brak binarek to BŁĄD, nie pominięcie testu: dowód P0 nie może cicho zniknąć
z wyników. Budowanie: cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build
--bin ballista_node --bin dist_ops

Uruchomienie: pytest tests/test_ballista_multiprocess.py -v
Testy operacji importują wyrocznie polars-bio — import trwa kilka minut.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import time
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

import pytest

BALLISTA_DIR = Path(__file__).resolve().parent.parent / "ballista_genomics"
TARGET_DIR = BALLISTA_DIR / "target" / "debug"
NODE_BINARY = TARGET_DIR / "ballista_node"
DIST_BINARY = TARGET_DIR / "dist_ops"
OUTPUT_DIR = BALLISTA_DIR / "output"
BUILD_HINT = (
    "cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build "
    "--bin ballista_node --bin dist_ops"
)


def _require(binary: Path) -> None:
    if not binary.exists():
        pytest.fail(f"brak binarki {binary.name} — zbuduj: {BUILD_HINT}")


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@dataclass
class Cluster:
    scheduler_port: int
    log_dir: Path
    procs: dict[str, subprocess.Popen] = field(default_factory=dict)
    work_dirs: dict[str, Path] = field(default_factory=dict)

    @property
    def url(self) -> str:
        return f"df://localhost:{self.scheduler_port}"

    def pids(self) -> dict[str, int]:
        return {name: p.pid for name, p in self.procs.items()}

    def stop(self) -> None:
        for p in self.procs.values():
            if p.poll() is None:
                p.terminate()
        for p in self.procs.values():
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()


def _spawn(args: list[str], log: Path) -> subprocess.Popen:
    with log.open("w") as f:
        return subprocess.Popen(
            [str(NODE_BINARY), *args],
            cwd=BALLISTA_DIR,
            stdout=f,
            stderr=subprocess.STDOUT,
        )


def _registered_executors(port: int) -> list[dict]:
    url = f"http://127.0.0.1:{port}/api/executors"
    with urllib.request.urlopen(url, timeout=2) as response:
        return json.loads(response.read())


def _wait_for_executors(cluster: Cluster, expected: int, timeout: float = 60.0) -> list[dict]:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        for name, p in cluster.procs.items():
            if p.poll() is not None:
                pytest.fail(
                    f"proces {name} zakończył się przedwcześnie (kod {p.returncode}); "
                    f"logi w {cluster.log_dir}"
                )
        try:
            executors = _registered_executors(cluster.scheduler_port)
            if len(executors) == expected:
                return executors
        except OSError as e:
            last_error = e
        time.sleep(0.5)
    pytest.fail(
        f"scheduler nie zarejestrował {expected} executorów w {timeout:.0f} s "
        f"(ostatni błąd: {last_error}); logi w {cluster.log_dir}"
    )


def _start_cluster(log_dir: Path, executors: list[tuple[str, list[str]]]) -> Cluster:
    """Scheduler + executory jako osobne procesy; każdy executor ma własny
    katalog roboczy i 2 sloty zadań (specyfikacja, sekcja 9.1)."""
    _require(NODE_BINARY)
    cluster = Cluster(scheduler_port=_free_port(), log_dir=log_dir)
    try:
        cluster.procs["scheduler"] = _spawn(
            ["scheduler", "--port", str(cluster.scheduler_port)],
            log_dir / "scheduler.log",
        )
        for name, extra_args in executors:
            work_dir = log_dir / f"work_{name}"
            work_dir.mkdir()
            cluster.work_dirs[name] = work_dir
            cluster.procs[name] = _spawn(
                [
                    "executor",
                    "--scheduler-port", str(cluster.scheduler_port),
                    "--port", str(_free_port()),
                    "--grpc-port", str(_free_port()),
                    "--work-dir", str(work_dir),
                    "--concurrent-tasks", "2",
                    *extra_args,
                ],
                log_dir / f"{name}.log",
            )
        _wait_for_executors(cluster, expected=len(executors))
    except BaseException:
        cluster.stop()
        raise
    return cluster


@pytest.fixture(scope="module")
def cluster(tmp_path_factory):
    c = _start_cluster(
        tmp_path_factory.mktemp("p0_klaster"),
        [("executor_1", []), ("executor_2", [])],
    )
    yield c
    c.stop()


def test_ballista_node_rejects_bad_arguments():
    """Literówka w nazwie flagi albo brak wymaganej flagi nie może uruchomić
    węzła z wartościami domyślnymi — ma skończyć się kodem 2 i instrukcją użycia."""
    _require(NODE_BINARY)
    for args in (
        [],
        ["executor", "--port", "1"],
        ["scheduler", "--port", "1", "--nieznana-flaga"],
    ):
        r = subprocess.run(
            [str(NODE_BINARY), *args],
            cwd=BALLISTA_DIR,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert r.returncode == 2, f"{args}: kod {r.returncode}, stderr: {r.stderr}"
        assert "użycie" in r.stderr, f"{args}: brak instrukcji użycia: {r.stderr}"


def test_start_cluster_reports_crashed_process(tmp_path):
    """Gdy węzeł padnie przy starcie, pomocnik ma wskazać KTÓRY, zamiast czekać
    w nieskończoność na rejestrację."""
    with pytest.raises(pytest.fail.Exception, match="executor_zly"):
        _start_cluster(tmp_path, [("executor_zly", ["--nieznana-flaga"])])


def test_cluster_stop_terminates_all_processes(tmp_path):
    c = _start_cluster(tmp_path, [("executor_1", [])])
    c.stop()
    still_running = [name for name, p in c.procs.items() if p.poll() is None]
    assert not still_running, f"po stop() nadal działają: {still_running}"


def test_scheduler_sees_two_separate_executors(cluster):
    """Dowód 1 (część klastrowa): trzy osobne procesy, a scheduler widzi dwa
    RÓŻNE executory — różne identyfikatory i porty."""
    pids = cluster.pids()
    assert len(set(pids.values())) == 3, f"PID-y nie są różne: {pids}"
    assert all(p.poll() is None for p in cluster.procs.values())
    executors = _registered_executors(cluster.scheduler_port)
    assert len({e["id"] for e in executors}) == 2, executors
    assert len({e["port"] for e in executors}) == 2, executors
```

- [ ] **Step 2: Uruchom testy — mają nie przejść**

Run: `pytest tests/test_ballista_multiprocess.py -v`
Expected: 4 × FAIL z komunikatem `brak binarki ballista_node — zbuduj: ...`.

- [ ] **Step 3: Dodaj zależności**

W `ballista_genomics/Cargo.toml`, w sekcji `[dependencies]` pod linią `ballista = ...`, dodaj:

```toml
# Zadanie P0: własne procesy schedulera i executora (binarka ballista_node).
# Te crate'y i tak są już kompilowane przez `ballista` (cecha standalone),
# z domyślnymi cechami — bezpośrednia zależność z tymi samymi cechami nie
# wymusza ponownej kompilacji ani pobierania.
ballista-core = "53.0.0"
ballista-scheduler = "53.0.0"
ballista-executor = "53.0.0"
# Logi węzłów na stderr; wersja i cecha env-filter jak u ballista-scheduler.
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
```

- [ ] **Step 4: Utwórz moduł `cluster`**

Utwórz `ballista_genomics/src/cluster.rs`:

```rust
//! Jedno źródło konfiguracji węzłów klastra — klienta, schedulera i executora.
//!
//! W trybie standalone scheduler i executor dostają GOTOWY stan sesji klienta
//! (`new_standalone_scheduler_from_state` / `new_standalone_executor_from_state`):
//! bio-owego planistę, reguły optymalizatora i kodery. Osobny proces nie ma
//! dostępu do pamięci klienta, więc musi zbudować identyczny stan sam — stąd te
//! funkcje, wołane przez `runner` (klient) i `bin/ballista_node.rs` (węzły).

use std::sync::Arc;

use ballista_core::extension::SessionConfigExt;
use datafusion::error::Result;
use datafusion::execution::SessionState;
use datafusion::prelude::{SessionConfig, SessionContext as DFSessionContext};
use datafusion_bio_function_ranges::BioSessionExt;
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;

use crate::bio_phys_codec::BioRangesPhysicalCodec;
use crate::logical_codec::BioDistLogicalCodec;
use crate::runner::bio_session_config;

/// Koder planu logicznego (klient → scheduler).
pub fn bio_logical_codec() -> Arc<dyn LogicalExtensionCodec> {
    Arc::new(BioDistLogicalCodec::default())
}

/// Koder planu fizycznego (scheduler → executor). Deleguje do
/// `IntervalJoinPhysicalCodec`, a ten do domyślnego kodeka Ballisty.
pub fn bio_physical_codec() -> Arc<dyn PhysicalExtensionCodec> {
    Arc::new(BioRangesPhysicalCodec::default())
}

/// `bio_session_config()` z zarejestrowanymi oboma koderami.
pub fn bio_ballista_config() -> SessionConfig {
    bio_session_config()
        .with_ballista_logical_extension_codec(bio_logical_codec())
        .with_ballista_physical_extension_codec(bio_physical_codec())
}

/// Bio-owy stan sesji dla podanej konfiguracji. Scheduler MUSI planować na
/// takim stanie, żeby `IntervalJoinPhysicalOptimizationRule` w ogóle zadziałała.
pub fn bio_session_state(config: SessionConfig) -> Result<SessionState> {
    Ok(DFSessionContext::new_with_bio(config).state())
}
```

- [ ] **Step 5: Zarejestruj moduł w bibliotece**

W `ballista_genomics/src/lib.rs` dopisz w mapie modułów (po linii z `bio_phys_codec`):

```rust
//! - `cluster`         — kodery i stan sesji wspólne dla klienta i węzłów (P0)
```

oraz w liście modułów (po `pub mod bio_phys_codec;`):

```rust
pub mod cluster;
```

- [ ] **Step 6: Przełącz `runner` na moduł `cluster` (refaktor bez zmiany zachowania)**

W `ballista_genomics/src/runner.rs` zastąp blok importów (linie 5–21) tym:

```rust
use std::sync::Arc;
use std::time::Instant;

use ballista::prelude::SessionContextExt;
use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::prelude::{SessionConfig, SessionContext as DFSessionContext};
use datafusion_bio_function_ranges::BioConfig;

use crate::cluster::{bio_ballista_config, bio_session_state};
use crate::dist_payload::DistOp;
use crate::dist_udtf::DistTableFunction;
```

W funkcji `run` zastąp fragment od komentarza `// Sesja SCHEDULERA/klienta MUSI być bio-owa` do linii `let state = bio_ctx.state();` włącznie tym:

```rust
    // Sesja klienta MUSI być bio-owa (new_with_bio), a konfiguracja mieć oba
    // kodery — patrz cluster.rs. Ten sam stan dostaje scheduler standalone.
    let state = bio_session_state(bio_ballista_config())?;
```

(Reszta funkcji bez zmian — `standalone_with_state(state)` zostaje do Zadania 2.)

- [ ] **Step 7: Utwórz binarkę `ballista_node`**

Utwórz `ballista_genomics/src/bin/ballista_node.rs`:

```rust
//! Zadanie P0: węzeł klastra Ballista jako OSOBNY proces — scheduler albo
//! executor z koderami operacji genomicznych.
//!
//! Jedna binarka z dwiema rolami zamiast dwóch binarek: na tej maszynie
//! dominującym kosztem iteracji jest linkowanie (patrz dist_ops.rs).
//!
//! Wszystkie procesy klastra i klient muszą działać w katalogu
//! `ballista_genomics/`, bo ładunki planu przenoszą ŚCIEŻKI WZGLĘDNE do danych.

use std::collections::HashMap;
use std::error::Error;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use ballista_core::config::TaskSchedulingPolicy;
use ballista_core::extension::SessionConfigExt;
use ballista_core::ConfigProducer;
use ballista_executor::executor_process::{ExecutorProcessConfig, start_executor_process};
use ballista_genomics::cluster::{
    bio_ballista_config, bio_logical_codec, bio_physical_codec, bio_session_state,
};
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::{SchedulerConfig, TaskDistributionPolicy};
use ballista_scheduler::scheduler_process::start_server;

const USAGE: &str = "użycie:
  ballista_node scheduler --port <P>
  ballista_node executor --scheduler-port <P> --port <F> --grpc-port <G> \\
                         --work-dir <DIR> --concurrent-tasks <N>";

type Flags = HashMap<String, Option<String>>;

/// Parsuje `--klucz wartość` i flagi bez wartości. Nieznany argument to błąd:
/// literówka w nazwie flagi nie może cicho uruchomić węzła z wartością domyślną.
fn parse_flags(args: &[String], value_flags: &[&str], bool_flags: &[&str]) -> Result<Flags, String> {
    let mut flags = Flags::new();
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        if value_flags.contains(&arg.as_str()) {
            let value = it.next().ok_or_else(|| format!("brak wartości dla {arg}"))?;
            flags.insert(arg.clone(), Some(value.clone()));
        } else if bool_flags.contains(&arg.as_str()) {
            flags.insert(arg.clone(), None);
        } else {
            return Err(format!("nieznany argument: {arg}"));
        }
    }
    Ok(flags)
}

fn required<T: FromStr>(flags: &Flags, name: &str) -> Result<T, String> {
    flags
        .get(name)
        .and_then(|v| v.as_deref())
        .ok_or_else(|| format!("brak wymaganej flagi {name}"))?
        .parse::<T>()
        .map_err(|_| format!("niepoprawna wartość flagi {name}"))
}

struct ExecutorOpts {
    scheduler_port: u16,
    port: u16,
    grpc_port: u16,
    work_dir: String,
    concurrent_tasks: usize,
}

fn parse_executor(args: &[String]) -> Result<ExecutorOpts, String> {
    let flags = parse_flags(
        args,
        &["--scheduler-port", "--port", "--grpc-port", "--work-dir", "--concurrent-tasks"],
        &[],
    )?;
    Ok(ExecutorOpts {
        scheduler_port: required(&flags, "--scheduler-port")?,
        port: required(&flags, "--port")?,
        grpc_port: required(&flags, "--grpc-port")?,
        work_dir: required(&flags, "--work-dir")?,
        concurrent_tasks: required(&flags, "--concurrent-tasks")?,
    })
}

async fn run_scheduler(port: u16) -> Result<(), Box<dyn Error>> {
    let mut config = SchedulerConfig::default()
        .with_hostname("localhost")
        .with_port(port)
        // Push + round-robin: scheduler sam rozdaje zadania po kolei na
        // wszystkie executory. Przy pull (executory same pytają o pracę)
        // i milisekundowych zadaniach jeden executor potrafi zgarnąć wszystko,
        // a dowód „praca na obu executorach” byłby loterią.
        .with_scheduler_policy(TaskSchedulingPolicy::PushStaged)
        .with_task_distribution(TaskDistributionPolicy::RoundRobin)
        // 0 = nie każ executorom kasować danych zakończonych zadań: pliki etapów
        // w katalogach roboczych są dowodem, kto co liczył.
        .with_finished_job_data_clean_up_interval_seconds(0)
        // Odpowiednik stanu sesji, który standalone przekazuje od klienta.
        .with_override_config_producer(Arc::new(bio_ballista_config))
        .with_override_session_builder(Arc::new(bio_session_state));
    config.bind_host = "127.0.0.1".into();
    config.override_logical_codec = Some(bio_logical_codec());
    config.override_physical_codec = Some(bio_physical_codec());

    let addr: SocketAddr = format!("127.0.0.1:{port}").parse()?;
    let cluster = BallistaCluster::new_from_config(&config).await?;
    println!("ballista_node: scheduler nasłuchuje na {addr}");
    start_server(cluster, addr, Arc::new(config)).await?;
    Ok(())
}

async fn run_executor(o: ExecutorOpts) -> Result<(), Box<dyn Error>> {
    let config_producer: ConfigProducer = Arc::new(|| bio_ballista_config().upgrade_for_ballista());
    let config = ExecutorProcessConfig {
        bind_host: "127.0.0.1".into(),
        external_host: Some("localhost".into()),
        port: o.port,
        grpc_port: o.grpc_port,
        scheduler_host: "localhost".into(),
        scheduler_port: o.scheduler_port,
        concurrent_tasks: o.concurrent_tasks,
        task_scheduling_policy: TaskSchedulingPolicy::PushStaged,
        work_dir: Some(o.work_dir.clone()),
        override_config_producer: Some(config_producer),
        override_logical_codec: Some(bio_logical_codec()),
        override_physical_codec: Some(bio_physical_codec()),
        ..ExecutorProcessConfig::default()
    };
    println!(
        "ballista_node: executor (flight {}, grpc {}, katalog {}, sloty {})",
        o.port, o.grpc_port, o.work_dir, o.concurrent_tasks
    );
    start_executor_process(Arc::new(config)).await?;
    Ok(())
}

fn init_logging() {
    use tracing_subscriber::EnvFilter;
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();
}

fn usage_error(msg: &str) -> ! {
    eprintln!("błąd: {msg}\n{USAGE}");
    std::process::exit(2);
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let rest = args.get(1..).unwrap_or(&[]);
    // Najpierw WYŁĄCZNIE parsowanie (kod 2), potem działanie (kod 1).
    let outcome = match args.first().map(String::as_str) {
        Some("scheduler") => {
            let port = parse_flags(rest, &["--port"], &[])
                .and_then(|f| required::<u16>(&f, "--port"))
                .unwrap_or_else(|e| usage_error(&e));
            init_logging();
            run_scheduler(port).await
        }
        Some("executor") => {
            let opts = parse_executor(rest).unwrap_or_else(|e| usage_error(&e));
            init_logging();
            run_executor(opts).await
        }
        _ => usage_error("podaj rolę: scheduler albo executor"),
    };
    if let Err(e) = outcome {
        eprintln!("ballista_node: {e}");
        std::process::exit(1);
    }
}
```

- [ ] **Step 8: Zbuduj**

Run: `cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build --bin ballista_node --bin dist_ops 2>&1 | tail -20`
Expected: `Finished` bez błędów i bez ostrzeżeń o nieużywanych importach w `runner.rs`. Build przyrostowy — kilka minut (dwa linkowania). Jeśli kompilator zgłosi inną nazwę typu/ścieżki niż w kodzie wyżej, popraw wyłącznie import zgodnie z komunikatem (`rustc` podaje sugerowaną ścieżkę) — nie zmieniaj konfiguracji węzłów.

- [ ] **Step 9: Uruchom testy klastra — mają przejść**

Run: `pytest tests/test_ballista_multiprocess.py -v`
Expected: 4 × PASS. Przy porażce `test_scheduler_sees_two_separate_executors` najpierw obejrzyj logi z komunikatu (`scheduler.log`, `executor_*.log`) — nie zmieniaj testu, dopóki przyczyna nie jest znana.

- [ ] **Step 10: Sprawdź, że refaktor `runner` niczego nie zepsuł**

Run: `pytest tests/test_ballista_distributed_ops.py tests/test_ballista_distribution_evidence.py -v`
Expected: wszystkie PASS (import polars-bio trwa kilka minut).

- [ ] **Step 11: Commit**

```bash
git add ballista_genomics/Cargo.toml ballista_genomics/Cargo.lock ballista_genomics/src/cluster.rs \
        ballista_genomics/src/lib.rs ballista_genomics/src/runner.rs \
        ballista_genomics/src/bin/ballista_node.rs tests/test_ballista_multiprocess.py
git commit -F - <<'EOF'
P0: binarka ballista_node - scheduler i executor jako osobne procesy

Wspolny modul cluster (kodery, konfiguracja i bio-owy stan sesji) dla klienta
i wezlow; scheduler w trybie push z rozdzialem round-robin i bez sprzatania
danych zadan. Testy: rejestracja dwoch executorow, bledne argumenty,
raportowanie padnietego wezla, zatrzymanie wszystkich procesow.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

---

### Task 2: Tryb zdalny klienta i dowody 1–3 dla pięciu operacji

**Files:**
- Modify: `ballista_genomics/src/runner.rs` (funkcja `run`: wybór standalone/zdalny)
- Test: `tests/test_ballista_multiprocess.py` (dopisanie na końcu pliku)

**Interfaces:**
- Consumes: z Zadania 1 — `Cluster`, fixture `cluster`, `_require`, `_free_port`, `DIST_BINARY`, `OUTPUT_DIR`, `BALLISTA_DIR`; `cluster::{bio_ballista_config, bio_session_state}`.
- Produces:
  - `runner::SCHEDULER_URL_ENV: &str = "BALLISTA_SCHEDULER_URL"`; niepusta wartość → `remote_with_state`, pusta lub brak → standalone. W trybie zdalnym stdout zawiera `Łączenie z zewnętrznym schedulerem Ballisty`, w standalone — `standalone`.
  - Python: `_run_client(op, scheduler_url: str | None, timeout=180) -> tuple[subprocess.CompletedProcess, int]`, `_stages_by_job(work_dir) -> dict[str, set[str]]`, `_check_against_oracle(op)`, `_write_evidence(op, pids, new_jobs)`; plik dowodów `ballista_genomics/output/p0_dowod_<op>.json`.

- [ ] **Step 1: Dopisz testy operacji (najpierw test)**

Dopisz na końcu `tests/test_ballista_multiprocess.py`:

```python
# Kanoniczny zestaw danych — identyczny jak w tests/test_ballista_distributed_ops.py
# (tamten plik pilnuje zgodności z data/parts_a i data/parts_b).
INTERVALS_A = [
    ("chr1", 100, 200, "gene_A1"),
    ("chr1", 150, 300, "gene_A2"),
    ("chr1", 400, 500, "gene_A3"),
    ("chr2", 50, 150, "gene_A4"),
    ("chr2", 200, 350, "gene_A5"),
]
INTERVALS_B = [
    ("chr1", 180, 250, "peak_B1"),
    ("chr1", 290, 420, "peak_B2"),
    ("chr1", 450, 600, "peak_B3"),
    ("chr2", 100, 220, "peak_B4"),
    ("chr2", 300, 400, "peak_B5"),
]

#: Operacje czytające KATALOGI dwóch plików (data/parts_*): etap źródłowy ma
#: ≥ 2 zadania, więc przy rozdziale round-robin musi trafić na oba executory.
#: `overlap` czyta pojedyncze pliki (data/intervals_*.csv) i nie ma równoległości
#: hash (znane ograniczenie — sprawozdanie, sekcja 6); dla niego wymagamy ≥ 1
#: executora, a faktyczny rozkład trafia do pliku dowodów.
MULTI_TASK_OPS = {"merge", "subtract", "nearest", "coverage"}
#: Operacje z hash-shuffle po chromosomie — plan musi mieć ≥ 2 etapy.
SHUFFLE_OPS = {"merge", "subtract"}


def _run_client(op: str, scheduler_url: str | None, timeout: int = 180):
    """Uruchamia dist_ops; zwraca (CompletedProcess, PID klienta)."""
    _require(DIST_BINARY)
    env = dict(os.environ)
    if scheduler_url is None:
        env.pop("BALLISTA_SCHEDULER_URL", None)
    else:
        env["BALLISTA_SCHEDULER_URL"] = scheduler_url
    p = subprocess.Popen(
        [str(DIST_BINARY), op],
        cwd=BALLISTA_DIR,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        out, err = p.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        p.kill()
        out, err = p.communicate()
        pytest.fail(f"dist_ops {op} przekroczyło {timeout} s\nstdout: {out}\nstderr: {err}")
    return subprocess.CompletedProcess(p.args, p.returncode, out, err), p.pid


def _stages_by_job(work_dir: Path) -> dict[str, set[str]]:
    """{job_id: {stage_id, ...}} — etapy, dla których executor zapisał pliki
    (układ Ballisty: work_dir/job_id/stage_id/...)."""
    jobs: dict[str, set[str]] = {}
    for job in work_dir.iterdir():
        if not job.is_dir():
            continue
        stages = {
            stage.name
            for stage in job.iterdir()
            if stage.is_dir() and any(f.is_file() for f in stage.rglob("*"))
        }
        if stages:
            jobs[job.name] = stages
    return jobs


def _check_against_oracle(op: str) -> None:
    """Dowód 2: wynik klienta == lokalny polars-bio (wyrocznie jak w
    tests/test_ballista_distributed_ops.py). Importy leniwe — polars-bio ładuje
    się kilka minut, a testy samego klastra go nie potrzebują."""
    import pandas as pd

    df = pd.read_csv(OUTPUT_DIR / f"dist_{op}_result.csv")
    if op == "overlap":
        from tests.overlap_oracle import normalize_engine_pairs, reference_overlap_pairs

        actual = normalize_engine_pairs(df, "name_a", "name_b")
        expected = reference_overlap_pairs(INTERVALS_A, INTERVALS_B)
    elif op == "merge":
        from tests.merge_oracle import reference_merge_intervals

        actual = {(r.chrom, int(r.start), int(r.end)) for r in df.itertuples()}
        expected = reference_merge_intervals(INTERVALS_A)
    elif op == "subtract":
        from tests.coverage_subtract_oracle import reference_subtract

        actual = {(r.chrom, int(r.start), int(r.end)) for r in df.itertuples()}
        expected = reference_subtract(INTERVALS_A, INTERVALS_B)
    elif op == "coverage":
        from tests.coverage_subtract_oracle import reference_coverage

        actual = {
            (r.chrom, int(r.start), int(r.end), int(r.coverage)) for r in df.itertuples()
        }
        expected = reference_coverage(INTERVALS_A, INTERVALS_B)
    elif op == "nearest":
        # Porównujemy ODLEGŁOŚCI, nie wybranych sąsiadów — remisy rozstrzygane
        # są dowolnie (patrz tests/test_nearest_correctness.py).
        from tests.nearest_oracle import reference_nearest_min_distances

        actual = {name: int(d) for name, d in zip(df["left_name"], df["distance"])}
        expected = {
            name: int(d)
            for name, d in reference_nearest_min_distances(INTERVALS_A, INTERVALS_B).items()
        }
    else:
        raise ValueError(op)
    assert actual == expected, f"{op}: wynik rozproszony {actual} != wyrocznia {expected}"


def _write_evidence(op: str, pids: dict[str, int], new_jobs: dict[str, dict[str, set[str]]]) -> None:
    """Zapisuje, który executor liczył które etapy — materiał do opisu P0."""
    evidence = {
        "operacja": op,
        "pid": pids,
        "etapy_na_executorach": {
            name: {job: sorted(stages) for job, stages in jobs.items()}
            for name, jobs in new_jobs.items()
        },
    }
    OUTPUT_DIR.mkdir(exist_ok=True)
    (OUTPUT_DIR / f"p0_dowod_{op}.json").write_text(
        json.dumps(evidence, indent=2, ensure_ascii=False)
    )


@pytest.mark.parametrize("op", ["overlap", "merge", "subtract", "nearest", "coverage"])
def test_operation_runs_distributed_across_processes(cluster, op):
    """Dowody 1–3 dla jednej operacji uruchomionej przez klaster z procesów."""
    before = {name: _stages_by_job(wd) for name, wd in cluster.work_dirs.items()}
    result, client_pid = _run_client(op, cluster.url)
    assert result.returncode == 0, (
        f"dist_ops {op}:\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )
    assert "zewnętrznym schedulerem" in result.stdout, (
        f"klient nie użył trybu zdalnego:\n{result.stdout}"
    )
    after = {name: _stages_by_job(wd) for name, wd in cluster.work_dirs.items()}
    new_jobs = {
        name: {job: st for job, st in after[name].items() if job not in before[name]}
        for name in after
    }

    # Dowód 1: cztery różne procesy.
    pids = {**cluster.pids(), "klient": client_pid}
    assert len(set(pids.values())) == 4, f"PID-y nie są różne: {pids}"

    # Dowód 2: poprawność względem polars-bio.
    _check_against_oracle(op)

    # Dowód 3: praca wykonana w executorach, a dla operacji wielozadaniowych — w OBU.
    participating = sorted(name for name, jobs in new_jobs.items() if jobs)
    assert participating, (
        f"{op}: żaden executor nie zapisał danych etapów — zapytanie nie przeszło przez klaster"
    )
    if op in MULTI_TASK_OPS:
        assert participating == sorted(cluster.work_dirs), (
            f"{op}: pracowały tylko {participating}; etapy: {new_jobs}"
        )
    if op in SHUFFLE_OPS:
        stages = set().union(*(st for jobs in new_jobs.values() for st in jobs.values()))
        assert len(stages) >= 2, f"{op}: oczekiwano ≥ 2 etapów (shuffle), są {stages}"

    _write_evidence(op, pids, new_jobs)


def test_client_fails_fast_when_scheduler_is_down():
    """Adres nieistniejącego schedulera ma dać błąd w rozsądnym czasie, nie zawieszenie."""
    result, _ = _run_client("merge", f"df://localhost:{_free_port()}", timeout=120)
    assert result.returncode != 0, f"klient zakończył się sukcesem bez schedulera:\n{result.stdout}"


def test_empty_scheduler_url_means_standalone():
    """Pusta zmienna BALLISTA_SCHEDULER_URL = brak zmiennej (tryb standalone)."""
    result, _ = _run_client("merge", "", timeout=180)
    assert result.returncode == 0, result.stderr
    assert "standalone" in result.stdout, result.stdout
```

- [ ] **Step 2: Uruchom nowe testy — mają nie przejść**

Run: `pytest tests/test_ballista_multiprocess.py -v -k "operation or scheduler_is_down or empty_scheduler"`
Expected: `test_operation_runs_distributed_across_processes[*]` → 5 × FAIL z `klient nie użył trybu zdalnego` (klient ignoruje zmienną i liczy w standalone); `test_client_fails_fast_when_scheduler_is_down` → FAIL (klient w standalone kończy się sukcesem); `test_empty_scheduler_url_means_standalone` → PASS (już dziś działa standalone — ten test pilnuje, żeby Zadanie 2 tego nie zepsuło).

- [ ] **Step 3: Dodaj tryb zdalny do `runner`**

W `ballista_genomics/src/runner.rs` dodaj pod importami:

```rust
/// Adres zewnętrznego schedulera (np. `df://localhost:50050`). Niepusta wartość
/// → klient łączy się z klastrem z osobnych procesów (`ballista_node`); pusta
/// albo brak → dotychczasowy tryb standalone (scheduler + executor in-proc).
pub const SCHEDULER_URL_ENV: &str = "BALLISTA_SCHEDULER_URL";
```

W funkcji `run` zastąp linie:

```rust
    println!("Łączenie z Ballista standalone (scheduler + executor in-proc)...");
    let ctx = DFSessionContext::standalone_with_state(state).await?;
    println!("Klaster Ballista wystartował.\n");
```

tym:

```rust
    let ctx = match std::env::var(SCHEDULER_URL_ENV) {
        Ok(url) if !url.is_empty() => {
            println!("Łączenie z zewnętrznym schedulerem Ballisty: {url}");
            DFSessionContext::remote_with_state(&url, state).await?
        }
        _ => {
            println!("Łączenie z Ballista standalone (scheduler + executor in-proc)...");
            DFSessionContext::standalone_with_state(state).await?
        }
    };
    println!("Klaster Ballista gotowy.\n");
```

- [ ] **Step 4: Zbuduj**

Run: `cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build --bin ballista_node --bin dist_ops 2>&1 | tail -5`
Expected: `Finished` bez błędów.

- [ ] **Step 5: Uruchom cały plik testów — ma przejść**

Run: `pytest tests/test_ballista_multiprocess.py -v`
Expected: wszystkie PASS; w `ballista_genomics/output/` pięć plików `p0_dowod_<op>.json`. Jeśli `test_operation_runs_distributed_across_processes[nearest]` albo `[coverage]` pokaże pracę tylko jednego executora, NIE przenoś operacji poza `MULTI_TASK_OPS` — najpierw sprawdź liczbę partycji źródłowych w `output/dist_<op>_explain.txt` (superpowers:systematic-debugging) i zapisz ustalenie. Jeśli `test_client_fails_fast_when_scheduler_is_down` przekroczy 120 s, zapisz faktyczny czas do ustaleń zamiast podnosić limit bez uzasadnienia.

- [ ] **Step 6: Commit**

```bash
git add ballista_genomics/src/runner.rs tests/test_ballista_multiprocess.py
git commit -F - <<'EOF'
P0: tryb zdalny klienta i dowody rozproszenia dla pieciu operacji

dist_ops laczy sie z zewnetrznym schedulerem, gdy ustawiono
BALLISTA_SCHEDULER_URL (pusta wartosc = standalone). Test sprawdza dla
kazdej operacji: cztery rozne procesy, zgodnosc z wyrocznia polars-bio oraz
pliki etapow w katalogach roboczych obu executorow; dowody zapisywane do
output/p0_dowod_<op>.json.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

---

### Task 3: Kontrola negatywna — executor bez koderów

**Files:**
- Modify: `ballista_genomics/src/bin/ballista_node.rs` (`USAGE`, `ExecutorOpts`, `parse_executor`, `run_executor`)
- Test: `tests/test_ballista_multiprocess.py` (dopisanie na końcu pliku)

**Interfaces:**
- Consumes: z Zadania 1 — `_start_cluster`, `Cluster.stop`, `OUTPUT_DIR`; z Zadania 2 — `_run_client`; `runner::bio_session_config`.
- Produces: flaga `ballista_node executor ... --no-codecs` — executor bez koderów operacji genomicznych (ani w nadpisaniach, ani w konfiguracji sesji).

- [ ] **Step 1: Dopisz test kontroli negatywnej (najpierw test)**

Dopisz na końcu `tests/test_ballista_multiprocess.py`:

```python
def test_executor_without_codecs_cannot_run_bio_plan(tmp_path):
    """Dowód 4 (kontrola negatywna): ta sama konfiguracja co w teście operacji,
    ale executor bez koderów. Plan dociera do niego jako bajty, których nie umie
    zdekodować — zapytanie MUSI się wywrócić. Gdyby przeszło, plan byłby
    wykonywany gdzie indziej niż w executorze."""
    c = _start_cluster(tmp_path, [("executor_bez_koderow", ["--no-codecs"])])
    try:
        out_csv = OUTPUT_DIR / "dist_merge_result.csv"
        out_csv.unlink(missing_ok=True)
        result, _ = _run_client("merge", c.url, timeout=180)
        assert result.returncode != 0, (
            f"zapytanie przeszło mimo executora bez koderów:\n{result.stdout}"
        )
        assert not out_csv.exists(), "wynik zapisany mimo błędu zapytania"
    finally:
        c.stop()
```

- [ ] **Step 2: Uruchom test — ma nie przejść**

Run: `pytest tests/test_ballista_multiprocess.py -v -k without_codecs`
Expected: FAIL z komunikatem `proces executor_bez_koderow zakończył się przedwcześnie (kod 2)` — `ballista_node` nie zna jeszcze flagi `--no-codecs`.

- [ ] **Step 3: Dodaj flagę `--no-codecs`**

W `ballista_genomics/src/bin/ballista_node.rs`:

1. Zmień `USAGE` na:

```rust
const USAGE: &str = "użycie:
  ballista_node scheduler --port <P>
  ballista_node executor --scheduler-port <P> --port <F> --grpc-port <G> \\
                         --work-dir <DIR> --concurrent-tasks <N> [--no-codecs]";
```

2. Dodaj pole do `ExecutorOpts` (po `concurrent_tasks: usize,`):

```rust
    /// `false` = kontrola negatywna: executor bez koderów operacji genomicznych.
    codecs: bool,
```

3. W `parse_executor` zmień listę flag bez wartości z `&[]` na `&["--no-codecs"]` i dodaj pole w zwracanej strukturze (po `concurrent_tasks: ...,`):

```rust
        codecs: !flags.contains_key("--no-codecs"),
```

4. W `run_executor` zastąp definicję `config_producer` oraz dwie linie `override_logical_codec` / `override_physical_codec`:

```rust
    // Kontrola negatywna usuwa kodery z OBU miejsc — nadpisań i konfiguracji
    // sesji — żeby executor nie miał skąd ich wziąć.
    let config_producer: ConfigProducer = if o.codecs {
        Arc::new(|| bio_ballista_config().upgrade_for_ballista())
    } else {
        Arc::new(|| bio_session_config().upgrade_for_ballista())
    };
```

```rust
        override_logical_codec: o.codecs.then(bio_logical_codec),
        override_physical_codec: o.codecs.then(bio_physical_codec),
```

5. Rozszerz import z biblioteki:

```rust
use ballista_genomics::cluster::{
    bio_ballista_config, bio_logical_codec, bio_physical_codec, bio_session_state,
};
use ballista_genomics::runner::bio_session_config;
```

6. W komunikacie startowym executora dodaj informację o koderach — zamień `println!` w `run_executor` na:

```rust
    println!(
        "ballista_node: executor (flight {}, grpc {}, katalog {}, sloty {}, kodery: {})",
        o.port,
        o.grpc_port,
        o.work_dir,
        o.concurrent_tasks,
        if o.codecs { "tak" } else { "NIE (kontrola negatywna)" }
    );
```

- [ ] **Step 4: Zbuduj**

Run: `cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build --bin ballista_node 2>&1 | tail -5`
Expected: `Finished` bez błędów.

- [ ] **Step 5: Uruchom cały plik testów — ma przejść**

Run: `pytest tests/test_ballista_multiprocess.py -v`
Expected: wszystkie PASS. W logu `executor_bez_koderow.log` (katalog z komunikatu pytest / `tmp_path`) powinien być widoczny błąd dekodowania planu — zanotuj jego treść do Zadania 4.

- [ ] **Step 6: Commit**

```bash
git add ballista_genomics/src/bin/ballista_node.rs tests/test_ballista_multiprocess.py
git commit -F - <<'EOF'
P0: kontrola negatywna - executor bez koderow nie wykona planu

Flaga --no-codecs usuwa kodery operacji genomicznych z nadpisan i konfiguracji
sesji executora; test wymaga, zeby zapytanie merge zakonczylo sie bledem
i nie zapisalo wyniku.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

---

### Task 4: Dokumentacja wyniku P0 i pełna regresja

**Files:**
- Modify: `ballista_genomics/OPIS.md` (nowa sekcja na końcu pliku)
- Modify: `deploy/ballista/docker-compose.yml:50-57` (komentarz „DO ZROBIENIA”)

**Interfaces:**
- Consumes: pliki `ballista_genomics/output/p0_dowod_<op>.json` z Zadania 2; treść błędu z logu executora bez koderów z Zadania 3.
- Produces: opis P0 w `OPIS.md` (materiał do raportów i pracy).

- [ ] **Step 1: Pełna regresja**

Run: `pytest tests/ -v 2>&1 | tail -30`
Expected: wszystkie dotychczasowe testy (42) oraz nowe testy P0 — PASS. Import polars-bio trwa kilka minut.

- [ ] **Step 2: Wygeneruj tabelę dowodów**

Run:

```bash
cd /home/milosz/praca_magisterska && python3 - <<'EOF'
import json
from pathlib import Path
print("| Operacja | executor_1: etapy | executor_2: etapy |")
print("|---|---|---|")
for op in ["overlap", "merge", "subtract", "nearest", "coverage"]:
    d = json.loads(Path(f"ballista_genomics/output/p0_dowod_{op}.json").read_text())
    cells = []
    for name in ["executor_1", "executor_2"]:
        jobs = d["etapy_na_executorach"].get(name, {})
        cells.append("; ".join(f"zadanie {i + 1}: {', '.join(st)}" for i, st in enumerate(jobs.values())) or "—")
    print(f"| {op} | {cells[0]} | {cells[1]} |")
EOF
```

Expected: tabela w Markdownie — pięć wierszy; dla `merge`, `subtract`, `nearest`, `coverage` niepuste obie kolumny.

- [ ] **Step 3: Dopisz sekcję do `ballista_genomics/OPIS.md`**

Dopisz na końcu pliku (tabelę wklej z wyjścia kroku 2, treść błędu — z kroku 5 Zadania 3):

```markdown
## P0 — klaster z osobnych procesów (wrzesień 2026)

Cel: dowód, że wykonanie jest rzeczywiście rozproszone, a nie tylko poprawne
w trybie standalone (scheduler i executor w jednym procesie). Bez pomiarów czasu.

### Jak uruchomić ręcznie

Wszystkie polecenia z katalogu `ballista_genomics/` (ładunki planu przenoszą
ścieżki względne do danych):

    ./target/debug/ballista_node scheduler --port 50050
    ./target/debug/ballista_node executor --scheduler-port 50050 --port 50051 \
        --grpc-port 50052 --work-dir /tmp/ex1 --concurrent-tasks 2
    ./target/debug/ballista_node executor --scheduler-port 50050 --port 50061 \
        --grpc-port 50062 --work-dir /tmp/ex2 --concurrent-tasks 2
    BALLISTA_SCHEDULER_URL=df://localhost:50050 ./target/debug/dist_ops merge

### Co musiało się zmienić względem standalone

- Scheduler buduje bio-owy stan sesji sam (`cluster::bio_session_state`) —
  standalone dostawał go niejawnie od klienta.
- Kodery rejestrowane są w każdym procesie osobno (klient, scheduler, executor).
- Scheduler w trybie push z rozdziałem round-robin: przy pull i milisekundowych
  zadaniach jeden executor potrafił zgarnąć całą pracę.
- Sprzątanie danych zakończonych zadań wyłączone — pliki etapów są dowodem.

### Dowody (`tests/test_ballista_multiprocess.py`)

1. Cztery różne procesy; scheduler widzi dwa executory o różnych
   identyfikatorach i portach.
2. Wynik każdej z pięciu operacji zgodny z wyrocznią polars-bio.
3. Pliki etapów w katalogach roboczych obu executorów:

<WKLEJ TABELĘ Z KROKU 2>

   `overlap` czyta pojedyncze pliki i nie ma równoległości hash (znane
   ograniczenie), więc może pracować na jednym executorze.
4. Executor uruchomiony z `--no-codecs` powoduje błąd zapytania:

<WKLEJ TREŚĆ BŁĘDU Z LOGU EXECUTORA>

Znane ryzyko poza zakresem P0: procesy muszą współdzielić system plików (ładunek
planu zawiera ścieżki) — w kontenerach i w chmurze potrzebny wspólny magazyn.
```

Obie wstawki `<WKLEJ ...>` zastąp faktycznym wynikiem przed commitem — to dane z wykonania, nie projekt.

- [ ] **Step 4: Zaktualizuj nieaktualny komentarz w `deploy/ballista/docker-compose.yml`**

Zastąp blok komentarza zaczynający się od `# DO ZROBIENIA przed uruchomieniem (Faza D):` (do końca pliku) tym:

```yaml
# Stan (P0, wrzesień 2026): tryb klastra z osobnych procesów jest zaimplementowany
# i przetestowany NATYWNIE (bez Dockera) — binarka `ballista_node` (role
# `scheduler` i `executor`) oraz klient `dist_ops` ze zmienną
# BALLISTA_SCHEDULER_URL; instrukcja: ballista_genomics/OPIS.md, sekcja P0.
# Komendy `--help` powyżej trzeba zastąpić wywołaniami `ballista_node`
# z tymi samymi flagami; ten plik NIE był jeszcze uruchamiany. Uwaga: ładunki
# planu przenoszą ścieżki do danych, więc kontenery muszą widzieć dane pod tą
# samą ścieżką (wspólny wolumen).
```

- [ ] **Step 5: Commit i push**

```bash
git add ballista_genomics/OPIS.md deploy/ballista/docker-compose.yml
git commit -F - <<'EOF'
P0: dokumentacja klastra z osobnych procesow i tabela dowodow

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
git push origin master
```
