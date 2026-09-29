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


#: Okno obserwacji kontroli negatywnej. Odrzucenie planu przez executor następuje
#: w pierwszych sekundach (zaraz po etapie 1), a z koderami całe zapytanie trwa
#: ~2 s — 45 s z dużym zapasem rozdziela oba przypadki.
NEGATIVE_WINDOW_S = 45


def test_executor_without_codecs_cannot_run_bio_plan(tmp_path):
    """Dowód 4 (kontrola negatywna): ta sama konfiguracja co w teście operacji,
    ale executor bez koderów. Etap 1 (zwykłe węzły DataFusion) executor policzy,
    ale etapu z węzłem MergeExec nie umie zdekodować — plan jest więc dekodowany
    w executorze, a nie gdzie indziej.

    Ballista 53 w trybie push nie zgłasza tego jako błędu zapytania: executor
    odrzuca zadanie przy dekodowaniu („Could not deserialize ...”), a scheduler
    uznaje go za utraconego, wyrejestrowuje i po ponownej rejestracji próbuje
    od nowa — zapytanie wisi zamiast się wywrócić. Dowodem jest więc brak wyniku
    w oknie obserwacji oraz komunikat dekodowania, który executor zwrócił
    schedulerowi (log schedulera)."""
    _require(DIST_BINARY)
    c = _start_cluster(tmp_path, [("executor_bez_koderow", ["--no-codecs"])])
    out_csv = OUTPUT_DIR / "dist_merge_result.csv"
    out_csv.unlink(missing_ok=True)
    client = subprocess.Popen(
        [str(DIST_BINARY), "merge"],
        cwd=BALLISTA_DIR,
        env={**os.environ, "BALLISTA_SCHEDULER_URL": c.url},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        try:
            client.communicate(timeout=NEGATIVE_WINDOW_S)
            client_succeeded = client.returncode == 0
        except subprocess.TimeoutExpired:
            client.kill()
            client.communicate()
            client_succeeded = False
        scheduler_log = (tmp_path / "scheduler.log").read_text()
    finally:
        c.stop()
    assert not client_succeeded, "zapytanie przeszło mimo executora bez koderów"
    assert not out_csv.exists(), "wynik zapisany mimo braku koderów"
    assert "Could not deserialize" in scheduler_log, (
        f"brak śladu odrzucenia planu przez executor; log: {tmp_path / 'scheduler.log'}"
    )
