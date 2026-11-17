"""
Evidence that Ballista executes genomic operations in SEPARATE processes.
The scheduler and two executors are started from the `ballista_node` binary,
and the `dist_ops` client connects to them through the BALLISTA_SCHEDULER_URL variable.

Four pieces of evidence:
  1. separate processes: different PIDs of the scheduler, the executors and the client, and
     the scheduler sees two registered executors (REST: /api/executors);
  2. a result that agrees with the polars-bio oracle (the same oracles as the standalone tests);
  3. work on BOTH executors: an executor writes the data of every stage it computed
     in ITS OWN working directory (work_dir/job_id/stage_id/...), so the directories
     show directly who computed what;
  4. a negative control: an executor without codecs (--no-codecs) cannot decode
     the plan, so the query must end with an error.

No timing measurements: this asks "is it distributed", not "how fast".

Missing binaries are an ERROR, not a skipped test: the evidence must not silently
disappear from the results. Build: cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build
--bin ballista_node --bin dist_ops

Run: pytest tests/test_ballista_multiprocess.py -v
The operation tests import the polars-bio oracles.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
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
        pytest.fail(f"missing binary {binary.name} - build it: {BUILD_HINT}")


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
                    f"process {name} exited early (code {p.returncode}); "
                    f"logs in {cluster.log_dir}"
                )
        try:
            executors = _registered_executors(cluster.scheduler_port)
            if len(executors) == expected:
                return executors
        except OSError as e:
            last_error = e
        time.sleep(0.5)
    pytest.fail(
        f"scheduler did not register {expected} executors in {timeout:.0f} s "
        f"(last error: {last_error}); logs in {cluster.log_dir}"
    )


def _wait_for_scheduler(cluster: Cluster, timeout: float = 30.0) -> None:
    """A Ballista executor (53 and 55) connects to the scheduler only once, at startup
    (scheduler_connect_timeout_seconds = 0 means a single attempt), so executors
    may be started only once the scheduler is already listening."""
    deadline = time.monotonic() + timeout
    scheduler = cluster.procs["scheduler"]
    while time.monotonic() < deadline:
        if scheduler.poll() is not None:
            pytest.fail(
                f"process scheduler exited early (code {scheduler.returncode}); "
                f"logs in {cluster.log_dir}"
            )
        try:
            _registered_executors(cluster.scheduler_port)
            return
        except OSError:
            time.sleep(0.2)
    pytest.fail(f"scheduler did not start listening in {timeout:.0f} s; logs in {cluster.log_dir}")


def _start_cluster(log_dir: Path, executors: list[tuple[str, list[str]]]) -> Cluster:
    """Scheduler + executors as separate processes; each executor has its own
    working directory and 2 task slots."""
    _require(NODE_BINARY)
    cluster = Cluster(scheduler_port=_free_port(), log_dir=log_dir)
    try:
        cluster.procs["scheduler"] = _spawn(
            ["scheduler", "--port", str(cluster.scheduler_port)],
            log_dir / "scheduler.log",
        )
        _wait_for_scheduler(cluster)
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
        tmp_path_factory.mktemp("multiprocess_cluster"),
        [("executor_1", []), ("executor_2", [])],
    )
    yield c
    c.stop()


def test_ballista_node_rejects_bad_arguments():
    """A typo in a flag name or a missing required flag must not start the
    node with default values; it must end with exit code 2 and a usage message."""
    _require(NODE_BINARY)
    for args in (
        [],
        ["executor", "--port", "1"],
        ["scheduler", "--port", "1", "--unknown-flag"],
    ):
        r = subprocess.run(
            [str(NODE_BINARY), *args],
            cwd=BALLISTA_DIR,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert r.returncode == 2, f"{args}: exit code {r.returncode}, stderr: {r.stderr}"
        assert "usage" in r.stderr, f"{args}: no usage text: {r.stderr}"


def test_start_cluster_reports_crashed_process(tmp_path):
    """When a node crashes at startup, the helper must say WHICH one instead of waiting
    forever for registration."""
    with pytest.raises(pytest.fail.Exception, match="executor_bad"):
        _start_cluster(tmp_path, [("executor_bad", ["--unknown-flag"])])


def test_cluster_stop_terminates_all_processes(tmp_path):
    c = _start_cluster(tmp_path, [("executor_1", [])])
    c.stop()
    still_running = [name for name, p in c.procs.items() if p.poll() is None]
    assert not still_running, f"still running after stop(): {still_running}"


def test_start_cluster_waits_for_scheduler_before_executors(tmp_path, monkeypatch):
    """An executor connects to the scheduler only once, at startup; if it started
    before the scheduler is listening, it would fail. The helper must wait for the
    scheduler first; here the scheduler deliberately starts with a 2-second delay."""
    original_spawn = _spawn

    def spawn_with_delayed_scheduler(args: list[str], log: Path) -> subprocess.Popen:
        if args and args[0] == "scheduler":
            with log.open("w") as f:
                return subprocess.Popen(
                    ["sh", "-c", 'sleep 2; exec "$0" "$@"', str(NODE_BINARY), *args],
                    cwd=BALLISTA_DIR,
                    stdout=f,
                    stderr=subprocess.STDOUT,
                )
        return original_spawn(args, log)

    monkeypatch.setattr(sys.modules[__name__], "_spawn", spawn_with_delayed_scheduler)
    c = _start_cluster(tmp_path, [("executor_1", [])])
    c.stop()


def test_scheduler_sees_two_separate_executors(cluster):
    """Evidence 1 (cluster part): three separate processes, and the scheduler sees two
    DIFFERENT executors: different identifiers and ports."""
    pids = cluster.pids()
    assert len(set(pids.values())) == 3, f"PIDs are not distinct: {pids}"
    assert all(p.poll() is None for p in cluster.procs.values())
    executors = _registered_executors(cluster.scheduler_port)
    assert len({e["id"] for e in executors}) == 2, executors
    assert len({e["port"] for e in executors}) == 2, executors


# Canonical dataset, identical to the one in test_ballista_distributed_ops.py of the Ballista
# repository (that file guards the agreement with data/parts_a and data/parts_b).
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

#: Operations that read DIRECTORIES of two files (data/parts_*): the source stage has
#: >= 2 tasks, so under round-robin assignment it must reach both executors.
#: In Ballista 55 only with one partition per task
#: (`ballista.scheduler.max_partitions_per_task` = 1, `cluster.rs`); with the default
#: 0 both files went as one task to one executor (nearest, coverage).
#: `overlap` reads single files (data/intervals_*.csv) and has no hash
#: parallelism (a known limitation: every join task indexes the whole build side, see
#: OPIS.md); for it we require >= 1
#: executor, and the actual distribution is checked by test_ballista_distribution_evidence.py
#: of the Ballista repository.
MULTI_TASK_OPS = {"merge", "subtract", "nearest", "coverage"}
#: Operations with a hash shuffle by chromosome: the plan must have >= 2 stages.
SHUFFLE_OPS = {"merge", "subtract"}


def _run_client(op: str, scheduler_url: str | None, output_dir: Path, timeout: int = 180):
    """Runs dist_ops with results in `output_dir` (the DIST_OUTPUT_DIR variable);
    returns (CompletedProcess, client PID).

    A separate results directory is necessary: the standalone-mode tests rely
    on files in ballista_genomics/output/ (among them the EXPLAIN ANALYZE plans),
    so runs from this file must not overwrite them."""
    _require(DIST_BINARY)
    env = dict(os.environ)
    env["DIST_OUTPUT_DIR"] = str(output_dir)
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
        pytest.fail(f"dist_ops {op} exceeded {timeout} s\nstdout: {out}\nstderr: {err}")
    return subprocess.CompletedProcess(p.args, p.returncode, out, err), p.pid


def _stages_by_job(work_dir: Path) -> dict[str, set[str]]:
    """{job_id: {stage_id, ...}}: the stages for which the executor wrote files
    (Ballista layout: work_dir/job_id/stage_id/...)."""
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


def _check_against_oracle(op: str, output_dir: Path) -> None:
    """Evidence 2: client result == local polars-bio (oracles as in
    test_ballista_distributed_ops.py of the Ballista repository). Imports are lazy: the
    cluster-only tests do not need polars-bio."""
    import pandas as pd

    df = pd.read_csv(output_dir / f"dist_{op}_result.csv")
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
        # We compare DISTANCES, not the chosen neighbours: ties are broken
        # arbitrarily (see test_nearest_correctness.py of the Ballista repository).
        from tests.nearest_oracle import reference_nearest_min_distances

        # One row per interval of A; A is the right (queried) table of dist_nearest.
        assert len(df) == len(INTERVALS_A)
        actual = {name: int(d) for name, d in zip(df["right_name"], df["distance"])}
        expected = {
            name: int(d)
            for name, d in reference_nearest_min_distances(INTERVALS_A, INTERVALS_B).items()
        }
    else:
        raise ValueError(op)
    assert actual == expected, f"{op}: distributed result {actual} != oracle {expected}"


def _write_evidence(op: str, pids: dict[str, int], new_jobs: dict[str, dict[str, set[str]]]) -> None:
    """Records which executor computed which stages, as material for describing the run."""
    evidence = {
        "operation": op,
        "pid": pids,
        "stages_on_executors": {
            name: {job: sorted(stages) for job, stages in jobs.items()}
            for name, jobs in new_jobs.items()
        },
    }
    OUTPUT_DIR.mkdir(exist_ok=True)
    (OUTPUT_DIR / f"evidence_{op}.json").write_text(
        json.dumps(evidence, indent=2, ensure_ascii=False)
    )


@pytest.mark.parametrize("op", ["overlap", "merge", "subtract", "nearest", "coverage"])
def test_operation_runs_distributed_across_processes(cluster, op, tmp_path):
    """Evidence 1-3 for one operation run through a cluster of separate processes."""
    standalone_plan = OUTPUT_DIR / f"dist_{op}_explain.txt"
    standalone_plan_before = (
        standalone_plan.stat().st_mtime_ns if standalone_plan.exists() else None
    )
    before = {name: _stages_by_job(wd) for name, wd in cluster.work_dirs.items()}
    result, client_pid = _run_client(op, cluster.url, tmp_path)
    assert result.returncode == 0, (
        f"dist_ops {op}:\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )
    assert "external Ballista scheduler" in result.stdout, (
        f"the client did not use remote mode:\n{result.stdout}"
    )
    assert (tmp_path / f"dist_{op}_result.csv").exists(), (
        f"{op}: the result did not reach the test directory {tmp_path}"
    )
    standalone_plan_after = (
        standalone_plan.stat().st_mtime_ns if standalone_plan.exists() else None
    )
    assert standalone_plan_after == standalone_plan_before, (
        f"{op}: the remote run overwrote {standalone_plan}, which the standalone-mode "
        f"tests rely on"
    )
    after = {name: _stages_by_job(wd) for name, wd in cluster.work_dirs.items()}
    new_jobs = {
        name: {job: st for job, st in after[name].items() if job not in before[name]}
        for name in after
    }

    # Evidence 1: four different processes.
    pids = {**cluster.pids(), "client": client_pid}
    assert len(set(pids.values())) == 4, f"PIDs are not distinct: {pids}"

    # Evidence 2: correctness against polars-bio.
    _check_against_oracle(op, tmp_path)

    # Evidence 3: work done in the executors, and for multi-task operations in BOTH.
    participating = sorted(name for name, jobs in new_jobs.items() if jobs)
    assert participating, (
        f"{op}: no executor wrote stage data - the query did not go through the cluster"
    )
    if op in MULTI_TASK_OPS:
        assert participating == sorted(cluster.work_dirs), (
            f"{op}: only {participating} worked; stages: {new_jobs}"
        )
    if op in SHUFFLE_OPS:
        stages = set().union(*(st for jobs in new_jobs.values() for st in jobs.values()))
        assert len(stages) >= 2, f"{op}: expected ≥ 2 stages (shuffle), got {stages}"

    _write_evidence(op, pids, new_jobs)


def test_client_fails_fast_when_scheduler_is_down(tmp_path):
    """The address of a nonexistent scheduler must produce an error in reasonable time, not a hang."""
    result, _ = _run_client("merge", f"df://localhost:{_free_port()}", tmp_path, timeout=120)
    assert result.returncode != 0, f"the client succeeded without a scheduler:\n{result.stdout}"


def test_empty_scheduler_url_means_standalone(tmp_path):
    """An empty BALLISTA_SCHEDULER_URL variable = no variable (standalone mode)."""
    result, _ = _run_client("merge", "", tmp_path, timeout=180)
    assert result.returncode == 0, result.stderr
    assert "standalone" in result.stdout, result.stdout


def test_scheduler_accepts_ballista_settings_from_client(tmp_path, monkeypatch):
    """`ballista.*` keys sent by the client must reach the job configuration on the
    scheduler. Without the BallistaConfig extension in the scheduler configuration
    (upgrade_for_ballista), Ballista rejects them silently, with a debug-level log
    only, and tuning Ballista options in measurements would have no effect."""
    monkeypatch.setenv("RUST_LOG", "info,ballista_core::extension=debug")
    c = _start_cluster(tmp_path, [("executor_1", [])])
    try:
        result, _ = _run_client("merge", c.url, tmp_path / "results")
        scheduler_log = (tmp_path / "scheduler.log").read_text()
    finally:
        c.stop()
    assert result.returncode == 0, result.stderr
    rejected = [
        line
        for line in scheduler_log.splitlines()
        if "could not set configuration key: `ballista." in line
    ]
    assert not rejected, (
        "the scheduler rejected Ballista settings from the client:\n" + "\n".join(rejected[:5])
    )


#: Observation window of the negative control. The executor rejects the plan within the
#: first seconds (right after stage 1; Ballista 55 then ends the query with an error in a
#: fraction of a second), and with codecs the whole query takes ~2 s; 45 s with a large margin
#: separates the cases "error", "result" and "hang" (Ballista 53 behaviour).
NEGATIVE_WINDOW_S = 45


def test_executor_without_codecs_cannot_run_bio_plan(tmp_path):
    """Evidence 4 (negative control): the same configuration as in the operation test,
    but with an executor without codecs. The executor will compute stage 1 (plain DataFusion
    nodes) but cannot decode the stage with the MergeExec node, which shows that the plan is
    decoded in the executor and nowhere else.

    The executor rejects the task while decoding ("Could not deserialize ..." in its
    log). In push mode Ballista 53 did not report this as a query error (it treated the
    executor as lost and retried, so the query hung). Ballista 55 returns the rejected
    task to the scheduler, which ends the query with an error ("Job failed"), and the client
    gets "rejected by executor". The evidence is therefore a fast client error with no result,
    plus the decoding message in the executor log."""
    _require(DIST_BINARY)
    c = _start_cluster(tmp_path, [("executor_no_codecs", ["--no-codecs"])])
    output_dir = tmp_path / "results"
    out_csv = output_dir / "dist_merge_result.csv"
    client = subprocess.Popen(
        [str(DIST_BINARY), "merge"],
        cwd=BALLISTA_DIR,
        env={**os.environ, "BALLISTA_SCHEDULER_URL": c.url, "DIST_OUTPUT_DIR": str(output_dir)},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        try:
            _, client_stderr = client.communicate(timeout=NEGATIVE_WINDOW_S)
            timed_out = False
        except subprocess.TimeoutExpired:
            client.kill()
            _, client_stderr = client.communicate()
            timed_out = True
        scheduler_log = (tmp_path / "scheduler.log").read_text()
        executor_log = (tmp_path / "executor_no_codecs.log").read_text()
    finally:
        c.stop()
    assert not timed_out, f"the query hung for {NEGATIVE_WINDOW_S} s instead of failing"
    assert client.returncode != 0, "the query succeeded despite an executor without codecs"
    assert not out_csv.exists(), "result written despite missing codecs"
    assert "rejected by executor" in client_stderr, client_stderr
    assert "Job failed" in scheduler_log, f"log: {tmp_path / 'scheduler.log'}"
    assert "Could not deserialize" in executor_log, (
        f"no trace of the executor rejecting the plan; log: {tmp_path / 'executor_no_codecs.log'}"
    )
