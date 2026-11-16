"""
Assertions on the EVIDENCE of distribution, not only on the correctness of the result.

Motivation: a query can give a 100% correct result and still be computed in a
single query stage, with no parallelism at all. There would be no visible symptom,
and the claim of "distributed execution" would be empty. This is exactly what was
found for overlap (see `test_overlap_crosses_serialization_boundary` below).

Ballista implements `EXPLAIN ANALYZE` so that it returns sections
`=========SuccessfulStage[stage_id=N, partitions=M]=========` with the operator
tree and per-stage metrics. The `dist_ops` binary dumps this to
`output/dist_<op>_explain.txt`, and this file is parsed here.

This module deliberately does not import polars_bio, so it runs in a few seconds
(importing polars_bio takes ~4.5 min) and gives a fast feedback loop when working
on the codecs. The full correctness tests are in test_ballista_distributed_ops.py

Run: pytest tests/test_ballista_distribution_evidence.py -v
"""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path

import pytest

BALLISTA_DIR = Path(__file__).resolve().parent.parent / "ballista_genomics"
OUTPUT_DIR = BALLISTA_DIR / "output"
OPIS = BALLISTA_DIR / "OPIS.md"
CLIENT_BINARY = BALLISTA_DIR / "target" / "debug" / "bench_client"

STAGE_RE = re.compile(r"SuccessfulStage\[stage_id=(\d+), partitions=(\d+)\]")
#: Keyed shuffle reader in Ballista 55: `ShuffleReaderExec: upstream_stage: N,
#: partitioning: Hash([chrom@0], 4)` (in 53 without `upstream_stage`).
HASH_READER_RE = re.compile(r"ShuffleReaderExec: upstream_stage: \d+, partitioning: Hash\(\[chrom")


def _run_client(args: list[str], timeout: int) -> subprocess.CompletedProcess:
    """Runs `bench_client` (standalone mode); a missing binary is an error, not a skip."""
    if not CLIENT_BINARY.exists():
        pytest.fail(
            f"missing binary {CLIENT_BINARY.name} - build it: "
            "cd ballista_genomics && CARGO_BUILD_JOBS=1 cargo build --bins"
        )
    return subprocess.run(
        [str(CLIENT_BINARY), *args],
        cwd=BALLISTA_DIR,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _explain(op: str) -> tuple[str, list[tuple[int, int]]]:
    """Returns (raw plan text, [(stage_id, number_of_partitions), ...])."""
    path = OUTPUT_DIR / f"dist_{op}_explain.txt"
    if not path.exists():
        pytest.skip(
            f"no {path} - run first: "
            f"cd ballista_genomics && ./target/debug/dist_ops {op}"
        )
    txt = path.read_text()
    stages = [(int(a), int(b)) for a, b in STAGE_RE.findall(txt)]
    return txt, stages


# --------------------------------------------------------------------------
# merge: hash-shuffle pattern (the strongest evidence of distribution)
# --------------------------------------------------------------------------


def test_merge_plan_is_split_into_stages():
    """The plan must be cut into stages; otherwise there was no shuffle at all."""
    txt, stages = _explain("merge")
    assert len(stages) >= 2, f"merge ran in a single stage:\n{txt}"


def test_merge_has_hash_shuffle_on_contig():
    """
    The shuffle must be HASH-based ON THE CONTIG COLUMN, not round-robin.
    This is a necessary condition for correctness: rows of the same chromosome
    must land in the same partition so that MergeExec can merge them.
    """
    txt, _ = _explain("merge")
    assert re.search(r"ShuffleWriterExec: partitioning=Hash\(\[chrom", txt), txt


def test_merge_operator_reads_from_network():
    """
    MergeExec must have ShuffleReaderExec as its source, i.e. consume data
    FROM THE NETWORK (from another stage), not from a local file scan.
    """
    txt, _ = _explain("merge")
    assert "MergeExec" in txt, txt
    merge_pos = txt.index("MergeExec")
    after = txt[merge_pos:]
    assert "ShuffleReaderExec" in after, (
        f"MergeExec does not read from ShuffleReaderExec - no real shuffle:\n{txt}"
    )
    assert HASH_READER_RE.search(after), after


def test_merge_source_stage_is_parallel():
    """
    The source stage must have >1 partitions, and the scan must see both input
    files as separate groups. Without this the correctness test (overlapping
    gene_A1/gene_A2 in different files) would not test distribution.
    """
    txt, stages = _explain("merge")
    assert stages[0][1] >= 2, f"source stage is not parallel:\n{txt}"
    assert re.search(r"file_groups=\{2 groups:", txt), (
        f"both parts_a/ files should be read as separate groups:\n{txt}"
    )


def test_merge_compute_stage_is_parallel():
    """The stage that runs MergeExec must have >1 partitions."""
    txt, stages = _explain("merge")
    assert max(p for _, p in stages) > 1, f"all stages have a single partition:\n{txt}"


# --------------------------------------------------------------------------
# subtract: TWO-SIDED hash-shuffle pattern (binary node)
# --------------------------------------------------------------------------


def test_subtract_has_two_shuffled_inputs():
    """
    SubtractExec is a binary node: `execute(partition)` reaches for the same
    partition on BOTH sides. Both must therefore be co-partitioned by `chrom`;
    in the plan this shows up as two separate source stages with a hash shuffle.
    """
    txt, stages = _explain("subtract")
    assert len(stages) >= 3, f"subtract needs >=3 stages:\n{txt}"
    assert txt.count("partitioning=Hash([chrom") >= 2, (
        f"both subtract sides should be hash-partitioned by chrom:\n{txt}"
    )


def test_subtract_operator_reads_both_sides_from_network():
    """SubtractExec must have TWO ShuffleReaderExec as children."""
    txt, _ = _explain("subtract")
    assert "SubtractExec" in txt, txt
    after = txt[txt.index("SubtractExec"):]
    # cut off at the end of this stage so that readers from later stages are not counted
    stage_end = after.find("=========SuccessfulStage")
    block = after if stage_end == -1 else after[:stage_end]
    readers = len(HASH_READER_RE.findall(block))
    assert readers == 2, (
        f"SubtractExec should read from 2 ShuffleReaderExec (Hash by chrom), "
        f"found {readers}:\n{block}"
    )


def test_subtract_both_source_stages_are_parallel():
    """Both source stages (parts_a and parts_b) must be parallel."""
    txt, stages = _explain("subtract")
    assert stages[0][1] >= 2 and stages[1][1] >= 2, (
        f"both source stages should have >=2 partitions:\n{txt}"
    )
    assert txt.count("file_groups={2 groups:") >= 2, txt


# --------------------------------------------------------------------------
# nearest: BROADCAST pattern (parallelism on the right side, no shuffle)
# --------------------------------------------------------------------------


def test_nearest_is_broadcast_not_shuffle():
    """
    NearestExec does not override `required_input_distribution()`, so a hash
    shuffle does NOT occur and should not. The evidence of distribution here is
    a different pattern: parallelism on the right-table side plus the left table
    carried in the plan payload.
    """
    txt, stages = _explain("nearest")
    assert "NearestExec" in txt, txt
    assert not re.search(r"partitioning=Hash\(", txt), (
        f"nearest should not have a hash shuffle (broadcast pattern):\n{txt}"
    )


def test_nearest_compute_stage_is_parallel():
    """NearestExec must run on >1 partitions (parallelism from the right side)."""
    txt, stages = _explain("nearest")
    assert max(p for _, p in stages) >= 2, (
        f"NearestExec does not run in parallel:\n{txt}"
    )
    assert re.search(r"file_groups=\{2 groups:", txt), (
        f"the right table should be read as 2 separate file groups:\n{txt}"
    )


def test_nearest_left_table_travels_in_plan_not_as_scan():
    """
    Evidence of the BROADCAST: the left (indexed) table does NOT appear in the plan
    as a separate DataSourceExec; its data travelled inside the physical-plan
    payload (Arrow IPC), and the executor rebuilt the index from it locally.
    The left (broadcast) table is B (parts_b), because the result has a row for each
    row of the RIGHT table, while pb.nearest(A, B) has one for each interval of A;
    so only the scan of A (parts_a) is visible in the plan.
    """
    txt, _ = _explain("nearest")
    assert "parts_a" in txt, f"no scan of the right table (A):\n{txt}"
    assert "parts_b" not in txt, (
        "The left table appears in the plan as a scan - so it is NOT "
        f"broadcast in the plan payload:\n{txt}"
    )


# --------------------------------------------------------------------------
# coverage: broadcast + our own carrier node
# --------------------------------------------------------------------------


def test_coverage_uses_carrier_node_with_broadcast():
    """
    The plan must contain OUR carrier node with a visible number of broadcast
    rows. `broadcast_rows` in DisplayAs is deliberate: it is both evidence that
    the left table travelled in the plan payload and material for judging how
    close we are to the 16 MB gRPC message limit.
    """
    txt, _ = _explain("coverage")
    assert re.search(r"DistCoverageExec: coverage=true, broadcast_rows=\d+", txt), txt


def test_coverage_compute_stage_is_parallel():
    """The coverage node must run on >1 partitions (parallelism from the source)."""
    txt, stages = _explain("coverage")
    assert max(p for _, p in stages) >= 2, txt
    assert re.search(r"file_groups=\{2 groups:", txt), txt


def test_coverage_roundrobin_is_absent():
    """
    AN ASSERTION DOCUMENTING A FINDING: `CountOverlapsProvider::scan()` in the vendored
    code inserts RepartitionExec(RoundRobinBatch), and the static planner of Ballista 53
    removed from the distributed plan every repartition other than a hash one
    (ballista-scheduler/src/planner.rs). Our DistCoverageProvider therefore deliberately
    does not insert it, so the local and distributed plans have the same shape and
    parallelism comes from partitioning the source.

    Ballista 55 with AQE does not remove such repartitions; they do not appear because the
    sessions have `datafusion.optimizer.enable_round_robin_repartition = false`
    (`cluster::bio_ballista_config`: `upgrade_for_ballista()` before the Ballista setters;
    without it standalone got RoundRobinBatch(4) in front of DistCoverageExec).
    """
    txt, _ = _explain("coverage")
    assert "RoundRobinBatch" not in txt, (
        f"RoundRobinBatch appeared in the distributed plan - check whether "
        f"Ballista changed its behaviour:\n{txt}"
    )


# --------------------------------------------------------------------------
# overlap: an assertion DOCUMENTING a finding
# --------------------------------------------------------------------------


def test_overlap_crosses_serialization_boundary():
    """
    Overlap crosses the serialization boundary: IntervalJoinExec with the COITrees
    algorithm runs in a stage on the executor side, which writes the stage result
    (ShuffleWriterExec); that is, LogicalExtensionCodec and PhysicalExtensionCodec
    actually worked.

    Ballista 53 split this plan in standalone mode into two stages (a separate stage that
    merges the sorted result, read by ShuffleReaderExec); Ballista 55 with AQE sorts the
    single partition within the same stage, so there is one stage. The CSV data has one
    partition each, so the left side already satisfies the `SinglePartition` of CollectLeft
    mode and gets no separate stage (cf. `test_overlap_indexes_the_whole_left_side_in_every_task`).

    The join mode is CollectLeft, decided at planning time
    (`dist_provider::collect_left_at_planning`), rather than Auto that is only resolved in the codec.
    """
    txt, stages = _explain("overlap")
    assert len(stages) >= 1, txt
    first_stage = txt[txt.index("SuccessfulStage"):]
    assert "ShuffleWriterExec" in first_stage, txt
    assert "IntervalJoinExec: mode=CollectLeft" in first_stage, txt
    assert "alg=Coitrees" in first_stage, f"COITrees are not used on the executor side:\n{txt}"


def test_overlap_has_no_hash_partitioned_parallelism():
    """
    AN ASSERTION DOCUMENTING A FINDING, not a wish.

    Unlike merge, overlap does NOT get hash-partitioned parallelism.
    IntervalJoinExec is created in the vendored code with
    `mode = PartitionMode::Auto` (`with_config_rt_bio()` removes the `join_selection` rule,
    which would normally resolve Auto -> Partitioned/CollectLeft); we resolve it
    at planning time to CollectLeft (`dist_provider::collect_left_at_planning`, Ballista 55).
    `required_input_distribution()` then returns `SinglePartition` for the left side
    and `UnspecifiedDistribution` for the right; a hash repartition is never requested.
    The parallelism of overlap comes from the partitions of the right side, and each task indexes
    the whole left side (on CSV both sides have one partition each, hence one partition in the stage).

    When this test starts failing, it means the limitation has been lifted
    (e.g. by a custom optimizer rule forcing Partitioned); then update this test
    and the description in OPIS.md instead of "fixing" the code.
    """
    txt, stages = _explain("overlap")
    has_hash = bool(re.search(r"partitioning=Hash\(", txt))
    max_parts = max(p for _, p in stages)
    assert not has_hash and max_parts == 1, (
        "Overlap gained hash-partitioned parallelism - a CHANGE for the better "
        "compared with the documented state. Update this test and OPIS.md.\n"
        f"{txt}"
    )


def _metric(block: str, name: str) -> int:
    """Value of a counter from the plan metrics; the test dataset is small, so the numbers are
    plain integers (no abbreviations like `1.75 M`)."""
    found = re.findall(rf"\b{name}=(\d+)(?=[,\]])", block)
    assert len(found) == 1, f"expected exactly one plain integer {name}, found {found}:\n{block}"
    return int(found[0])


def test_overlap_indexes_the_whole_left_side_in_every_task(parquet_dirs, tmp_path):
    """
    Overlap on an input with several partitions (Parquet test dataset, Ballista 55): each join
    task indexes the WHOLE left side.

    The 55 scheduler narrows the plan's leaves to the task's partitions (`restrict_plan_to_partitions`),
    except for inputs that have to be coalesced. The CollectLeft mode, decided at
    planning time, requires `SinglePartition` for the left side: the left side is a separate stage
    (scan -> shuffle), and the join reads all of it through `CoalescePartitionsExec` <-
    `ShuffleReaderExec`. The evidence is in the metrics: `build_input_rows` (summed over tasks)
    = number of join tasks x number of left-side rows. With the Auto mode that is only resolved
    in the codec, the sum equalled the number of left-side rows (each task had only its own
    slice), and overlap lost pairs.
    """
    left, right = parquet_dirs
    target = tmp_path / "overlap_explain.txt"
    r = _run_client(
        ["--op", "overlap", "--left", str(left), "--right", str(right),
         "--explain-analyze", str(target)],
        timeout=180,
    )
    assert r.returncode == 0, r.stderr[-3000:]
    txt = target.read_text()
    stages = [(int(a), int(b)) for a, b in STAGE_RE.findall(txt)]
    assert len(stages) == 2, f"expected the left-side stage and the join stage:\n{txt}"
    (left_stage, _), (_, join_tasks) = stages
    left_block, join_block = txt.split("=========SuccessfulStage")[1:]
    assert "IntervalJoinExec" not in left_block, txt
    assert join_tasks >= 2, f"the join stage is not parallel:\n{txt}"

    join_lines = [line.strip() for line in join_block.splitlines()]
    at = next(i for i, line in enumerate(join_lines) if line.startswith("IntervalJoinExec"))
    assert join_lines[at].startswith("IntervalJoinExec: mode=CollectLeft"), txt
    assert join_lines[at + 1].startswith("CoalescePartitionsExec"), txt
    assert join_lines[at + 2].startswith(f"ShuffleReaderExec: upstream_stage: {left_stage},"), txt

    left_rows = _metric(left_block.splitlines()[1], "output_rows")
    assert left_rows > 0, txt
    assert _metric(join_lines[at], "build_input_rows") == join_tasks * left_rows, txt


# --------------------------------------------------------------------------
# Ballista 55 settings: explicit and consistent with OPIS.md
# --------------------------------------------------------------------------


def _declared(key: str) -> str:
    """Value of a Ballista key declared in OPIS.md in the form `key` = `value`
    (exactly one declaration)."""
    found = re.findall(rf"`{re.escape(key)}` = `([^`]+)`", OPIS.read_text())
    assert len(found) == 1, f"OPIS.md must declare `{key}` = `...` exactly once, found: {found}"
    return found[0]


def _version_info() -> dict:
    """Ballista settings read from the cluster configuration (`bench_client --version-info`)."""
    r = _run_client(["--version-info"], timeout=60)
    assert r.returncode == 0, r.stderr
    return json.loads(r.stdout)


def test_aqe_setting_is_explicit():
    """
    AQE (the adaptive query planner) is enabled by default in Ballista 55 (#2315), and 53
    disabled it by default, so the setting is explicit (`cluster::ADAPTIVE_QUERY_PLANNER`) and the
    decision is described in OPIS.md. A single source of truth: the value from the cluster
    configuration (`--version-info`) must be the one declared in OPIS.md, and the plan must
    come from that planner: AQE numbers stages from 0 (`AdaptivePlanner`: `stage_id_generator: 0`),
    the static planner from 1 (as Ballista 53 did).
    """
    declared = _declared("ballista.planner.adaptive.enabled")
    assert declared in ("true", "false"), declared
    assert _version_info()["adaptive_query_planner"] == (declared == "true")
    _, stages = _explain("merge")
    first_stage = min(stage_id for stage_id, _ in stages)
    assert first_stage == (0 if declared == "true" else 1), stages


def test_scheduling_settings_are_explicit():
    """
    Ballista 55 settings that restore the 53 semantics; the values from the cluster
    configuration must be those from OPIS.md:
    - `ballista.scheduler.max_partitions_per_task`: by default (0, no limit) 55 packs into one
      task as many partitions as the executor has free slots; with 2 slots a stage of two
      partitions ran as one task on one executor; 1 = one partition per task;
    - `ballista.scheduler.share_file_statistics_cache`: a file-statistics cache in the scheduler
      shared across sessions (new in 55); disabled, as in 53 (each session starts with a cold one).
    """
    info = _version_info()
    partitions = _declared("ballista.scheduler.max_partitions_per_task")
    assert partitions == "1", partitions
    assert info["max_partitions_per_task"] == int(partitions)
    shared = _declared("ballista.scheduler.share_file_statistics_cache")
    assert shared == "false", shared
    assert info["share_file_statistics_cache"] is False
