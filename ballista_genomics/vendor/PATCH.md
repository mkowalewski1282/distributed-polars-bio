# Local patches: datafusion-bio-function-ranges

**This is NOT a fork of Ballista or Sail** (the project deliberately avoids forking the
distributed engines). These are **visibility** fixes in a helper library
(`datafusion-bio-function-ranges`, biodatageeks, Apache-2.0), vendored locally only
because upstream does not have them yet.

## Version of the copy

The copy is `datafusion/bio-function-ranges` from tag **v0.25.0** of the
biodatageeks/datafusion-bio-functions repository (DataFusion =55.2.0, the same as Ballista 55).
The copy's `Cargo.toml` has the upstream v0.25.0 workspace inheritance expanded
(`version.workspace` etc.); otherwise it is unchanged. Upstream v0.25.0 still exposes neither
`physical_planner::intervals` nor the physical nodes of the operations, so both patches are
re-applied unchanged - the structs have the same fields as in 0.18.0 and 0.22.2 (updated
15.11.2026).

**Transitional period:** polars-bio 0.37 is built from v0.24.0 (DataFusion 53), while Ballista
uses the v0.25.0 copy (DataFusion 55.2). v0.24.x -> v0.25.0 is only a port to the DataFusion 55
API (plus a deterministic change of the hash function for equality keys in `IntervalJoinExec`),
with no change to the interval algorithms; `tests/test_algorithm_versions.py`
(`EQUIVALENT_RANGES`) explicitly allows this equivalence. The asymmetry should disappear with
polars-bio 0.38 (polars-bio's main branch already has DataFusion 55.2 and v0.25.0; there is no
release yet - it may get v0.26.0, which does not change the ranges crate).

Both patches have the same nature: **zero lines of logic, only the word `pub` and re-exports**.
The maintenance rule is checked against the clean upstream tag:

```bash
git clone -q --depth 1 --branch v0.25.0 https://github.com/biodatageeks/datafusion-bio-functions /tmp/dbf
diff -ru /tmp/dbf/datafusion/bio-function-ranges/src ballista_genomics/vendor/datafusion-bio-function-ranges/src \
  | grep '^[+-]' | grep -v '^[+-][+-]'
```

If anything other than an added `pub`, a re-export or a comment shows up in the output, the
patch is no longer "clean" and needs a separate decision. To update the vendored copy, the
patches are the difference between the copy and the clean tag of the previous version
(`diff -ru -x Cargo.toml`, header paths reduced to `a/`/`b/`), applied with `patch -p1` to the
new one; the new copy's `Cargo.toml` is expanded by hand with the values from the new tag's
workspace (last done for v0.22.2 -> v0.25.0).

## Patch 1: `pub mod intervals` - `ColIntervals` cannot be named

### What and why

`IntervalJoinExec` (the physical overlap-join operator using COITrees) has a public
constructor `try_new(...)`, but one of its required arguments - `ColIntervals` - is defined in
a module declared as `mod intervals;` (without `pub`) inside `physical_planner/mod.rs`.

In Rust, visibility requires the WHOLE path to a type to be public - `pub struct
ColIntervals { ... }` alone is not enough if the module containing it is private. As a result
`ColIntervals` **cannot be named or constructed from outside the crate at all**, even though it
is itself declared `pub`. This was a fundamental blocker for writing a
`PhysicalExtensionCodec` for `IntervalJoinExec` (distributing the COITree-based operations in
Ballista) - without it, `IntervalJoinExec::try_new()` cannot be called from outside the crate.

### Change (exactly 2 lines)

`src/physical_planner/mod.rs`:
```diff
-mod intervals;
+pub mod intervals;
```

`src/lib.rs` (a convenience, optional, but it makes importing easier):
```diff
+pub use physical_planner::intervals::{ColInterval, ColIntervals, parse as parse_intervals};
```

### Status: ready to submit upstream

This change modifies no logic, only visibility - it is safe, conflict-free and easy to accept
as a separate small PR to
[biodatageeks/datafusion-bio-functions](https://github.com/biodatageeks/datafusion-bio-functions).
If such a PR landed, this `vendor/` directory would no longer be needed - it would be enough to
go back to a regular `git` dependency or a future crates.io version in `Cargo.toml`.

## Patch 2: public physical nodes of the range operations

### Problem

`MergeExec`, `SubtractExec`, `NearestExec` and `CountOverlapsExec` are declared **without
`pub`** and have NO inherent impls - neither constructors nor getters. They are created only as
struct literals inside the `scan()` of the corresponding provider.

From outside the crate they therefore cannot be **named** (`downcast_ref::<MergeExec>()` in
`try_encode`), **read**, or **constructed** (`try_decode`). Without that, a
`PhysicalExtensionCodec` cannot be written, and without it Ballista cannot ship the physical
plan to the executor - so the operation cannot run distributed.

Note: the **modules** themselves (`pub mod merge`, `pub mod nearest`, ...) are already public,
so the problem is one level lower than with Patch 1.

### Change (38x `pub` + 5 re-exports, 0 lines of logic)

| file | item |
|---|---|
| `src/merge.rs` | `MergeExec` + 6 fields |
| `src/subtract.rs` | `SubtractExec` + 9 fields |
| `src/nearest.rs` | `NearestExec` + 11 fields, and `fn build_nearest_indexes` -> `pub fn` |
| `src/count_overlaps.rs` | `CountOverlapsExec` + 6 fields, and `enum CountOverlapsIndex` -> `pub enum` |
| `src/lib.rs` | re-exports of the above + `build_coitree_from_batches`/`build_count_index_from_batches` |

### Why `pub` on the fields rather than a constructor + getters

Rust has no partial literal - to build a `MergeExec` from outside the crate, **all** fields
must be public. `pub` on the fields provides both the constructor and the getters at zero added
code, in line with the "patch = pure visibility" rule adopted for Patch 1.

If this were submitted upstream, a more idiomatic shape would be `pub fn try_new(...)` plus
getters (encapsulating the computation of `cache`). Our code is prepared for such a change:
the whole construction of these nodes is confined to the `build_*_exec()` functions in
`src/bio_phys_codec.rs` and `src/coverage_node.rs`, so swapping the vendored API shape means
editing those few functions, nothing more.

### What the patch does NOT do

It does not turn `cache: Arc<PlanProperties>` into anything serializable - and does not need
to. `with_new_children()` recomputes `cache` deterministically from the `schema` and the
child's partition count, so after decoding the codec recomputes it immediately and gets exactly
what the vendored code would compute (including the correct `EmissionType`). Since DataFusion
55, calling `with_new_children()` directly is deprecated: the codec calls
`recompute_with_children` (`src/bio_phys_codec.rs`) -> `replace_children(...,
ChildrenPropertiesMode::Recompute)`, which by default ends up in the vendored node's
`with_new_children()`.

### What the patch was NOT enough for

`coverage` additionally required **our own carrier node** (`DistCoverageExec`,
`src/coverage_node.rs`) - not because of visibility, but because
`CountOverlapsProvider::scan()` materializes the left table, passes it to the index
constructor and **discards** it. The resulting `CountOverlapsExec` stores neither the left
table's data, nor its column names, nor the `coverage` flag, and the index itself cannot be
read (`COITree` has no serde). No visibility patch can fix that - it is a matter of what the
node remembers at all.

Separately: **`cluster` remains non-distributable** and is not covered by the patch.
`ClusterIdCoordinator` (`src/cluster.rs`) is a rendezvous barrier built on a `Vec<Waker>` in a
`Mutex`, working only within a single process. After distribution each executor would get its
own copy, so the plan would either hang or silently duplicate the cluster IDs. This is a
**semantic** blocker, not a visibility one.

### Status: ready to submit upstream

Like Patch 1 - zero logic changes, so a natural candidate for one small PR ("expose the
physical nodes of the range operations so that a `PhysicalExtensionCodec` can be written for
them").

## Why vendored locally rather than as a GitHub fork

Vendoring (a copy in the repository, not a separate fork under the user's account) was chosen
deliberately: it does not require creating a new repository/account on the user's behalf or
maintaining a remote fork - the whole change fits in this one file, so it is fully transparent
and easy to verify in the commit diff.
