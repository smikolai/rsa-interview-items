# Spark Internals — Interview Prep Notebooks

Four self-contained Jupyter notebooks covering core PySpark internals, each
isolating a concept and making it observable via `explain()`, partition
inspection, or timing.

**Stack:** PySpark 4.0.2 · JDK 17 · Jupyter Lab · Docker

---

## Quick Start

```bash
make run    # build image, start container, open browser
make stop  # stop container, delete generated files (spark-warehouse, checkpoints)
```

Jupyter opens at `http://localhost:8888` with all four notebooks in the file
browser. The Spark UI is available at `http://localhost:4040` while a session
is live.

> **Prerequisites:** Docker Desktop running. Nothing else needed — PySpark runs
> inside the container.

---

## Notebooks

Work through them in order — each builds on concepts introduced in the previous one.

---

### 1. `catalyst_examples.ipynb` — The Catalyst Optimizer

Spark's query optimizer rewrites your logical plan before execution. This
notebook isolates eight optimizer rules and uses `explain()` to make each one
visible.

| Section | Rule | What to observe |
|---------|------|-----------------|
| 1 | Predicate Pushdown | Filter moves below the join in the physical plan |
| 2 | Column Pruning | Unused columns disappear from the scan |
| 3 | Constant Folding | `10 - 5` becomes `5` before execution |
| 4 | Broadcast Hash Join vs Sort-Merge Join | `BroadcastHashJoin` vs `Exchange` + `SortMergeJoin` |
| 5 | Partition Pruning | `PartitionFilters` in `FileScan` skips irrelevant directories |
| 6 | Filter Simplification | `lit(True)` disappears; `lit(False)` collapses plan to empty `LocalTableScan` |
| 7 | All Four Plan Stages | `explain("extended")` shows Parsed → Analyzed → Optimized → Physical |
| 8 | Cost-Based Optimizer | Join reorder from `big → med → small` to `med ⋈ small → big` after `ANALYZE TABLE` |

**Key concept:** The Optimized Logical Plan is where rules fire. Compare it
against the Parsed plan to see exactly what changed.

---

### 2. `coalesce_vs_repartition.ipynb` — Partition Management

Both operations change partition count, but their mechanics are completely
different.

| Section | What it shows | How it's observable |
|---------|---------------|---------------------|
| 1 | `coalesce` — no shuffle | `explain()`: no `Exchange` node |
| 2 | `repartition` — always shuffles | `explain()`: `Exchange RoundRobinPartitioning` |
| 3 | `coalesce` can't increase count | `getNumPartitions()` before/after |
| 4 | `repartition` distributes evenly, `coalesce` can skew | Per-partition row counts |
| 5 | `repartition(n, col)` — hash by key | `Exchange hashpartitioning(col, n)` + distinct values per partition |
| 6 | The coalesce trap | `coalesce` before a wide op starves the map stage of parallelism |

**Key concept:** `coalesce` is a narrow transformation — it merges adjacent
partitions locally with no network I/O. `repartition` is wide — it always
shuffles, but distributes evenly and can increase partition count.

---

### 3. `shuffle_files.ipynb` — Shuffle Mechanics and Performance

Every wide transformation writes shuffle files to disk. This notebook surfaces
five failure modes and their fixes.

| Section | Problem | Fix |
|---------|---------|-----|
| 1 | Default 200 `shuffle.partitions` on small data | Right-size the setting; observe empty partition count |
| 2 | AQE partition coalescing | Enable AQE; `AQEShuffleRead` wraps the `Exchange`, count drops |
| 3 | Shuffle stage compounding | Each chained wide op adds an `Exchange` and a stage boundary |
| 4 | Skewed shuffle | One reducer gets 90% of rows; AQE skew join splits it |
| 5 | Broadcast join | `Exchange` disappears from the large side; no shuffle write at all |

**Key concept:** An `Exchange` node in the physical plan means shuffle write to
disk + shuffle read. Every `Exchange` is a stage boundary — all upstream tasks
must complete before downstream tasks can start.

---

### 4. `wide_vs_narrow.ipynb` — Transformation Taxonomy and Consequences

The narrow/wide distinction determines stage boundaries, codegen fusion,
lineage cost, and fault recovery overhead.

| Section | What it shows | How it's observable |
|---------|---------------|---------------------|
| 1 | Taxonomy | `Exchange` count for every common operation |
| 2 | Whole-stage codegen fusion | `*` prefix and `[codegen id : N]` in `explain()` |
| 3 | RDD lineage | `toDebugString()` — narrow gives `MapPartitionsRDD` chain, wide introduces `ShuffledRDD` |
| 4 | Checkpointing | `explain()` before/after — full plan collapses to single `ExistingRDD` scan |
| 5 | Pipeline fusion cost | Injecting an unnecessary `repartition()` mid-chain adds a measurable shuffle round-trip |
| 6 | Fault recovery cost | Lineage depth as a proxy — wide ops deepen the ancestry that must be replayed on failure |

**Key concept:** Narrow transforms are pipelined into one stage with no
intermediate disk I/O. Wide transforms create a stage boundary, write shuffle
files, and reset the codegen region. Checkpointing truncates lineage after
expensive wide op chains so recovery is cheap.

---

## Environment Notes

- `local[8]` mode — 8 threads in one JVM, no real network. Plans and partition
  mechanics are identical to a cluster; shuffle cost is local-disk I/O only.
- `spark.sql.shuffle.partitions` is set to `8` in most notebooks (default 200
  would make plans noisy for small datasets).
- AQE is disabled at session start in notebooks that demonstrate raw shuffle
  behaviour, and re-enabled explicitly where its effect is the subject.
