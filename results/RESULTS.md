# Results

Measured with `python -m perf_lab.experiments --rows 2,000,000` on Linux with **1 CPU core(s)**, Spark local mode,
2026-10-14. Partition-size and file-count metrics do not depend on hardware; wall-clock
times do, so re-run on a real cluster for representative timings (salting in particular only pays off when there are
many cores waiting on the straggler task).

## 1. Data skew and salting

Hot key(s) detected: `[0]` (one store holds ~40% of all sales).

| Shuffle on | Largest partition (rows) | Median partition (rows) | Skew ratio (max / median) |
|---|---|---|---|
| `store_id` | 862,355 | 74,628 | **11.6x** |
| `store_id` + salt | 186,936 | 125,352 | **1.5x** |

The salted shuffle cuts the straggler task's input by 78%,
so the stage no longer waits on one overloaded task. Join wall-clock here: plain 2.14s vs salted 2.66s.

## 2. Broadcast hash join vs sort-merge join

| Strategy | Shuffle exchanges in plan | Time |
|---|---|---|
| SortMergeJoin | 2 | 1.83s |
| BroadcastHashJoin | 0 | 0.59s (68% faster) |

Broadcasting the small store dimension removes the shuffle of the large fact table entirely.

## 3. Adaptive Query Execution: partition coalescing

| Setting (`shuffle.partitions=200`) | Output files | Time |
|---|---|---|
| AQE off | 200 | 12.6s |
| AQE on | 1 | 1.34s (89% faster) |

AQE merges tiny post-shuffle partitions at runtime, avoiding hundreds of near-empty tasks and output files.

## 4. Partition pruning

Query: one day of sales (`sale_date = '2026-02-14'`).

| Layout | Files in table | Files read | Data read | Time |
|---|---|---|---|---|
| Not partitioned | 16 | 16 | 21.4 MB | 0.42s |
| Partitioned by `sale_date` | 90 | 1 | 0.3 MB | 0.15s (64% faster) |

Partition filter present in the physical plan: **True**.

## 5. Small files vs compaction (OPTIMIZE)

| Layout | Files | Full-scan aggregate time |
|---|---|---|
| Many small files | 400 | 1.06s |
| Compacted | 4 | 0.26s (75% faster) |

On Delta Lake the same fix is `OPTIMIZE table ZORDER BY (col)` or auto-compaction.
