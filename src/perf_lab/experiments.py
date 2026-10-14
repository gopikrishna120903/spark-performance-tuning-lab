"""Run all experiments and write results/RESULTS.md with real measured numbers.

    python -m perf_lab.experiments --rows 2000000 --out results
"""
import argparse
import os
import platform
import shutil
import tempfile
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from . import datagen, metrics, salting
from .spark import get_spark


def exp_skew(spark: SparkSession, sales, stores, partitions: int) -> dict:
    hot = salting.find_hot_keys(sales, "store_id", 0.05)
    plain = metrics.partition_skew(sales, ["store_id"], partitions)
    salted_df = sales.withColumn(
        "_salt", F.when(F.col("store_id").isin(hot), (F.rand(1) * partitions).cast("int")).otherwise(0))
    salted = metrics.partition_skew(salted_df, ["store_id", "_salt"], partitions)

    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    t_plain = metrics.time_action(lambda: metrics.force(sales.join(stores, "store_id")))
    t_salted = metrics.time_action(lambda: metrics.force(salting.salted_join(sales, stores, "store_id", partitions, hot)))
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
    return {"hot_keys": hot, "plain": plain, "salted": salted, "t_plain": t_plain, "t_salted": t_salted}


def exp_broadcast(spark: SparkSession, sales, stores) -> dict:
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    smj = sales.join(stores, "store_id")
    bhj = sales.join(F.broadcast(stores), "store_id")
    out = {
        "smj_exchanges": metrics.count_shuffles(smj), "bhj_exchanges": metrics.count_shuffles(bhj),
        "smj_strategy": "SortMergeJoin" if "SortMergeJoin" in metrics.physical_plan(smj) else "?",
        "bhj_strategy": "BroadcastHashJoin" if "BroadcastHashJoin" in metrics.physical_plan(bhj) else "?",
        "t_smj": metrics.time_action(lambda: metrics.force(smj)),
        "t_bhj": metrics.time_action(lambda: metrics.force(bhj)),
    }
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
    return out


def exp_aqe_coalesce(spark: SparkSession, sales, tmp: str) -> dict:
    agg = sales.groupBy("store_id", "sale_date").agg(F.sum("amount").alias("revenue"))
    res = {}
    for label, enabled in (("aqe_off", "false"), ("aqe_on", "true")):
        spark.conf.set("spark.sql.adaptive.enabled", enabled)
        spark.conf.set("spark.sql.shuffle.partitions", "200")
        path = os.path.join(tmp, label)
        res["t_" + label] = metrics.time_action(lambda p=path: agg.write.mode("overwrite").parquet(p), repeats=1)
        res["files_" + label] = metrics.count_files(path)
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", os.getenv("SHUFFLE_PARTITIONS", "8"))
    return res


def exp_partition_pruning(spark: SparkSession, sales, tmp: str) -> dict:
    flat, part = os.path.join(tmp, "flat"), os.path.join(tmp, "partitioned")
    sales.repartition(16).write.mode("overwrite").parquet(flat)
    sales.repartition("sale_date").write.mode("overwrite").partitionBy("sale_date").parquet(part)
    day = "2026-02-14"
    flat_df, part_df = spark.read.parquet(flat), spark.read.parquet(part)   # file listing happens once, here

    def files_read(df):
        return [r[0] for r in df.filter(F.col("sale_date") == day).select(F.input_file_name()).distinct().collect()]

    def mb(files):
        return round(sum(os.path.getsize(f.replace("file://", "")) for f in files) / 1024 / 1024, 1)

    read_flat, read_part = files_read(flat_df), files_read(part_df)
    plan = metrics.physical_plan(part_df.filter(F.col("sale_date") == day))
    return {"files_total_flat": metrics.count_files(flat), "files_total_part": metrics.count_files(part),
            "files_read_flat": len(read_flat), "files_read_part": len(read_part),
            "mb_read_flat": mb(read_flat), "mb_read_part": mb(read_part),
            "pruned": "PartitionFilters: [isnotnull(sale_date" in plan,
            "t_flat": metrics.time_action(lambda: flat_df.filter(F.col("sale_date") == day).agg(F.sum("amount")).collect()),
            "t_part": metrics.time_action(lambda: part_df.filter(F.col("sale_date") == day).agg(F.sum("amount")).collect())}


def exp_small_files(spark: SparkSession, sales, tmp: str) -> dict:
    small, compact = os.path.join(tmp, "small"), os.path.join(tmp, "compact")
    sales.repartition(400).write.mode("overwrite").parquet(small)
    spark.read.parquet(small).repartition(4).write.mode("overwrite").parquet(compact)  # = OPTIMIZE on Delta
    return {
        "files_small": metrics.count_files(small), "files_compact": metrics.count_files(compact),
        "t_small": metrics.time_action(lambda: spark.read.parquet(small).agg(F.sum("amount")).collect()),
        "t_compact": metrics.time_action(lambda: spark.read.parquet(compact).agg(F.sum("amount")).collect()),
    }


def speedup(before: float, after: float) -> str:
    return f"{(1 - after / before) * 100:.0f}% faster" if after < before else f"{(after / before - 1) * 100:.0f}% slower"


def report(rows: int, cores: int, sk, bc, aqe, pp, sf) -> str:
    return f"""# Results

Measured with `python -m perf_lab.experiments --rows {rows:,}` on {platform.system()} with **{cores} CPU core(s)**, Spark local mode,
{datetime.now(timezone.utc):%Y-%m-%d}. Partition-size and file-count metrics do not depend on hardware; wall-clock
times do, so re-run on a real cluster for representative timings (salting in particular only pays off when there are
many cores waiting on the straggler task).

## 1. Data skew and salting

Hot key(s) detected: `{sk['hot_keys']}` (one store holds ~40% of all sales).

| Shuffle on | Largest partition (rows) | Median partition (rows) | Skew ratio (max / median) |
|---|---|---|---|
| `store_id` | {sk['plain']['max_rows']:,} | {sk['plain']['median_rows']:,.0f} | **{sk['plain']['skew_ratio']}x** |
| `store_id` + salt | {sk['salted']['max_rows']:,} | {sk['salted']['median_rows']:,.0f} | **{sk['salted']['skew_ratio']}x** |

The salted shuffle cuts the straggler task's input by {(1 - sk['salted']['max_rows'] / sk['plain']['max_rows']) * 100:.0f}%,
so the stage no longer waits on one overloaded task. Join wall-clock here: plain {sk['t_plain']}s vs salted {sk['t_salted']}s.

## 2. Broadcast hash join vs sort-merge join

| Strategy | Shuffle exchanges in plan | Time |
|---|---|---|
| {bc['smj_strategy']} | {bc['smj_exchanges']} | {bc['t_smj']}s |
| {bc['bhj_strategy']} | {bc['bhj_exchanges']} | {bc['t_bhj']}s ({speedup(bc['t_smj'], bc['t_bhj'])}) |

Broadcasting the small store dimension removes the shuffle of the large fact table entirely.

## 3. Adaptive Query Execution: partition coalescing

| Setting (`shuffle.partitions=200`) | Output files | Time |
|---|---|---|
| AQE off | {aqe['files_aqe_off']} | {aqe['t_aqe_off']}s |
| AQE on | {aqe['files_aqe_on']} | {aqe['t_aqe_on']}s ({speedup(aqe['t_aqe_off'], aqe['t_aqe_on'])}) |

AQE merges tiny post-shuffle partitions at runtime, avoiding hundreds of near-empty tasks and output files.

## 4. Partition pruning

Query: one day of sales (`sale_date = '2026-02-14'`).

| Layout | Files in table | Files read | Data read | Time |
|---|---|---|---|---|
| Not partitioned | {pp['files_total_flat']} | {pp['files_read_flat']} | {pp['mb_read_flat']} MB | {pp['t_flat']}s |
| Partitioned by `sale_date` | {pp['files_total_part']} | {pp['files_read_part']} | {pp['mb_read_part']} MB | {pp['t_part']}s ({speedup(pp['t_flat'], pp['t_part'])}) |

Partition filter present in the physical plan: **{pp['pruned']}**.

## 5. Small files vs compaction (OPTIMIZE)

| Layout | Files | Full-scan aggregate time |
|---|---|---|
| Many small files | {sf['files_small']} | {sf['t_small']}s |
| Compacted | {sf['files_compact']} | {sf['t_compact']}s ({speedup(sf['t_small'], sf['t_compact'])}) |

On Delta Lake the same fix is `OPTIMIZE table ZORDER BY (col)` or auto-compaction.
"""


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--rows", type=int, default=2_000_000)
    p.add_argument("--stores", type=int, default=500)
    p.add_argument("--hot-share", type=float, default=0.4)
    p.add_argument("--partitions", type=int, default=16)
    p.add_argument("--out", default="results")
    a = p.parse_args()

    spark = get_spark("perf-lab")
    spark.sparkContext.setLogLevel("ERROR")
    sales = datagen.sales(spark, a.rows, a.stores, a.hot_share).cache()
    sales.count()
    stores = datagen.stores_dim(spark, a.stores).cache()
    stores.count()

    tmp = tempfile.mkdtemp(prefix="perf_lab_")
    try:
        sk = exp_skew(spark, sales, stores, a.partitions)
        bc = exp_broadcast(spark, sales, stores)
        aqe = exp_aqe_coalesce(spark, sales, tmp)
        pp = exp_partition_pruning(spark, sales, tmp)
        sf = exp_small_files(spark, sales, tmp)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    os.makedirs(a.out, exist_ok=True)
    text = report(a.rows, os.cpu_count() or 1, sk, bc, aqe, pp, sf)
    with open(os.path.join(a.out, "RESULTS.md"), "w") as f:
        f.write(text)
    print(text)


if __name__ == "__main__":
    main()
