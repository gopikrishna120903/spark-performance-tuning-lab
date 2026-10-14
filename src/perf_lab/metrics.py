"""Hardware-independent measurements, so results are comparable on a laptop or a cluster."""
import os
import statistics
import time
from typing import Callable, Dict, List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def partition_skew(df: DataFrame, cols: List[str], num_partitions: int) -> Dict[str, float]:
    """Rows per shuffle partition when hash-partitioned on `cols` (exactly what a join/agg shuffle does)."""
    sizes = [
        r["n"] for r in df.repartition(num_partitions, *cols)
        .groupBy(F.spark_partition_id().alias("p")).agg(F.count("*").alias("n")).collect()
    ]
    sizes += [0] * (num_partitions - len(sizes))
    med = statistics.median(sizes)
    return {"max_rows": max(sizes), "median_rows": med,
            "skew_ratio": round(max(sizes) / med, 1) if med else float("inf")}


def physical_plan(df: DataFrame) -> str:
    return df._jdf.queryExecution().executedPlan().toString()


def count_shuffles(df: DataFrame) -> int:
    """Number of shuffle exchanges (hash/range repartitioning) in the physical plan."""
    plan = physical_plan(df)
    return plan.count("Exchange hashpartitioning") + plan.count("Exchange rangepartitioning")


def time_action(action: Callable[[], object], repeats: int = 3) -> float:
    """Median wall-clock seconds over `repeats` runs."""
    runs = []
    for _ in range(repeats):
        t = time.perf_counter()
        action()
        runs.append(time.perf_counter() - t)
    return round(statistics.median(runs), 2)


def force(df: DataFrame) -> None:
    """Execute the full plan without collecting results to the driver."""
    df.write.format("noop").mode("overwrite").save()


def count_files(path: str, suffix: str = ".parquet") -> int:
    return sum(1 for _, _, files in os.walk(path) for f in files if f.endswith(suffix))
