"""Skew mitigation by key salting.

A join or aggregation on a skewed key sends every row of the hot key to ONE task, so the
stage waits on a single straggler. Salting spreads the hot key over N sub-keys:
  * fact side: add a random salt 0..N-1 (only for hot keys, to keep the dimension small)
  * dim side:  replicate hot-key rows once per salt value
"""
from typing import List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def find_hot_keys(df: DataFrame, key: str, threshold_share: float = 0.05) -> List:
    """Keys holding more than `threshold_share` of all rows."""
    total = df.count()
    return [r[key] for r in df.groupBy(key).count().filter(F.col("count") > total * threshold_share).collect()]


def salted_join(fact: DataFrame, dim: DataFrame, key: str, buckets: int,
                hot_keys: Optional[List] = None, how: str = "inner") -> DataFrame:
    is_hot = F.col(key).isin(hot_keys) if hot_keys else F.lit(True)
    salted_fact = fact.withColumn("_salt", F.when(is_hot, (F.rand(42) * buckets).cast("int")).otherwise(F.lit(0)))
    salts = F.array(*[F.lit(i) for i in range(buckets)])
    salted_dim = dim.withColumn(
        "_salt", F.explode(F.when(is_hot, salts).otherwise(F.array(F.lit(0))))
    )
    return salted_fact.join(salted_dim, [key, "_salt"], how).drop("_salt")


def salted_aggregate(df: DataFrame, key: str, value: str, buckets: int) -> DataFrame:
    """Two-phase aggregation: partial sums per (key, salt), then final sum per key."""
    partial = (
        df.withColumn("_salt", (F.rand(42) * buckets).cast("int"))
        .groupBy(key, "_salt").agg(F.sum(value).alias("_partial"), F.count("*").alias("_n"))
    )
    return partial.groupBy(key).agg(F.sum("_partial").alias(f"total_{value}"), F.sum("_n").alias("row_count"))
