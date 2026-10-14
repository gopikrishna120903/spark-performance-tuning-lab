"""Synthetic retail data with controllable skew (one 'hot' store dominates, like a flagship/online store)."""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def sales(spark: SparkSession, rows: int, stores: int, hot_share: float, seed: int = 7,
          num_partitions: int = 8) -> DataFrame:
    r = F.rand(seed)
    return (
        spark.range(0, rows, 1, num_partitions)
        .withColumn("store_id", F.when(r < hot_share, F.lit(0)).otherwise((F.rand(seed + 1) * stores).cast("int")))
        .withColumn("product_id", (F.rand(seed + 2) * 5000).cast("int"))
        .withColumn("sale_date", F.date_add(F.lit("2026-01-01"), (F.rand(seed + 3) * 90).cast("int")))
        .withColumn("amount", F.round(F.rand(seed + 4) * 200, 2))
        .withColumnRenamed("id", "sale_id")
    )


def stores_dim(spark: SparkSession, stores: int) -> DataFrame:
    return (
        spark.range(0, stores, 1, 2).withColumnRenamed("id", "store_id")
        .withColumn("store_id", F.col("store_id").cast("int"))
        .withColumn("region", F.element_at(F.array(*[F.lit(x) for x in ["SOUTH", "WEST", "EAST", "MIDWEST"]]),
                                           (F.col("store_id") % 4 + 1).cast("int")))
        .withColumn("store_name", F.concat(F.lit("Store #"), F.col("store_id")))
    )
