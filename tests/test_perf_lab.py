from pyspark.sql import functions as F

from perf_lab import datagen, metrics, salting


def test_salted_join_matches_plain_join(spark):
    sales = datagen.sales(spark, 20_000, 50, 0.5)
    stores = datagen.stores_dim(spark, 50)
    hot = salting.find_hot_keys(sales, "store_id", 0.05)
    assert hot == [0]

    plain = sales.join(stores, "store_id").agg(F.count("*").alias("n"), F.sum("amount").alias("s")).collect()[0]
    salted = salting.salted_join(sales, stores, "store_id", 8, hot).agg(
        F.count("*").alias("n"), F.sum("amount").alias("s")).collect()[0]
    assert plain["n"] == salted["n"] == 20_000
    assert round(plain["s"], 2) == round(salted["s"], 2)


def test_salted_aggregate_matches_groupby(spark):
    sales = datagen.sales(spark, 10_000, 20, 0.6)
    expected = {r.store_id: (round(r.t, 2), r.n) for r in
                sales.groupBy("store_id").agg(F.sum("amount").alias("t"), F.count("*").alias("n")).collect()}
    got = {r.store_id: (round(r.total_amount, 2), r.row_count) for r in
           salting.salted_aggregate(sales, "store_id", "amount", 8).collect()}
    assert got == expected


def test_salting_reduces_partition_skew(spark):
    sales = datagen.sales(spark, 50_000, 100, 0.4)
    plain = metrics.partition_skew(sales, ["store_id"], 8)
    salted = metrics.partition_skew(
        sales.withColumn("_salt", F.when(F.col("store_id") == 0, (F.rand(3) * 8).cast("int")).otherwise(0)),
        ["store_id", "_salt"], 8)
    assert salted["max_rows"] < plain["max_rows"] * 0.6


def test_broadcast_hint_removes_shuffle(spark):
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    sales, stores = datagen.sales(spark, 1000, 10, 0.1), datagen.stores_dim(spark, 10)
    smj = sales.join(stores, "store_id")
    bhj = sales.join(F.broadcast(stores), "store_id")
    assert "SortMergeJoin" in metrics.physical_plan(smj)
    assert "BroadcastHashJoin" in metrics.physical_plan(bhj)
    assert metrics.count_shuffles(bhj) < metrics.count_shuffles(smj)
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
