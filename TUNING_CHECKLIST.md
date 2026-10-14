# Spark tuning checklist

The order I work through when a Databricks job is slow:

1. **Read the Spark UI first.** Find the slowest stage; compare max vs median task duration and input size. A large gap means skew.
2. **Skew.** Turn on AQE skew join (`spark.sql.adaptive.skewJoin.enabled`). If one key still dominates, salt it (`perf_lab.salting`).
3. **Joins.** Broadcast dimensions under a few hundred MB (`F.broadcast(dim)`); check the plan for `BroadcastHashJoin`.
4. **Shuffle sizing.** Keep AQE coalescing on; aim for ~128-200 MB per shuffle partition instead of the default 200 partitions.
5. **Pruning.** Partition large tables by a low-cardinality filter column (usually date); use Z-ORDER for high-cardinality filters.
6. **Small files.** `OPTIMIZE` regularly, or enable optimized writes and auto-compaction.
7. **Caching.** Cache only data reused several times in the same job, and unpersist it after.
8. **Cluster sizing.** Right-size after the above; more nodes rarely fix skew.
