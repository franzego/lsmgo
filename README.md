# lsmgo

Log Search Merge Trees have become ubiquitous in today's database world. The name has become synonymous with "modern databases". It is the underlying structure behind databases like
CassandraDB, CockroachDB, RocksDB, LevelDB, PebbleDB and the likes. One thing these have in common is their usage in write-heavy operations. The goal of LSMs, is to as much as possible
and in as little time ingest writes at an unprecedented scale. It sacrifices read speed for this as nothing is ever free but even then some optimizations can make the tradeoff quite manageable.

This repository is taking a dive into the internals of the data structures that powers this technology. LSM-trees have specific components that sustain it.
Lsmgo is a prototype in golang. It will not have the production features as seen in the more powerful databases that inspired it, but the basic building blocks as much as possible,
will be made present.

## What Are These Building Blocks 
1. **Batch Writes:** Batch writes were included in this prototype as I consider them to be the foundation of the entire system. A prototype like this does not need batch writes, but I find it
instructive to have. Why? Because it clearly exposes the limitations of the one write approach - one write = one WAL append + one memtable insert. The problem is that each WAL append involves 
a fsync to guarantee durability — and fsync is expensive. It forces the OS to flush its write buffers all the way to the physical disk. On a typical NVMe drive that's maybe 100-200 microseconds per fsync. 
Do that per key and your throughput ceiling is maybe 5,000-10,000 writes per second regardless of how fast everything else is. Batching changes the math entirely. You take 10 (or 100, or 1000) key-value pairs, 
write them all to the WAL in one sequential append, do one fsync, then apply all of them to the MemTable. The fsync cost is now amortized across the entire batch. This is much more efficient. It is the foundation
of the entire prototype. In this prototype a batch pool was introduced. This reduces the GC pressure as Batches are constantly created and discarded.
2. **WAL(Write Ahead Log):** 

