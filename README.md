# lsmgo

Log Search Merge Trees have become ubiquitous in today's database world. The name has become synonymous with "modern databases". It is the underlying structure behind databases like
CassandraDB, CockroachDB, RocksDB, LevelDB, PebbleDB and the likes. One thing these have in common is their usage in write-heavy operations. The goal of LSMs, is to as much as possible
and in as little time ingest writes at an unprecedented scale. It sacrifices read speed for this as nothing is ever free but even then, some optimizations can make the tradeoff quite manageable.

This repository is taking a dive into the internals of the data structures that powers this technology. LSM-trees have specific components that sustain it.
Lsmgo is a prototype in golang. It will not have the production features as seen in the more powerful databases that inspired it, but the basic building blocks as much as possible,
will be made present.

## What Are These Building Blocks ?
1. **Batch Writes:** Batch writes were included in this prototype as I consider them to be the foundation of the entire system. A prototype like this does not need batch writes, but I find it
instructive to have. Why? Because it clearly exposes the limitations of the one write approach - one write = one WAL append + one memtable insert. The problem is that each WAL append involves 
a fsync to guarantee durability — and fsync is expensive. It forces the OS to flush its write buffers all the way to the physical disk. On a typical NVMe drive that's maybe 100-200 microseconds per fsync. 
Do that per key and your throughput ceiling is maybe 5,000-10,000 writes per second regardless of how fast everything else is. Batching changes the math entirely. You take 10 (or 100, or 1000) key-value pairs, 
write them all to the WAL in one sequential append, do one fsync, then apply all of them to the MemTable. The fsync cost is now amortized across the entire batch. This is much more efficient. It is the foundation
of the entire prototype. In this prototype a batch pool was to be used, but it added so much complexity. The idea was that pool reduces the GC pressure as Batches are constantly created and discarded, but then it only makes sense
to be used when the need arises for it. In this prototype, there is no need for it. The scale does not require it.
2. **WAL(Write Ahead Log):** The Write Ahead Log or WAL for short is simply the guard against loss of data. It basically ensures durability. After the batch insertion, the data is stored in a WAL. As the name suggests,
it is a log. An append only log. Right before the data goes into the memtable (the primary write location in LSMs), it first has to be persisted here. This ensures that if the server crashed just before adding the data
to the memtable, the state can be recovered by replaying the WAL. It uses direct I/O bypassing the os and directly writing to the disk. It may feel counter-intuitive to write to a "slow disk" before the memtables that
are stored in the significantly faster DRAM. It is not. It is a sequential write, therefore the disk head just stays at the end of the file and keeps stremaing the files. No jumping around. It will definitely keep up the 
network or application logic. WAL is a safety net. It prevents the ambiguities or confusion that could occur when power goes off at the memtable.
3. **Memtable:** Now we are getting to meat of it all. Memtable is a very important building block. Simply put, this is what makes writes and reads - when the conditions are right - so quick. First thing to note is that it
is a data structure held in memory(DRAM). It temporarily holds recent writes in memory before flushing them to SSTables(another important building block). In this prototype a skip-list was used as its underlying data structure.
Why not a map ? Well, while maps have their strengths in O(1) time for insertion and deletion, they are usually unsorted. The real problem is that a flush - to the SSTable once the memtable has reached its threshold requires a 
sorted iteration over all keys. A hash map can't give you that without a full sort at flush time, which is O(n log n) and defeats the purpose. The skip-list maintains sorted order continuously as you insert, so flush is just a
linear scan — O(n). Skip-lists on the other hand offer this and is superior to Balanced BST (red-black tree) in that it offers a much easier time dealing with concurrent access as writes and reads happen simultaneously.
When you perform a read operation, the database always starts from the newest date(the top of the active skip-list) and then works backward. Therefore, if there are two entries for a key, it only sees the most recent thereby 
"shadowing" the earlier one. *LSMs do not do in-place deletes. An update is just a newer write for the same key. A delete operation is a special TOMBSTONE marker not an actual removal. The shadowing mechanism helps with both*
Note that the various nodes in the skip-list are in various locations in DRAM, but since they hold pointers to the next node in the list in the DRAM, it is fine as opposed to a disk. On a disk, random I/O is far slower.
So at flush time, the database does a single sorted scan of the skip-list and writes all key-value pairs sequentially to the SSTable thereby turning pointer-chased memory (caused by the random I/O) into a contiguous, sorted file. 
This is the core trade-off of the LSM design: accept pointer indirection in memory to get sequential writes on disk. To read data, the flow is - check the active memtable, check the ones that are about to be flushed, SSTables on
the disk from newest to oldest i.e the sequence numbers. A memtable key consist of: the userkey, the sequence/version number, the kind(put, tombstone). Also, at a certain point, there will be two memtables. One active, the other inactive.
This is what happens: As writes come in from the batches into the memtable, the threshold will be approached rapidly. Once that threshold or capacity is reached, that memtable is "retired" and writes stop going to it. It is moved aside to be 
replaced with a new installation of a memtable. The old and used memtable is then readied to be flushed in the background to the SSTable while the new one starts accepting writes. If a read operation comes in, the active memtable is hot first to
check if the entry is there, then the memtable to be flushed is hit next and then the SSTables are after. Quite Interesting.


