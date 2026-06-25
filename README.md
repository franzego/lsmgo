# lsmgo

`lsmgo` is a small Log-Structured Merge Tree prototype written in Go for learning systems programming.

The goal is not to build RocksDB. It is to understand the moving parts that make an LSM-style storage engine work:

- batching writes
- writing to a WAL before memory
- storing the recent writes in a memtable
- flushing immutable memtables to SSTables
- recovering the SSTable catalog with a manifest file

This is intentionally a learning project. Some production features are missing on purpose so the core ideas stay visible.

## Current Status

Implemented:

- Batch writes
- WAL record framing and replay
- Memtable backed by a skip list
- Memtable rotation into immutable memtables
- SSTable writing and point lookup
- Bloom filter per SSTable
- Tombstones for deletes
- Sequence numbers for newest-value wins
- Simple text manifest for SSTable catalog recovery
- DB reopen can find keys that live only in SSTables

Not implemented:

- Compaction
- Levels
- SSTable block index
- WAL recycling
- Manifest rotation
- Background flush workers
- Snapshots
- Range scans

This is the stopping point for the first version: a small LSM with WAL, memtable, SSTable flush, and manifest recovery. Hopeful to improve it as time permits.

## How To Use (Still very Immature)

Open a database by passing a root directory - any path you desire. The DB creates its WAL and SSTable/manifest files under that directory:

```go
db, err := lsm.Open("/tmp/lsm-demo")
if err != nil {
    return err
}
defer db.Close()
```

For simple one-key operations, use `Put`, `Get`, and `Delete`:

```go
if err := db.Put([]byte("name"), []byte("franz")); err != nil {
    return err
}

value, ok := db.Get([]byte("name"))
if ok {
    fmt.Println(string(value))
}

if err := db.Delete([]byte("name")); err != nil {
    return err
}
```

`Put` and `Delete` are convenience methods. Internally they create a one-operation batch and commit it through the same durable write path as every other write.

For multiple keys that should commit together, build a `batch.Batch` and call `db.Write(&b)` once:

```go
var b batch.Batch

if err := b.Put([]byte("k1"), []byte("v1")); err != nil {
    return err
}
if err := b.Put([]byte("k2"), []byte("v2")); err != nil {
    return err
}
if err := b.Put([]byte("k3"), []byte("v3")); err != nil {
    return err
}

if err := db.Write(&b); err != nil {
    return err
}
```

Use `db.Put` for one-off writes. Use `batch.Batch` plus `db.Write` when several operations should share one durable commit.

## Write Path

A write goes through this order:

```text
Batch
  -> WAL append + fsync
  -> memtable apply
  -> maybe rotate memtable
```

The WAL comes before the memtable so a crash does not lose acknowledged writes. If the process dies after the WAL append but before the memtable apply, recovery can replay the WAL.

Batching matters because fsync is expensive. Writing one key per fsync gives poor throughput. A batch lets many operations share one durable WAL append.

## Memtable

The memtable stores recent writes in memory.

This implementation uses a skip list instead of a map because LSM flushes need sorted output. A map gives fast lookup, but it does not preserve order. A skip list keeps keys sorted as writes arrive, so flushing to an SSTable is a linear scan.

Keys are internal keys:

```text
user key + sequence number + kind
```

The sequence number gives version ordering. Newer writes shadow older writes. Deletes are represented with tombstones instead of removing older data in place.

Read order is:

```text
active memtable
  -> immutable memtables, newest first
  -> SSTables, newest first
```

## WAL

The WAL is an append-only log of committed batches.

The physical WAL format uses:

- 32KB blocks
- 7-byte physical record headers
- record types for full, first, middle, and last fragments
- checksums

Large logical records can be split across physical records. Replay reconstructs complete logical entries and stops safely on truncated tail records.

## SSTables

An SSTable is the disk file produced when an immutable memtable is flushed.

This prototype writes a simple SSTable layout:

```text
[records][bloom filter][footer]
```

The footer contains metadata and a magic number so reads can reject files that are too short or not actually SSTables.

There are no blocks or sparse indexes yet. Point lookup reads the file layout, checks the Bloom filter, then scans records if the filter says the key might exist. That is not production-efficient, but it keeps the file format understandable.

## Manifest

The manifest is the durable list of SSTables that belong to the DB.

An SSTable file existing on disk is not enough. The DB also needs to remember which SSTable files are part of the current database state. Without that catalog, reopening the DB would forget flushed SSTables.

For now the manifest is deliberately simple text:

```text
add 1 /tmp/db/sst/000001.sst
next 2
add 2 /tmp/db/sst/000002.sst
next 3
```

On flush:

```text
write SSTable file
append manifest edit
publish SSTable in memory
```

On open:

```text
scan MANIFEST line by line
rebuild []SSTable
restore nextSSTableNum
```

This is not crash-perfect like the WAL. That is intentional for this milestone. The manifest is text so the catalog concept is easy to inspect and reason about before adding binary framing, checksums, rotation, or compaction edits.

## Why Compaction Is Not Here Yet

Compaction needs a durable way to say:

```text
remove old SSTables A, B
add new SSTable C
```

That means compaction depends on the manifest. The current version stops after implementing the manifest foundation. A future compaction milestone can add `remove` or `replace` manifest records.

## Tests

Run:

```bash
go test ./...
```

In restricted environments, use a writable Go build cache:

```bash
GOCACHE=/tmp/lsm-go-cache go test ./...
```

## Helpful Notes: Understanding The Building Blocks

Log-Structured Merge Trees have become ubiquitous in today's database world. The name has become almost synonymous with modern storage engines. LSM trees sit behind databases like CassandraDB, CockroachDB, RocksDB, LevelDB, PebbleDB, and others.

One thing these systems have in common is their use in write-heavy workloads. The goal of an LSM is to ingest writes at a large scale, as quickly as possible. It sacrifices some read speed for this, because nothing is ever free. Even then, some optimizations can make that tradeoff manageable.

This repository is a dive into the internals of the data structures that power that technology. LSM trees have specific components that sustain them. `lsmgo` is a prototype in Go. It will not have the production features found in the more powerful databases that inspired it, but the important building blocks are present.

### 1. Batch Writes

Batch writes were included in this prototype because I consider them to be one of the foundations of the entire system.

A prototype like this does not strictly need batch writes, but I find them instructive. Why? Because they clearly expose the limitation of the one-write approach:

```text
one write = one WAL append + one memtable insert
```

The problem is that each durable WAL append involves an `fsync`, and `fsync` is expensive. It forces the OS to flush its write buffers all the way to disk. On a typical NVMe drive, that might be around 100-200 microseconds per fsync. Do that per key and your throughput ceiling becomes limited by the number of fsyncs you can perform, regardless of how fast everything else is.

Batching changes the math entirely. You take 10, 100, or 1000 key-value pairs, write them all to the WAL in one sequential append, do one fsync, then apply all of them to the memtable. The fsync cost is now amortized across the entire batch. This is much more efficient, and it is one of the reasons batching is foundational in this prototype.

At one point, I considered using a batch pool. The idea was that a pool could reduce GC pressure because batches are constantly created and discarded. But it added complexity too early. A pool only makes sense when the need is real. At this scale, the prototype does not require it.

### 2. WAL (Write Ahead Log)

The Write Ahead Log, or WAL, is the guard against data loss. It exists to ensure durability.

After a batch is created, the data is first stored in the WAL. As the name suggests, it is a log. An append-only log. Before the data goes into the memtable, which is the primary write location in an LSM, it first has to be persisted here.

This means that if the server crashes just before adding the data to the memtable, the state can be recovered by replaying the WAL.

It may feel counter-intuitive to write to a slow disk before writing to the memtable, which lives in much faster DRAM. But this disk write is sequential. The file grows at the end, and the WAL keeps streaming bytes forward. No random jumping around. This makes the write pattern much friendlier to disk.

The WAL is a safety net. It prevents the ambiguity that would happen if power goes off around the memtable write. Did the write happen? Did it not happen? The WAL gives the DB something durable to replay.

In this implementation, the WAL is divided into 32KB blocks for organization. Each physical record has a fixed 7-byte header. Large records can be fragmented across blocks and reassembled during replay.

### 3. Memtable

Now we are getting to the meat of it.

The memtable is a very important building block. Simply put, it is what makes writes, and some reads, quick. The first thing to note is that it is a data structure held in memory. It temporarily holds recent writes before flushing them to SSTables.

In this prototype, a skip list is used as the underlying data structure.

Why not a map? Maps are strong for insertion and lookup, but they are usually unordered. The real problem appears at flush time. When a memtable reaches its threshold and needs to be written to an SSTable, the DB needs sorted iteration over all keys. A hash map cannot give you that without a full sort at flush time, which is `O(n log n)`.

The skip list maintains sorted order continuously as inserts happen, so flushing is just a linear scan. That is the real advantage here. Funnily enough, while running benchmarks, i realized that the skiplist library that I am working with - github.com/huandu/skiplist - accepts keys as interfaces `interface{}`. Go does its boxing leading to the key value probably escaping to the heap. The time per ops for a GetLatestKey iteration is between ~599 and 680 ns/op. Sometimes as far as 761.5 or even 800 ns/ops. While the current skiplist library is good for general purpose workand honsetly really great, it is quite evident that to mimic how real LSM engines carry out their tasks, it will not suffice. A newer, specific skiplist implementation has to be made for the "hot path" of an LSM engine.

When you perform a read operation, the database starts from the newest data first: the active memtable, then immutable memtables, then SSTables from newest to oldest. If there are two entries for a key, the newest one shadows the older one.

LSMs do not do in-place updates. An update is just a newer write for the same key. A delete is a special tombstone marker, not an immediate removal from every old file. The shadowing mechanism makes both updates and deletes work.

The skip list nodes live in different locations in DRAM, but that is acceptable because memory pointer chasing is still much faster than random disk I/O. At flush time, the DB turns that pointer-chased memory structure into a contiguous sorted file on disk.

This is the core LSM tradeoff: accept pointer indirection in memory so disk writes can be sequential.

At some point, there will be two memtables: one active and one immutable. As writes come in, the active memtable approaches its threshold. Once that threshold is reached, the memtable is retired and writes stop going to it. It is moved aside as an immutable memtable, and a fresh active memtable starts accepting new writes.

The retired memtable is then ready to be flushed to an SSTable.

### 4. SSTable

The SSTable is where flushed data lands on disk.

Unlike the memtable, which resides in memory, the SSTable is a durable file. When a memtable reaches its threshold, it is moved into the immutable queue and later flushed sequentially to an SSTable.

This implementation uses a fixed byte signature called a magic number. A magic number is a common technique in storage formats. I came to appreciate this while looking at systems like RocksDB and LevelDB.

When data is written to a database file, it is just bytes. Without a recognizable structure, we do not know whether the file is complete, corrupt, or even the right type of file. Imagine someone renames a PDF to `000001.sst`, just like one of our SSTable files. Without validation, the DB might try to parse it as an SSTable and behave unpredictably.

The magic number helps prevent this. If the expected magic number is missing when the file is read, it is safe to treat the file as corrupt or invalid.

This implementation avoids SSTable levels, blocks, and indexes because that complexity is too much for this stage of the project. Future improvements can bring them in.

This leads nicely into Bloom filters. Bloom filters are space-efficient probabilistic data structures used to check whether an element may be in a set. They are not always right. They can give false positives, but they never give false negatives. If a Bloom filter says a key is not there, you can trust that. If it says the key may be there, the DB still has to check.

That is a small price to pay for reducing unnecessary disk scans.

In this prototype, there are no SSTable blocks or indexes yet. But the general idea of an SSTable is implemented: sorted records, a Bloom filter, and a footer with validation metadata.

### 5. Manifest

At first, it feels like writing an SSTable file should be enough. If `000001.sst` exists on disk, why does the DB need anything else? The answer is that a database needs to know which files belong to it.

The OS directory can contain many files. Some may be old, temporary, corrupt, or unrelated. The DB cannot just trust every file it sees. It needs a durable catalog. That catalog is the manifest.

In this project, the manifest is intentionally simple and text-based:

```text
add 1 /tmp/db/sst/000001.sst
next 2
```

The `add` line records that an SSTable belongs to the DB. The `next` line records the next file number that should be allocated.

On startup, the DB replays the manifest from top to bottom and rebuilds its in-memory SSTable list. This means a key that exists only inside an SSTable can still be found after the DB is reopened.

The manifest is not using the same binary framing as the WAL yet. That is intentional. At this stage, the goal is to understand the catalog concept clearly. Later, this can be upgraded with checksums, rotation, and compaction edits.

Compaction will eventually need the manifest to say something like:

```text
remove old tables A, B
add new compacted table C
```

That is why the manifest comes before compaction.

## Resources That Helped

- https://www.freecodecamp.org/news/build-an-lsm-tree-storage-engine-from-scratch-handbook/ Must-read introduction to a memtable in Go.
- https://skyzh.github.io/mini-lsm/week1-01-memtable.html A very helpful guide, written in Rust.
- https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis A thorough guide on the functional options pattern in Go.
- https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/ Chapter 3 on storage engines. It lays a perfect foundation for LSMs and other storage structures.
- https://github.com/cockroachdb/pebble The inspiration and guide for many of my implementation decisions.
