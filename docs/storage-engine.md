# Storage-engine concepts

MeteorDB is an LSM tree with MVCC visibility. Foreground writes append a WAL
and update ordered memory. Flush creates immutable level-0 SSTables; explicit
leveled compaction merges overlapping files and reclaims versions no active
snapshot can observe.

## Reads and scans

Reads merge mutable/immutable memtables and a retained immutable file version.
L0 files may overlap and are searched newest first; each higher level has
non-overlapping ranges. A `Snapshot` fixes sequence visibility for point,
range, and prefix reads. `KvIterator` validates blocks lazily and returns an
error rather than skipping corrupt content.

Prefix scans calculate the exclusive upper bound by incrementing the last byte
that is not `0xff` and truncating after it. An empty prefix or an all-`0xff`
prefix has no finite upper bound. Exact prefix checking still prevents keys
outside the requested prefix from being emitted.

Bloom filters answer “definitely absent” or “possibly present.” False positives
are expected and cost a normal table lookup; false negatives are not allowed.
Metadata and data use separate cache partitions so bulk data cannot evict all
navigation blocks.

## Writes, flush, and compaction

Atomic batches are validated before logging and share one sequence. Size
options (`memtable_bytes`, block/file targets, cache bytes) govern resources;
file/block targets can be exceeded to preserve record boundaries.

Flush publication and recovery ordering are described in
[durability](durability.md). Compaction selects one overfull level per call.
It increases write amplification and may temporarily require input plus output
space, but reduces read/space amplification. Long-lived snapshots and iterators
delay version and file reclamation.

## TTL

TTLs are persisted absolute wall-clock deadlines. Expired values are invisible
to current and snapshot reads and are discarded when compaction can do so
safely. Snapshots do not freeze time. See the clock requirements and rollback
limitation in [durability](durability.md#ttl-and-clocks).

## Adapter semantics

Adapters encode versioned, namespace-prefixed keys over the byte API.
Inference-cache singleflight is process-local, not distributed. Feature rows
are immutable event-time records with typed values. Embedding batch gets use
one snapshot and preserve input order; batch puts are atomic. None provides
cross-adapter transactions or ANN search.

## Trade-offs

- Embedded synchronous calls simplify deployment but can perform filesystem
  work on application threads.
- LSM writes are sequential but compaction rewrites bytes.
- MVCC snapshots stabilize sequence visibility but retain old versions.
- Strong corruption detection favors explicit failure over best-effort reads.
- The engine is optimized for local ordered storage, not network distribution,
  relational queries, or RocksDB compatibility.
