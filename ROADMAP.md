# Roadmap

MeteorDB is pre-alpha. The order below is directional and not a delivery
commitment. Checked items are available in code on `main`; unchecked items
describe intended capability milestones.

## Available in the current pre-alpha

- [x] Checksummed, fragmented WAL segments
- [x] Atomic write batches
- [x] MVCC snapshot point reads
- [x] Memtable rotation and background flush
- [x] Immutable level 0 SSTables
- [x] Per-table Bloom filters
- [x] Durable manifest and WAL recovery
- [x] Level-aware point-read routing
- [x] Partitioned metadata and data block cache
- [x] Structured read-path and cache statistics

## Storage-engine completeness

- [ ] Range and prefix scans
- [ ] Automatic compaction across levels
- [ ] Safe obsolete-version and file reclamation
- [ ] TTL enforcement during reads and maintenance
- [ ] Stable file-format compatibility and migrations
- [ ] Backup, checkpoint, and repair workflows

## AI workload surfaces

- [ ] Inference-cache adapter and key conventions
- [ ] Feature-value adapter
- [ ] Embedding metadata and vector-value storage surface
- [ ] Approximate-nearest-neighbor indexing and search integration

These surfaces will build on the ordered byte API. They are not present in the
current crate.

## Performance and operations

- [ ] Reproducible benchmark harness and documented datasets
- [ ] Published results with hardware and configuration disclosure
- [ ] Workload-oriented cache and write-stall telemetry
- [ ] Operational guidance for sizing and durability modes
- [ ] Crash and corruption compatibility matrix

The project publishes no benchmark results or comparative performance claims
today.

## Explicit non-goals

- A SQL parser, relational query planner, or relational schema layer
- A distributed consensus system or transparent multi-node database
- A network database service in the core storage-engine crate
- General multi-key transactions in the current architecture
- Replacing model-serving, feature-computation, or orchestration systems
