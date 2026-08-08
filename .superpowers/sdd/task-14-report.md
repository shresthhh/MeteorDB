# Task 14 Report: Feature and Embedding Store Adapters

## Status

Implemented in:

- `11498f0` `feat: add feature and embedding adapters`
- `766ec96` `fix: bound workload adapter memory`

The commits have no co-author trailers and were not pushed.

## What changed

- Added `FeatureStore`, `FeatureKey`, `FeatureRecord`, and `FeatureValue`.
- Added `EmbeddingStore`, `EmbeddingKey`, `Embedding`, and `ScalarType`.
- Added versioned, binary-safe ordered key layouts with namespace isolation.
- Added chronological feature history, latest, as-of, group scan, exact lookup,
  and snapshot-backed ordered batch lookup.
- Added atomic complete-row feature replacement and optional TTL.
- Added deterministic typed feature rows backed by a bytewise ordered map.
- Added checked F32 and F16 embedding construction, portable little-endian
  vector bytes, borrowed byte access, and explicit conversion helpers.
- Added embedding model/version isolation, metadata, point and ordered batch
  reads, atomic batch writes, delete, TTL, and restart persistence.
- Added strict limits, finite-float checks, schema checks, canonical decoding,
  and corruption reporting.
- Added checked conservative feature-row sizing before mutation and
  serialization, preserving the 16 MiB postcard-body limit without allowing an
  unbounded serialization allocation.
- Changed public feature history/group retrieval to require a caller limit.
- Changed `latest` and `as_of` to stream the ordered engine iterator while
  retaining only the newest encoded candidate; `as_of` ends at the encoded
  requested timestamp.
- Added checked projected metadata accounting before mutation, including
  subtracting a replaced entry.

## Beginner walkthrough

### 1. Feature keys

A `FeatureKey` describes:

```text
entity type | entity id | feature group | signed event time
```

The durable key begins with a fixed adapter prefix and schema byte. Namespace,
entity type, entity ID, and group are each escaped independently:

- an ordinary byte is copied;
- an embedded zero becomes `00 ff`;
- the component ends with `00 00`.

This makes boundaries unambiguous even when callers use arbitrary binary data.
It also preserves byte ordering and allows a complete entity/group prefix to be
scanned without overlapping another namespace or entity.

The signed `i64` event time is converted to `u64`, has its sign bit flipped, and
is written big-endian. Lexicographic byte order then matches signed
chronological order, including negative event times.

### 2. Feature rows and event-time reads

`FeatureRecord` is a `BTreeMap` from binary names to typed `FeatureValue`
variants: null, Boolean, `i64`, finite `f64`, bytes, or UTF-8 string. The map's
bytewise ordering makes serialization deterministic regardless of insertion
order.

A complete row is encoded as one versioned value and written through one engine
put. Replacing a row cannot expose a partially updated set of features.
Optional TTL delegates to the engine's serialized TTL write path.

`history` constructs inclusive scan bounds by appending the encoded start and
end times to the group prefix. Results arrive oldest first, and callers must
supply the maximum number of rows. `scan_group` likewise requires a limit.
`latest` and `as_of` do not build a history `Vec`; they stream the ordered
iterator and retain only the newest encoded key/value candidate, decoding that
single candidate after the scan. `as_of` uses the requested event time as its
encoded upper bound. `get_many` validates all keys first, captures one MVCC
snapshot, and preserves misses, duplicates, and request order.

### 3. Embedding keys and values

Embedding keys contain:

```text
fixed prefix | key schema | namespace | optional model |
optional model version | entity id
```

Optional identity uses an explicit presence marker, so absence cannot collide
with an empty or present field. Model and version must both be present or both
be absent. The same identity is stored in the embedding value and checked
against the key before writing.

An `Embedding` stores dimension, scalar type, portable vector bytes,
model/version identity, and bytewise ordered binary metadata. Values have a
leading schema byte and a checked canonical postcard body.

### 4. Portable vectors

`Embedding::from_f32` writes each finite `f32` with `to_le_bytes`.
`Embedding::from_f16_bits` writes each finite IEEE-754 binary16 bit pattern with
`u16::to_le_bytes`. The stored representation is therefore independent of host
endianness.

`vector_bytes` borrows the stored bytes without allocating. `to_f32` and
`to_f16_bits` are explicit checked conversions and reject the wrong scalar
type. Raw construction verifies:

- dimension is nonzero and fits `u32`;
- byte length equals dimension times scalar width;
- vector bytes stay within 64 MiB;
- every decoded F32/F16 value is finite.

### 5. Batch and persistence behavior

Embedding `put_many` validates and encodes every item into one `WriteBatch`
before calling the engine, so the batch is committed at one sequence or not
written. `get_many` uses one snapshot and preserves input order and duplicates.
Point puts support TTL, while batch puts are intentionally non-expiring.

Restart tests close and reopen the engine, then compare the complete embedding,
including scalar type, little-endian bytes, model identity, and metadata.

## Validation and TDD evidence

The adapter tests were added before production code. The first targeted GCC run
failed with unresolved imports for every new adapter type. A later zero-dimension
test was also observed failing before the validation was added.

For the review fixes, tests were changed first. The first GCC workload run
failed to compile because `history` and `scan_group` did not yet accept caller
limits. After the limited API was added, the next GCC workload run compiled and
failed only these new regressions:

```text
feature_record_rejects_projected_aggregate_over_encoded_limit_before_mutation
embedding_metadata_replacement_checks_projected_total_before_mutation
```

Both failed because the inserts incorrectly succeeded. After the checked
projected-size validation was implemented, all 23 workload tests passed.

GCC environment:

```bash
ROOT="$PWD/.superpowers/sdd/local-toolchain/root"
export CC="$ROOT/usr/bin/gcc-13"
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER="$ROOT/usr/bin/gcc-13"
export LIBRARY_PATH="$ROOT/usr/lib/x86_64-linux-gnu:$ROOT/usr/lib/gcc/x86_64-linux-gnu/13"
export CARGO_INCREMENTAL=0
```

Fresh final gates before the implementation commit:

```text
cargo fmt --all --check
exit 0

cargo test -p meteordb --test workloads feature_store
4 passed; 0 failed

cargo test -p meteordb --test workloads embedding
3 passed; 0 failed

cargo test -p meteordb --test workloads
21 passed; 0 failed

cargo test --workspace
206 passed; 0 failed

cargo test --workspace --doc
0 passed; 0 failed

cargo clippy --workspace --all-targets -- -D warnings
exit 0

RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
exit 0

git diff --check
exit 0
```

Fresh GCC gates after `766ec96`:

```text
cargo fmt --all --check
exit 0

cargo test -p meteordb --test workloads
23 passed; 0 failed

cargo test --workspace
208 passed; 0 failed

cargo test --workspace --doc
0 passed; 0 failed

cargo clippy --workspace --all-targets -- -D warnings
exit 0

RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
exit 0

git diff --check
exit 0
```

## Trade-offs and concerns

- `latest` and `as_of` retain only one candidate, but the engine exposes only
  forward iteration. They therefore still perform linear I/O across the
  selected key range; `as_of` avoids scanning keys after its timestamp.
- Feature sizing is conservative for signed integers (up to ten postcard
  bytes), so a row within a few bytes of 16 MiB can be rejected even if its
  exact encoding would fit. This guarantees the serializer's allocation is
  bounded and never permits an encoded body over 16 MiB.
- Feature scans return owned rows. This keeps the public API simple but copies
  decoded feature values.
- Embedding vector bytes are borrowed from the owned `Embedding`; typed
  conversion allocates a new vector to avoid alignment and aliasing hazards.
- F16 is represented as checked `u16` bit patterns, avoiding a new half-float
  dependency. Numeric F16 arithmetic remains the caller's responsibility.
- Metadata is limited to 1,024 entries, 1 MiB per name/value component, and
  16 MiB total. Vectors are limited to 64 MiB.
- Individual feature and embedding writes support TTL. Atomic embedding batch
  writes intentionally omit relative TTL because the public engine does not
  expose its clock for one shared deadline calculation.
- Snapshot sequence consistency does not freeze wall-clock TTL evaluation,
  matching the engine's established snapshot contract.
- This task implements storage and retrieval only. It does not perform exact
  nearest-neighbor search or build an ANN index.
