# Task 13 Report: Inference Cache Adapter

## Status

Implemented in:

- `22c9c92` `feat: add inference cache workload adapter`

The commit has no co-author trailer and was not pushed.

## What changed

- Added `InferenceCache`, `InferenceKey`, `InferenceEntry`, and `CacheLookup`.
- Added point get, atomic put, delete, snapshot-backed batch lookup, TTL, and
  `get_or_compute`.
- Added deterministic canonical keys for binary model names, model versions,
  generation parameters, namespaces, and inputs.
- Added process-local singleflight shared by cloned cache handles.
- Added checked versioned entry decoding instead of treating malformed data as
  a cache miss.
- Added explicit limits for namespaces, models, versions, inputs, parameters,
  canonical key material, payloads, and TTL values.
- Added BLAKE3 with its pure-Rust feature. No general serialization dependency
  was needed for the small key and value formats.

## Beginner walkthrough

### 1. Describing a request

`InferenceKey::new(model, model_version, input)` describes the parts of a model
request that affect its answer. `with_parameter(name, value)` adds generation
settings such as temperature or top-p.

The key owns model and parameter bytes. It immediately hashes the potentially
large input and retains only its length and 32-byte digest. Reusing a key
therefore does not retain or repeatedly hash a multi-megabyte prompt.

Parameters live in a `BTreeMap`. Rust keeps that map in bytewise sorted order,
so callers get the same durable key whether they insert `temperature` before
`top_p` or the other way around.

### 2. Canonical durable key layout

The stored byte key is:

```text
fixed inference-cache prefix
key schema byte
u32 namespace length | namespace bytes
u32 model length | model bytes
u32 model-version length | model-version bytes
u32 parameter count
  repeated in sorted order:
    u32 name length | name bytes
    u32 value length | value bytes
u64 original input length
32-byte BLAKE3 input digest
```

All integers are big-endian. Length prefixes make boundaries unambiguous:
`("ab", "c")` cannot encode like `("a", "bc")`. The fixed prefix keeps adapter
keys separate from unrelated raw engine keys. Namespace is part of the encoded
key, so two tenants using the same model request do not overlap.

The input length supplements the BLAKE3 digest and avoids storing the original
prompt in the database key. BLAKE3 gives a stable cross-process digest; Rust's
standard hash maps are intentionally unsuitable because their seeds and
iteration order are not stable storage formats.

### 3. Values and TTL

An `InferenceEntry` owns arbitrary binary payload bytes. Its stored format is:

```text
entry schema byte | u32 payload length | payload bytes
```

The small checked encoder avoids adding `serde` and `postcard` solely for one
byte vector. The schema byte reserves an explicit compatibility boundary.
Unknown schema versions return `UnsupportedFormat`; truncated or inconsistent
lengths return `Corruption`, never `Miss`.

`put(..., Some(ttl_ms))` uses the engine's serialized `put_with_ttl` path.
MeteorDB stores an absolute wall-clock deadline with the value. Before the
deadline, decoding returns `CacheLookup::Hit`; at or after it, the engine hides
the value and the adapter returns `CacheLookup::Miss`. A zero TTL is valid and
immediately invisible. Negative TTLs are rejected before writing.

Plain `put`, TTL `put`, and `delete` each delegate to one atomic engine write.
`get_many` first validates and encodes every key, then reads them through one
MVCC snapshot, preserving request order and duplicate positions.

### 4. Singleflight

`get_or_compute` first performs a normal lookup. On a miss, cloned cache handles
coordinate through a mutex-protected map keyed by the complete encoded key.

The first caller creates a flight and becomes its leader. Followers wait on a
condition variable without running their computation. The leader rechecks the
cache after winning the flight, computes outside all cache mutexes, atomically
stores the complete entry, publishes the result, and wakes every follower.

Followers receive the same entry or a reconstructed error with the same public
variant and diagnostic. If computation panics, followers are awakened with a
background error before the leader resumes unwinding, preventing abandoned
waiters. The flight is process-local, not a distributed lock.

## Collision and validation coverage

Tests prove that:

- different parameter insertion orders address the same entry;
- ambiguous model and parameter concatenations remain distinct;
- model versions and namespaces remain isolated;
- binary inputs and payloads round-trip;
- batch lookup preserves misses, duplicates, and request order;
- empty required fields, oversized namespace/input/payload, empty parameter
  names, and negative TTLs fail explicitly;
- 32 simultaneous misses execute one successful computation;
- concurrent followers receive the leader's error without storing a value.

Current adapter limits are 16 KiB for namespace/model/version or one parameter
component, 256 parameters, 512 KiB total non-input key material, and 16 MiB
each for input and payload. The underlying engine may impose stricter configured
key, value, or batch limits, which continue to return normal engine errors.

## TDD evidence

`crates/meteordb/tests/workloads.rs` was created before production code. The
first targeted GCC run failed because `CacheLookup`, `InferenceCache`,
`InferenceEntry`, and `InferenceKey` did not exist.

After implementation, the initial BLAKE3 build exposed an environment-specific
failure: its default assembly build required a missing `libisl.so.23` through
the repository-local GCC. Enabling BLAKE3's `pure` feature removed that C build
step while preserving the required algorithm.

A later size-limit increment was also observed red first: a 16 MiB-plus-one
input was initially accepted under the broader draft limit. Tightening the
adapter's input and payload limits made the focused validation test pass.

## Validation

All Cargo commands used:

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

cargo test -p meteordb --test workloads inference_cache
8 passed; 0 failed

cargo clippy --workspace --all-targets -- -D warnings
exit 0

cargo test --workspace
193 passed; 0 failed

RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
exit 0

git diff --check
exit 0
```

The clean pre-change baseline was also `cargo test --workspace --quiet` with
185 passing tests.

## Trade-offs and concerns

- Singleflight is shared by clones of one `InferenceCache`. Independently
  constructed adapters, other processes, and other machines do not coordinate.
- BLAKE3 collision risk is negligible but not mathematically impossible. Input
  length is encoded alongside the digest as an additional discriminator.
- The pure-Rust BLAKE3 feature favors reliable builds in this repository's GCC
  environment over optional C/assembly implementations.
- Canonical parameter bytes remain in the database key. The 512 KiB aggregate
  limit bounds key amplification while keeping exact parameter identity.
- Entries intentionally contain only the binary result in this task. The schema
  byte permits a future version to add application metadata without ambiguous
  decoding.
- Batch lookup has one MVCC sequence view, but TTL remains a wall-clock decision
  and is not frozen by snapshots, matching the engine's established contract.
