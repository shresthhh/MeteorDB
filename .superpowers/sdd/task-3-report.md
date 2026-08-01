# Task 3 Report: Atomic Batch Codec and Segmented WAL

## Status

`DONE`

Task 3 is implemented and committed on `feature/meteordb-engine`. MeteorDB now
encodes each `WriteBatch` as one atomic logical WAL record, fragments large
records across checksummed 32 KiB physical blocks, supports synchronous and
buffered durability, exposes replaceable durable filesystem operations, and
replays complete records while handling torn tails conservatively. No memtable
or engine write-coordinator code was added.

## Commit

- Commit: `8b313c7baec9d5eafd8703489cb9f3b50d171f78`
- Message: `feat: add checksummed segmented WAL`
- The commit has no Copilot co-author trailer.

## Changed Files

- `Cargo.toml` — adds the shared `crc32c` dependency.
- `Cargo.lock` — records the reproducible CRC32C dependency resolution.
- `crates/meteordb/Cargo.toml` — enables `crc32c` and the `tempfile` test
  dependency.
- `crates/meteordb/src/batch.rs` — adds the versioned atomic batch encoder and
  defensive decoder.
- `crates/meteordb/src/fs.rs` — defines `DurableFs` and `OsDurableFs`.
- `crates/meteordb/src/lib.rs` — registers and re-exports the Task 3 APIs while
  retaining the Task 1/2 exports.
- `crates/meteordb/src/wal.rs` — implements the segmented writer, checksum
  masking, durability control, and replay.
- `crates/meteordb/tests/wal.rs` — covers complete and torn records,
  fragmentation, corruption, limits, synchronization, format versions, and
  filesystem replacement.

## Test-Driven Development Record

The WAL integration tests were written before production code. The first host
run:

```bash
cargo test -p meteordb --test wal
```

stopped with exit 101 because the host has no `cc`. Using the repository's
existing ignored GCC environment, the same RED command failed for the expected
reason: unresolved imports for `WalWriter`, `replay_wal`, `DurableFs`, and
`OsDurableFs`.

After the minimal implementation, the focused suite passed nine tests. A
self-review then identified an exact-block-boundary edge case: a torn six-byte
header ending at byte 32,768 was classified as corruption because a full read
block was assumed to imply more file data. A regression test was added first
and failed with:

```text
Corruption { context: "WAL",
detail: "nonzero bytes in physical block trailer" }
```

Replay was changed to identify the final block from the file length rather than
from a short read. The focused suite then passed all ten tests.

## Exact Validation

All Cargo commands used:

```bash
ROOT="$PWD/.superpowers/sdd/local-toolchain/root"
export CC="$ROOT/usr/bin/gcc-13"
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER="$ROOT/usr/bin/gcc-13"
export LIBRARY_PATH="$ROOT/usr/lib/x86_64-linux-gnu:$ROOT/usr/lib/gcc/x86_64-linux-gnu/13"
```

The final pre-commit validation command was:

```bash
cargo fmt --check &&
cargo clippy -p meteordb --all-targets -- -D warnings &&
cargo test -p meteordb --test wal &&
cargo test -p meteordb &&
cargo doc -p meteordb --no-deps &&
git diff --check
```

Results:

- `cargo fmt --check`: passed.
- Clippy for all targets with warnings denied: passed.
- Focused WAL suite: 10 passed, 0 failed.
- Full crate suite: 23 integration tests passed, 0 failed; unit and doc tests
  had 0 failures.
- `cargo doc -p meteordb --no-deps`: passed under `#![deny(missing_docs)]`.
- `git diff --check`: passed.
- `git diff --cached --check`: passed immediately before commit.
- Post-commit branch status: clean on `feature/meteordb-engine`.

## Self-Review

- Checked every Task 3 interface and test category against the brief.
- Confirmed the existing `WriteBatch`, `Durability`, `SequenceNumber`, error,
  snapshot, and order-preserving internal-key APIs remain intact.
- Confirmed one batch becomes one logical record and replay publishes it only
  after every fragment and operation has validated.
- Confirmed physical fragments never cross 32 KiB blocks and use exactly the
  seven-byte little-endian header required by the plan.
- Confirmed CRC32C covers the fragment type and payload, is masked for storage,
  and is unmasked before comparison.
- Confirmed empty batches and payloads above `max_batch_bytes` are rejected
  before writing.
- Confirmed decoded lengths are checked with overflow-safe arithmetic against
  remaining validated bytes before keys or values are copied.
- Confirmed fragmented records round-trip puts, deletes, insertion order, and
  expiration timestamps.
- Confirmed buffered append does not call file sync, explicit `sync` does, and
  synchronous append calls sync before returning.
- Confirmed a short final header, short final payload, final bad checksum, or
  unfinished final fragment chain is ignored as a torn tail.
- Confirmed checksum damage or invalid fragment order before later records is
  reported as `Error::Corruption`.
- Confirmed unsupported logical format versions return
  `Error::UnsupportedFormat`.
- Confirmed the recovery parser reads fixed-size physical blocks instead of
  allocating from an untrusted file length.
- Confirmed `DurableFs` includes create, append, file sync, directory sync, and
  atomic replace, and that the standard implementation replaces and syncs in
  the filesystem test.
- Confirmed no memtable, write coordinator, SSTable, manifest, or compaction
  functionality entered this commit.
- Confirmed public rustdoc explains the binary format, fragmentation,
  little-endian integers, checksum masking, durability differences, torn-tail
  handling, and corruption policy in beginner-oriented terms.

## Concerns

1. The host still lacks a system `cc`; validation requires the ignored local
   GCC extraction described above. No toolchain artifacts were committed.
2. Recovery enforces a defensive 256 MiB encoded logical-record limit in
   addition to the writer's configured payload limit. This prevents an
   unbounded corrupt fragment chain from growing memory, but applications that
   deliberately configure batches near that size must remain below the encoded
   limit.
3. A checksum failure at the exact physical end of the WAL is treated as a torn
   final write. Recovery cannot distinguish power-loss damage from deliberate
   corruption when no later durable bytes exist; corruption before later bytes
   is always an error.
4. `sync_file` persists file contents, while persistence of a newly created or
   renamed filename additionally requires `sync_directory`. The abstraction
   exposes both operations; future segment/manifest coordination must call
   directory sync at the appropriate lifecycle boundary.

## Beginner-Oriented Code Walkthrough

### Atomic binary batch encoding

A `WriteBatch` may contain several ordered puts and deletes. Writing each
operation as a separate recovery record could expose half a batch after a
crash. MeteorDB instead serializes the whole batch into one logical payload:

```text
format_version:  u8
sequence:        u64
operation_count: u32
operations...
```

A put stores tag `1`, key and value lengths, an expiration-presence byte, an
optional expiration timestamp, then the key and value bytes. A delete stores
tag `2`, a key length, and the key. Operation order is unchanged, so replay
reconstructs the same atomic intent.

The decoder advances through a borrowed byte slice. Before it copies a key or
value, it uses checked addition and verifies that the declared length fits
inside the bytes that actually remain. Unknown tags, invalid expiration
markers, zero-operation records, truncated fields, and trailing bytes are
corruption rather than reasons to guess.

### Little-endian integers

Every multi-byte WAL integer uses little-endian order, meaning its least
significant byte is written first. Rust's `to_le_bytes` and `from_le_bytes`
make that choice explicit for sequence numbers, operation counts, lengths,
expiration timestamps, and checksums. A fixed byte order keeps files portable
between machines regardless of their processor's native integer order.

### Physical records versus logical records

The logical record is the complete atomic batch. The physical records are the
pieces used to place it on disk. Each physical fragment has:

```text
crc32c:        u32 little-endian
length:        u16 little-endian
fragment_type: u8
payload bytes
```

`FULL` means one fragment contains the whole logical record. Larger records use
`FIRST`, zero or more `MIDDLE` fragments, and `LAST`. Replay keeps fragments in
a temporary buffer and emits nothing until it sees a valid final fragment and
successfully decodes the complete logical batch.

### Why fragmentation exists

Physical blocks are 32 KiB. A batch can be much larger, and a smaller batch may
begin near the end of a block. Fragmentation lets the writer use the remaining
space, continue at the next block, and recover record boundaries without
requiring every batch to fit one block or one giant disk write. When fewer than
seven bytes remain, the writer pads that tiny trailer and starts the next
header at a new block.

### Checksums and masking

CRC32C detects accidental changes to each fragment. MeteorDB checksums the
fragment type as well as the payload, so changing `FIRST` into `MIDDLE` also
invalidates the checksum. Before storage, the CRC is rotated and incremented by
a fixed constant. This reversible mask prevents common raw CRC patterns from
recurring unchanged in data that may itself contain checksums. Replay reverses
the mask and compares the calculated CRC.

### Durability and synchronization

`Durability::Buffered` means `append` has handed bytes to the operating system,
but those bytes may still live only in volatile caches. It is faster, but a
power loss can discard an acknowledged append. Calling `WalWriter::sync`
later requests stable storage for all prior appends.

`Durability::Sync` performs that file synchronization before `append` returns.
This provides the stronger file-content guarantee at the cost of storage
latency. Directory synchronization is separate because persisting a file's
bytes does not necessarily persist the directory entry that names a newly
created or renamed file.

### Durable filesystem trait

`DurableFs` groups the crash-sensitive primitives: create, append, file sync,
directory sync, and atomic replace. `OsDurableFs` delegates them to the standard
library. `WalWriter::create_with_fs` accepts an `Arc<dyn DurableFs>`, allowing a
future crash test to inject a failure at one precise operation without changing
WAL logic or relying on nondeterministic real power loss.

Atomic replace uses a same-filesystem rename. A reader therefore sees either
the old complete destination or the new complete destination, not a half-copied
metadata file. A directory sync is still needed when the rename itself must
survive a crash.

### Recovery decisions

Replay reads one bounded physical block at a time. It validates header lengths,
block boundaries, checksum, and fragment order. Complete logical records are
decoded and returned in append order.

Damage exactly at the file end is treated as a torn tail: the final write may
have stopped midway through a header, payload, checksum-protected fragment, or
fragment chain. That incomplete logical batch is discarded in full. Earlier
damage is different. If later bytes exist, silently skipping the damaged region
could expose stale state or lose a committed batch, so replay returns
`Error::Corruption`.

The exact-block-boundary regression matters because a final read can contain
all 32 KiB and still be the final block. Replay compares its absolute position
with the file's metadata length rather than assuming only a short read can be
the tail.

---

## Task 3 Review Fixes

### Status

`DONE`

All Task 3 review findings are fixed without adding memtable, engine
coordination, manifest, SSTable, or compaction behavior. The fix commit uses
the message `fix: address Task 3 review findings` and has no Copilot co-author
trailer.

### Review-Fix TDD Record

Tests were added before each production change and observed failing:

1. `checksum_corruption_in_the_final_record_is_an_error` initially returned
   `Ok([])` instead of `Error::Corruption`; the adjacent truncation and
   unfinished-chain tests already passed.
2. The injectable durability tests initially failed to compile because
   `DurableFile` did not exist. They specify create/directory-sync/write/file-
   sync ordering plus directory-sync, write, and atomic-replacement failures.
3. The replay-limit tests initially failed to compile because `replay_wal`
   accepted no limit. They specify a matching `max_batch_bytes`, oversized
   replay rejection, and checked encoded-overhead overflow.

After each minimal implementation, the focused WAL suite returned to green.

### Exact Validation

Validation used the repository's ignored local GCC environment:

```bash
ROOT="$PWD/.superpowers/sdd/local-toolchain/root"
export CC="$ROOT/usr/bin/gcc-13"
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER="$ROOT/usr/bin/gcc-13"
export LIBRARY_PATH="$ROOT/usr/lib/x86_64-linux-gnu:$ROOT/usr/lib/gcc/x86_64-linux-gnu/13"
```

The exact final command was:

```bash
cargo fmt --check &&
cargo clippy -p meteordb --all-targets -- -D warnings &&
cargo test -p meteordb --test wal &&
cargo test -p meteordb &&
cargo doc -p meteordb --no-deps &&
git diff --check
```

Results:

- Formatting passed.
- Clippy passed for all crate targets with warnings denied.
- Focused WAL tests: 17 passed, 0 failed.
- Full crate tests: 30 integration tests passed, 0 failed; unit and doc tests
  had 0 failures.
- Rustdoc passed under `#![deny(missing_docs)]`.
- `git diff --check` passed.

### Review-Fix Self-Review

- Confirmed an invalid CRC32C always returns `Error::Corruption`, even when the
  damaged fragment ends exactly at EOF.
- Confirmed only structural tail incompleteness is ignored: fewer than seven
  final header bytes, a declared final payload cut short by EOF, or EOF while a
  valid FIRST/MIDDLE chain remains unfinished.
- Confirmed the seven-byte physical header, CRC masking, 32 KiB blocks,
  FULL/FIRST/MIDDLE/LAST values, fragmentation, and atomic logical-record
  publication are unchanged.
- Confirmed writer creation orders `create` before `sync_directory`, and no
  synchronous append can succeed if that directory sync fails.
- Confirmed synchronous append orders physical writes before file sync and
  propagates injected write errors without attempting file sync.
- Confirmed WAL physical writes and file synchronization use `DurableFile`;
  directory synchronization and atomic replacement remain injectable through
  `DurableFs`. No WAL `File::write_all` call remains.
- Confirmed replay now requires the writer's `max_batch_bytes`, rejects decoded
  payloads above it, and uses the same checked encoded-record ceiling as the
  writer before fragment accumulation.
- Confirmed the encoded ceiling uses checked multiplication/addition for the
  13-byte logical header, payload allowance, and worst per-operation metadata
  allowance; overflow is an actionable `Error::InvalidArgument`.
- Confirmed public rustdoc, the design document, plan example, tests, and
  exports reflect the new replay and durable-file APIs.

### Updated Beginner Explanations

#### Corruption is different from truncation

Recovery ignores a final record only when its shape proves the write stopped
early: the last header has fewer than seven bytes, the header declares payload
bytes that are not present, or a valid fragmented record reaches EOF before a
LAST fragment. A checksum mismatch is different. The header and payload bytes
are present, but they do not contain what the writer checksummed, so replay
always reports corruption rather than silently dropping a possibly committed
batch.

#### Why the directory is synchronized at writer creation

Synchronizing a WAL file persists its contents, but a newly created filename
is stored in its parent directory. MeteorDB therefore creates the file and
synchronizes the parent directory before returning the writer. Later
`Durability::Sync` appends write the physical fragments and synchronize the
file. This order means a successful synchronous append cannot refer to a WAL
name whose creation was never durably acknowledged.

#### Why `DurableFile` exists

`DurableFs` creates files, opens append handles, synchronizes directories, and
performs atomic replacement. The returned `DurableFile` owns the crash-
sensitive byte writes and file synchronization. Tests can wrap both traits to
record the exact operation order or fail one selected operation, while the OS
implementation still delegates to ordinary files.

#### Why replay receives `max_batch_bytes`

The old 256 MiB replay constant could disagree with the writer's configured
batch limit. `replay_wal(path, max_batch_bytes)` now receives the same limit
used to create the writer. Recovery checks the decoded key/value payload and
also derives a larger encoded ceiling that includes the 13-byte logical header
and operation metadata. All arithmetic is checked before allocation, so an
overflowing configuration is rejected rather than wrapping to a small or
unbounded limit.

### Remaining Concerns

The host still lacks a system `cc`, so Cargo validation requires the ignored
local GCC environment above. The previous concerns about a fixed 256 MiB replay
limit, EOF checksum suppression, and unsynchronized new WAL names are resolved.
