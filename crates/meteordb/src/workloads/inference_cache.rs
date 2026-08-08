use std::collections::{BTreeMap, HashMap};
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use crate::{Engine, Error, Result};

const KEY_PREFIX: &[u8] = b"\0meteordb:inference-cache\0";
const KEY_SCHEMA_VERSION: u8 = 1;
const ENTRY_SCHEMA_VERSION: u8 = 1;
const MAX_NAMESPACE_BYTES: usize = 16 * 1024;
const MAX_MODEL_BYTES: usize = 16 * 1024;
const MAX_MODEL_VERSION_BYTES: usize = 16 * 1024;
const MAX_INPUT_BYTES: usize = 16 * 1024 * 1024;
const MAX_PARAMETER_COUNT: usize = 256;
const MAX_PARAMETER_COMPONENT_BYTES: usize = 16 * 1024;
const MAX_KEY_MATERIAL_BYTES: usize = 512 * 1024;
const MAX_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;

/// The model request fields that determine whether an inference result is reusable.
///
/// Parameter names are maintained in bytewise sorted order, so constructing the
/// same request in a different map insertion order produces the same durable key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InferenceKey {
    model: Vec<u8>,
    model_version: Vec<u8>,
    input_len: usize,
    input_digest: [u8; 32],
    parameters: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl InferenceKey {
    /// Creates a key from model identity, model version, and arbitrary input bytes.
    pub fn new(
        model: impl AsRef<[u8]>,
        model_version: impl AsRef<[u8]>,
        input: impl AsRef<[u8]>,
    ) -> Self {
        let input = input.as_ref();
        Self {
            model: model.as_ref().to_vec(),
            model_version: model_version.as_ref().to_vec(),
            input_len: input.len(),
            input_digest: *blake3::hash(input).as_bytes(),
            parameters: BTreeMap::new(),
        }
    }

    /// Adds or replaces one generation parameter and returns the updated key.
    ///
    /// Names and values are binary-safe. Repeating a name uses the last value,
    /// matching ordinary map semantics.
    pub fn with_parameter(mut self, name: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Self {
        self.parameters
            .insert(name.as_ref().to_vec(), value.as_ref().to_vec());
        self
    }
}

/// One binary inference result stored in the cache.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InferenceEntry {
    payload: Vec<u8>,
}

impl InferenceEntry {
    /// Creates an entry by copying arbitrary result bytes.
    pub fn new(payload: impl AsRef<[u8]>) -> Self {
        Self {
            payload: payload.as_ref().to_vec(),
        }
    }

    /// Borrows the cached result bytes.
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Consumes the entry and returns its result bytes.
    pub fn into_payload(self) -> Vec<u8> {
        self.payload
    }
}

/// Outcome of a cache lookup.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CacheLookup {
    /// No live entry exists for the requested canonical key.
    Miss,
    /// A live, successfully decoded entry was found.
    Hit(InferenceEntry),
}

/// A namespace-isolated inference cache backed by a cloneable [`Engine`] handle.
///
/// Clones share one process-local singleflight table. This coalesces concurrent
/// misses in this process but deliberately does not act as a distributed lock.
#[derive(Clone)]
pub struct InferenceCache {
    inner: Arc<CacheInner>,
}

struct CacheInner {
    engine: Engine,
    namespace: Vec<u8>,
    flights: Mutex<HashMap<Vec<u8>, Arc<Flight>>>,
}

struct Flight {
    outcome: Mutex<Option<SharedResult>>,
    ready: Condvar,
}

type SharedResult = std::result::Result<InferenceEntry, SharedError>;

#[derive(Clone)]
enum SharedError {
    InvalidArgument(String),
    Io {
        operation: &'static str,
        path: PathBuf,
        kind: std::io::ErrorKind,
        message: String,
    },
    Corruption {
        context: &'static str,
        detail: String,
    },
    UnsupportedFormat {
        kind: &'static str,
        version: u32,
    },
    Locked(PathBuf),
    Closed,
    Background(String),
    WriteStall {
        immutable_memtables: usize,
    },
}

impl InferenceCache {
    /// Creates a cache whose keys cannot overlap keys from another namespace.
    pub fn new(engine: Engine, namespace: impl AsRef<[u8]>) -> Result<Self> {
        let namespace = namespace.as_ref();
        validate_required("namespace", namespace)?;
        validate_max("namespace", namespace.len(), MAX_NAMESPACE_BYTES)?;
        Ok(Self {
            inner: Arc::new(CacheInner {
                engine,
                namespace: namespace.to_vec(),
                flights: Mutex::new(HashMap::new()),
            }),
        })
    }

    /// Returns the live entry for `key`, or [`CacheLookup::Miss`] when absent or expired.
    pub fn get(&self, key: &InferenceKey) -> Result<CacheLookup> {
        let encoded_key = self.encode_key(key)?;
        self.lookup_encoded(&encoded_key)
    }

    /// Looks up every request against one MVCC snapshot and preserves input order.
    ///
    /// Duplicate requests produce duplicate output positions.
    pub fn get_many<'a>(
        &self,
        keys: impl IntoIterator<Item = &'a InferenceKey>,
    ) -> Result<Vec<CacheLookup>> {
        let encoded_keys = keys
            .into_iter()
            .map(|key| self.encode_key(key))
            .collect::<Result<Vec<_>>>()?;
        let snapshot = self.inner.engine.snapshot()?;
        encoded_keys
            .iter()
            .map(|key| match snapshot.get(key)? {
                Some(bytes) => decode_entry(&bytes).map(CacheLookup::Hit),
                None => Ok(CacheLookup::Miss),
            })
            .collect()
    }

    /// Atomically stores one entry, optionally expiring it after `ttl_ms`.
    ///
    /// A zero TTL is valid and immediately hides the new value. Negative TTLs
    /// and oversized fields are rejected before entering the engine write path.
    pub fn put(
        &self,
        key: &InferenceKey,
        entry: InferenceEntry,
        ttl_ms: Option<i64>,
    ) -> Result<()> {
        let encoded_key = self.encode_key(key)?;
        let encoded_entry = encode_entry(&entry)?;
        validate_ttl(ttl_ms)?;
        match ttl_ms {
            Some(ttl_ms) => self
                .inner
                .engine
                .put_with_ttl(encoded_key, encoded_entry, ttl_ms),
            None => self.inner.engine.put(encoded_key, encoded_entry),
        }
    }

    /// Atomically removes the exact canonical key.
    pub fn delete(&self, key: &InferenceKey) -> Result<()> {
        self.inner.engine.delete(self.encode_key(key)?)
    }

    /// Returns a cached entry or computes and stores it once per concurrent miss.
    ///
    /// Followers waiting on the same encoded key receive the leader's complete
    /// entry or the same structured error. A panic in the computation wakes
    /// followers with a background error before the leader resumes unwinding.
    pub fn get_or_compute<F>(
        &self,
        key: &InferenceKey,
        ttl_ms: Option<i64>,
        compute: F,
    ) -> Result<InferenceEntry>
    where
        F: FnOnce() -> Result<InferenceEntry>,
    {
        let encoded_key = self.encode_key(key)?;
        validate_ttl(ttl_ms)?;
        if let CacheLookup::Hit(entry) = self.lookup_encoded(&encoded_key)? {
            return Ok(entry);
        }

        let (flight, leader) = {
            let mut flights = lock_unpoisoned(&self.inner.flights);
            if let Some(flight) = flights.get(&encoded_key) {
                (flight.clone(), false)
            } else {
                let flight = Arc::new(Flight {
                    outcome: Mutex::new(None),
                    ready: Condvar::new(),
                });
                flights.insert(encoded_key.clone(), flight.clone());
                (flight, true)
            }
        };

        if !leader {
            return wait_for_flight(&flight);
        }

        let operation = catch_unwind(AssertUnwindSafe(|| {
            if let CacheLookup::Hit(entry) = self.lookup_encoded(&encoded_key)? {
                return Ok(entry);
            }
            let entry = compute()?;
            let encoded_entry = encode_entry(&entry)?;
            match ttl_ms {
                Some(ttl_ms) => {
                    self.inner
                        .engine
                        .put_with_ttl(encoded_key.clone(), encoded_entry, ttl_ms)?;
                }
                None => self.inner.engine.put(encoded_key.clone(), encoded_entry)?,
            }
            Ok(entry)
        }));

        match operation {
            Ok(result) => {
                let shared = result
                    .as_ref()
                    .map(Clone::clone)
                    .map_err(SharedError::from_error);
                self.finish_flight(&encoded_key, &flight, shared);
                result
            }
            Err(panic) => {
                self.finish_flight(
                    &encoded_key,
                    &flight,
                    Err(SharedError::Background(
                        "inference computation panicked".into(),
                    )),
                );
                resume_unwind(panic)
            }
        }
    }

    fn lookup_encoded(&self, encoded_key: &[u8]) -> Result<CacheLookup> {
        match self.inner.engine.get(encoded_key)? {
            Some(bytes) => decode_entry(&bytes).map(CacheLookup::Hit),
            None => Ok(CacheLookup::Miss),
        }
    }

    fn encode_key(&self, key: &InferenceKey) -> Result<Vec<u8>> {
        validate_required("model", &key.model)?;
        validate_max("model", key.model.len(), MAX_MODEL_BYTES)?;
        validate_required("model_version", &key.model_version)?;
        validate_max(
            "model_version",
            key.model_version.len(),
            MAX_MODEL_VERSION_BYTES,
        )?;
        validate_max("input", key.input_len, MAX_INPUT_BYTES)?;
        validate_max("parameter count", key.parameters.len(), MAX_PARAMETER_COUNT)?;

        let mut material_bytes = self
            .inner
            .namespace
            .len()
            .checked_add(key.model.len())
            .and_then(|size| size.checked_add(key.model_version.len()))
            .ok_or_else(|| Error::InvalidArgument("cache key size overflow".into()))?;
        for (name, value) in &key.parameters {
            validate_required("parameter name", name)?;
            validate_max("parameter name", name.len(), MAX_PARAMETER_COMPONENT_BYTES)?;
            validate_max(
                "parameter value",
                value.len(),
                MAX_PARAMETER_COMPONENT_BYTES,
            )?;
            material_bytes = material_bytes
                .checked_add(name.len())
                .and_then(|size| size.checked_add(value.len()))
                .ok_or_else(|| Error::InvalidArgument("cache key size overflow".into()))?;
        }
        validate_max(
            "canonical key material",
            material_bytes,
            MAX_KEY_MATERIAL_BYTES,
        )?;

        let mut encoded = Vec::with_capacity(
            KEY_PREFIX.len() + 1 + material_bytes + key.parameters.len() * 8 + 60,
        );
        encoded.extend_from_slice(KEY_PREFIX);
        encoded.push(KEY_SCHEMA_VERSION);
        push_bytes(&mut encoded, &self.inner.namespace);
        push_bytes(&mut encoded, &key.model);
        push_bytes(&mut encoded, &key.model_version);
        push_u32(&mut encoded, key.parameters.len());
        for (name, value) in &key.parameters {
            push_bytes(&mut encoded, name);
            push_bytes(&mut encoded, value);
        }
        encoded.extend_from_slice(&(key.input_len as u64).to_be_bytes());
        encoded.extend_from_slice(&key.input_digest);
        Ok(encoded)
    }

    fn finish_flight(&self, key: &[u8], flight: &Arc<Flight>, result: SharedResult) {
        *lock_unpoisoned(&flight.outcome) = Some(result);
        {
            let mut flights = lock_unpoisoned(&self.inner.flights);
            if flights
                .get(key)
                .is_some_and(|current| Arc::ptr_eq(current, flight))
            {
                flights.remove(key);
            }
        }
        flight.ready.notify_all();
    }
}

fn encode_entry(entry: &InferenceEntry) -> Result<Vec<u8>> {
    validate_max("payload", entry.payload.len(), MAX_PAYLOAD_BYTES)?;
    let mut encoded = Vec::with_capacity(5 + entry.payload.len());
    encoded.push(ENTRY_SCHEMA_VERSION);
    push_bytes(&mut encoded, &entry.payload);
    Ok(encoded)
}

fn decode_entry(bytes: &[u8]) -> Result<InferenceEntry> {
    let Some((&version, body)) = bytes.split_first() else {
        return Err(entry_corruption("missing schema version"));
    };
    if version != ENTRY_SCHEMA_VERSION {
        return Err(Error::UnsupportedFormat {
            kind: "inference-cache entry",
            version: u32::from(version),
        });
    }
    if body.len() < 4 {
        return Err(entry_corruption("missing payload length"));
    }
    let payload_len = u32::from_be_bytes(body[..4].try_into().unwrap()) as usize;
    let payload = &body[4..];
    if payload.len() != payload_len {
        return Err(entry_corruption(
            "payload length does not match entry bytes",
        ));
    }
    validate_max("stored payload", payload.len(), MAX_PAYLOAD_BYTES)
        .map_err(|error| entry_corruption(error.to_string()))?;
    Ok(InferenceEntry::new(payload))
}

fn wait_for_flight(flight: &Flight) -> Result<InferenceEntry> {
    let mut outcome = lock_unpoisoned(&flight.outcome);
    while outcome.is_none() {
        outcome = match flight.ready.wait(outcome) {
            Ok(outcome) => outcome,
            Err(poisoned) => poisoned.into_inner(),
        };
    }
    outcome
        .as_ref()
        .expect("condition loop requires a completed outcome")
        .clone()
        .map_err(SharedError::into_error)
}

fn push_bytes(output: &mut Vec<u8>, bytes: &[u8]) {
    push_u32(output, bytes.len());
    output.extend_from_slice(bytes);
}

fn push_u32(output: &mut Vec<u8>, value: usize) {
    output.extend_from_slice(
        &u32::try_from(value)
            .expect("validated cache component length fits in u32")
            .to_be_bytes(),
    );
}

fn validate_required(name: &str, bytes: &[u8]) -> Result<()> {
    if bytes.is_empty() {
        return Err(Error::InvalidArgument(format!("{name} must not be empty")));
    }
    Ok(())
}

fn validate_max(name: &str, actual: usize, maximum: usize) -> Result<()> {
    if actual > maximum {
        return Err(Error::InvalidArgument(format!(
            "{name} is {actual} bytes/items, exceeding the {maximum} limit"
        )));
    }
    Ok(())
}

fn validate_ttl(ttl_ms: Option<i64>) -> Result<()> {
    if ttl_ms.is_some_and(|ttl_ms| ttl_ms < 0) {
        return Err(Error::InvalidArgument("ttl_ms must not be negative".into()));
    }
    Ok(())
}

fn entry_corruption(detail: impl Into<String>) -> Error {
    Error::Corruption {
        context: "inference-cache entry",
        detail: detail.into(),
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl SharedError {
    fn from_error(error: &Error) -> Self {
        match error {
            Error::InvalidArgument(message) => Self::InvalidArgument(message.clone()),
            Error::Io {
                operation,
                path,
                source,
            } => Self::Io {
                operation,
                path: path.clone(),
                kind: source.kind(),
                message: source.to_string(),
            },
            Error::Corruption { context, detail } => Self::Corruption {
                context,
                detail: detail.clone(),
            },
            Error::UnsupportedFormat { kind, version } => Self::UnsupportedFormat {
                kind,
                version: *version,
            },
            Error::Locked(path) => Self::Locked(path.clone()),
            Error::Closed => Self::Closed,
            Error::Background(message) => Self::Background(message.clone()),
            Error::WriteStall {
                immutable_memtables,
            } => Self::WriteStall {
                immutable_memtables: *immutable_memtables,
            },
        }
    }

    fn into_error(self) -> Error {
        match self {
            Self::InvalidArgument(message) => Error::InvalidArgument(message),
            Self::Io {
                operation,
                path,
                kind,
                message,
            } => Error::Io {
                operation,
                path,
                source: std::io::Error::new(kind, message),
            },
            Self::Corruption { context, detail } => Error::Corruption { context, detail },
            Self::UnsupportedFormat { kind, version } => Error::UnsupportedFormat { kind, version },
            Self::Locked(path) => Error::Locked(path),
            Self::Closed => Error::Closed,
            Self::Background(message) => Error::Background(message),
            Self::WriteStall {
                immutable_memtables,
            } => Error::WriteStall {
                immutable_memtables,
            },
        }
    }
}
