use std::collections::BTreeMap;
use std::ops::{Bound, RangeInclusive};

use serde::{Deserialize, Serialize};

use crate::{Engine, Error, Result, ScanBounds};

const KEY_PREFIX: &[u8] = b"\0meteordb:feature-store\0";
const KEY_SCHEMA_VERSION: u8 = 1;
const RECORD_SCHEMA_VERSION: u8 = 1;
const MAX_COMPONENT_BYTES: usize = 16 * 1024;
const MAX_FEATURE_COUNT: usize = 1_024;
const MAX_FEATURE_NAME_BYTES: usize = 16 * 1024;
const MAX_FEATURE_VALUE_BYTES: usize = 16 * 1024 * 1024;

/// The entity, group, and event time identifying one immutable feature row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FeatureKey {
    entity_type: Vec<u8>,
    entity_id: Vec<u8>,
    feature_group: Vec<u8>,
    event_time: i64,
}

impl FeatureKey {
    /// Creates a binary-safe feature key at `event_time`.
    pub fn new(
        entity_type: impl AsRef<[u8]>,
        entity_id: impl AsRef<[u8]>,
        feature_group: impl AsRef<[u8]>,
        event_time: i64,
    ) -> Self {
        Self {
            entity_type: entity_type.as_ref().to_vec(),
            entity_id: entity_id.as_ref().to_vec(),
            feature_group: feature_group.as_ref().to_vec(),
            event_time,
        }
    }

    /// Borrows the entity type.
    pub fn entity_type(&self) -> &[u8] {
        &self.entity_type
    }

    /// Borrows the entity identifier.
    pub fn entity_id(&self) -> &[u8] {
        &self.entity_id
    }

    /// Borrows the feature group.
    pub fn feature_group(&self) -> &[u8] {
        &self.feature_group
    }

    /// Returns the signed event or version time.
    pub fn event_time(&self) -> i64 {
        self.event_time
    }
}

/// One deterministically encoded typed feature value.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub enum FeatureValue {
    /// A missing value represented explicitly.
    Null,
    /// A Boolean value.
    Bool(bool),
    /// A signed 64-bit integer.
    I64(i64),
    /// A finite 64-bit floating-point number.
    F64(f64),
    /// Arbitrary binary bytes.
    Bytes(Vec<u8>),
    /// A UTF-8 string.
    String(String),
}

/// One atomic feature row whose names are maintained in bytewise order.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct FeatureRecord {
    features: BTreeMap<Vec<u8>, FeatureValue>,
}

impl FeatureRecord {
    /// Creates an empty feature row.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds or replaces a named value and returns this row for chaining.
    pub fn insert(&mut self, name: impl AsRef<[u8]>, value: FeatureValue) -> Result<&mut Self> {
        validate_feature(name.as_ref(), &value)?;
        if !self.features.contains_key(name.as_ref()) && self.features.len() >= MAX_FEATURE_COUNT {
            return Err(Error::InvalidArgument(format!(
                "feature count exceeds the {MAX_FEATURE_COUNT} limit"
            )));
        }
        self.features.insert(name.as_ref().to_vec(), value);
        Ok(self)
    }

    /// Returns a named value.
    pub fn get(&self, name: impl AsRef<[u8]>) -> Option<&FeatureValue> {
        self.features.get(name.as_ref())
    }

    /// Borrows all features in deterministic bytewise name order.
    pub fn features(&self) -> &BTreeMap<Vec<u8>, FeatureValue> {
        &self.features
    }
}

/// A namespace-isolated event-time feature store backed by [`Engine`].
#[derive(Clone)]
pub struct FeatureStore {
    engine: Engine,
    namespace: Vec<u8>,
}

impl FeatureStore {
    /// Creates a feature store with an isolated binary namespace.
    pub fn new(engine: Engine, namespace: impl AsRef<[u8]>) -> Result<Self> {
        validate_component("namespace", namespace.as_ref())?;
        Ok(Self {
            engine,
            namespace: namespace.as_ref().to_vec(),
        })
    }

    /// Atomically replaces one complete feature row, optionally with a TTL.
    pub fn put(&self, key: &FeatureKey, record: &FeatureRecord, ttl_ms: Option<i64>) -> Result<()> {
        let key = self.encode_key(key)?;
        let record = encode_record(record)?;
        match ttl_ms {
            Some(ttl_ms) => self.engine.put_with_ttl(key, record, ttl_ms),
            None => self.engine.put(key, record),
        }
    }

    /// Reads the exact event-time row.
    pub fn get(&self, key: &FeatureKey) -> Result<Option<FeatureRecord>> {
        self.engine
            .get(self.encode_key(key)?)?
            .map(|bytes| decode_record(&bytes))
            .transpose()
    }

    /// Reads exact rows through one MVCC snapshot and preserves input order.
    pub fn get_many<'a>(
        &self,
        keys: impl IntoIterator<Item = &'a FeatureKey>,
    ) -> Result<Vec<Option<FeatureRecord>>> {
        let keys = keys
            .into_iter()
            .map(|key| self.encode_key(key))
            .collect::<Result<Vec<_>>>()?;
        let snapshot = self.engine.snapshot()?;
        keys.iter()
            .map(|key| {
                snapshot
                    .get(key)?
                    .map(|bytes| decode_record(&bytes))
                    .transpose()
            })
            .collect()
    }

    /// Returns the live row with the greatest event time.
    pub fn latest(
        &self,
        entity_type: impl AsRef<[u8]>,
        entity_id: impl AsRef<[u8]>,
        feature_group: impl AsRef<[u8]>,
    ) -> Result<Option<(FeatureKey, FeatureRecord)>> {
        self.scan_group(entity_type, entity_id, feature_group)
            .map(|rows| rows.into_iter().last())
    }

    /// Returns the newest live row whose event time is at most `event_time`.
    pub fn as_of(
        &self,
        entity_type: impl AsRef<[u8]>,
        entity_id: impl AsRef<[u8]>,
        feature_group: impl AsRef<[u8]>,
        event_time: i64,
    ) -> Result<Option<(FeatureKey, FeatureRecord)>> {
        self.history(entity_type, entity_id, feature_group, i64::MIN..=event_time)
            .map(|rows| rows.into_iter().last())
    }

    /// Scans all live versions in one entity's feature group chronologically.
    pub fn scan_group(
        &self,
        entity_type: impl AsRef<[u8]>,
        entity_id: impl AsRef<[u8]>,
        feature_group: impl AsRef<[u8]>,
    ) -> Result<Vec<(FeatureKey, FeatureRecord)>> {
        self.history(entity_type, entity_id, feature_group, i64::MIN..=i64::MAX)
    }

    /// Scans an inclusive event-time range in chronological order.
    pub fn history(
        &self,
        entity_type: impl AsRef<[u8]>,
        entity_id: impl AsRef<[u8]>,
        feature_group: impl AsRef<[u8]>,
        event_times: RangeInclusive<i64>,
    ) -> Result<Vec<(FeatureKey, FeatureRecord)>> {
        if event_times.is_empty() {
            return Ok(Vec::new());
        }
        let entity_type = entity_type.as_ref();
        let entity_id = entity_id.as_ref();
        let feature_group = feature_group.as_ref();
        let prefix = self.encode_group_prefix(entity_type, entity_id, feature_group)?;
        let start_time = *event_times.start();
        let end_time = *event_times.end();
        let mut start = prefix.clone();
        start.extend_from_slice(&encode_time(start_time));
        let mut end = prefix;
        end.extend_from_slice(&encode_time(end_time));

        self.engine
            .scan(
                ScanBounds::new(Bound::Included(start), Bound::Included(end)),
                usize::MAX,
            )?
            .map(|entry| {
                let (encoded_key, encoded_record) = entry?;
                let expected_key_len = KEY_PREFIX.len()
                    + 1
                    + escaped_len(&self.namespace)
                    + escaped_len(entity_type)
                    + escaped_len(entity_id)
                    + escaped_len(feature_group)
                    + 8;
                if encoded_key.len() != expected_key_len {
                    return Err(Error::Corruption {
                        context: "feature-store key",
                        detail: format!(
                            "expected {expected_key_len} bytes, got {}",
                            encoded_key.len()
                        ),
                    });
                }
                let time_bytes: [u8; 8] = encoded_key[encoded_key.len() - 8..]
                    .try_into()
                    .expect("feature keys always end in an eight-byte time");
                Ok((
                    FeatureKey::new(
                        entity_type,
                        entity_id,
                        feature_group,
                        decode_time(time_bytes),
                    ),
                    decode_record(&encoded_record)?,
                ))
            })
            .collect()
    }

    fn encode_key(&self, key: &FeatureKey) -> Result<Vec<u8>> {
        let mut encoded =
            self.encode_group_prefix(&key.entity_type, &key.entity_id, &key.feature_group)?;
        encoded.extend_from_slice(&encode_time(key.event_time));
        Ok(encoded)
    }

    fn encode_group_prefix(
        &self,
        entity_type: &[u8],
        entity_id: &[u8],
        feature_group: &[u8],
    ) -> Result<Vec<u8>> {
        validate_component("entity_type", entity_type)?;
        validate_component("entity_id", entity_id)?;
        validate_component("feature_group", feature_group)?;
        let mut encoded = Vec::new();
        encoded.extend_from_slice(KEY_PREFIX);
        encoded.push(KEY_SCHEMA_VERSION);
        push_escaped(&mut encoded, &self.namespace);
        push_escaped(&mut encoded, entity_type);
        push_escaped(&mut encoded, entity_id);
        push_escaped(&mut encoded, feature_group);
        Ok(encoded)
    }
}

fn encode_record(record: &FeatureRecord) -> Result<Vec<u8>> {
    validate_record(record)?;
    let body = postcard::to_allocvec(record).map_err(|error| {
        Error::InvalidArgument(format!("could not encode feature row: {error}"))
    })?;
    if body.len() > MAX_FEATURE_VALUE_BYTES {
        return Err(Error::InvalidArgument(format!(
            "encoded feature row is {} bytes, exceeding the {MAX_FEATURE_VALUE_BYTES} limit",
            body.len()
        )));
    }
    let mut encoded = Vec::with_capacity(body.len() + 1);
    encoded.push(RECORD_SCHEMA_VERSION);
    encoded.extend_from_slice(&body);
    Ok(encoded)
}

fn decode_record(bytes: &[u8]) -> Result<FeatureRecord> {
    let Some((&version, body)) = bytes.split_first() else {
        return Err(record_corruption("missing schema version"));
    };
    if version != RECORD_SCHEMA_VERSION {
        return Err(Error::UnsupportedFormat {
            kind: "feature-store record",
            version: u32::from(version),
        });
    }
    if body.len() > MAX_FEATURE_VALUE_BYTES {
        return Err(record_corruption(
            "serialized feature row exceeds size limit",
        ));
    }
    let (record, trailing) = postcard::take_from_bytes::<FeatureRecord>(body)
        .map_err(|error| record_corruption(format!("invalid postcard body: {error}")))?;
    if !trailing.is_empty() {
        return Err(record_corruption("trailing bytes after postcard body"));
    }
    validate_record(&record).map_err(|error| record_corruption(error.to_string()))?;
    let canonical = postcard::to_allocvec(&record)
        .map_err(|error| record_corruption(format!("could not re-encode feature row: {error}")))?;
    if canonical != body {
        return Err(record_corruption("non-canonical postcard body"));
    }
    Ok(record)
}

fn validate_record(record: &FeatureRecord) -> Result<()> {
    if record.features.len() > MAX_FEATURE_COUNT {
        return Err(Error::InvalidArgument(format!(
            "feature count exceeds the {MAX_FEATURE_COUNT} limit"
        )));
    }
    for (name, value) in &record.features {
        validate_feature(name, value)?;
    }
    Ok(())
}

fn validate_feature(name: &[u8], value: &FeatureValue) -> Result<()> {
    if name.is_empty() {
        return Err(Error::InvalidArgument(
            "feature name must not be empty".into(),
        ));
    }
    if name.len() > MAX_FEATURE_NAME_BYTES {
        return Err(Error::InvalidArgument(format!(
            "feature name is {} bytes, exceeding the {MAX_FEATURE_NAME_BYTES} limit",
            name.len()
        )));
    }
    match value {
        FeatureValue::F64(value) if !value.is_finite() => Err(Error::InvalidArgument(
            "feature floats must be finite".into(),
        )),
        FeatureValue::Bytes(value) if value.len() > MAX_FEATURE_VALUE_BYTES => Err(
            Error::InvalidArgument("feature bytes exceed size limit".into()),
        ),
        FeatureValue::String(value) if value.len() > MAX_FEATURE_VALUE_BYTES => Err(
            Error::InvalidArgument("feature string exceeds size limit".into()),
        ),
        _ => Ok(()),
    }
}

fn validate_component(name: &str, bytes: &[u8]) -> Result<()> {
    if bytes.is_empty() {
        return Err(Error::InvalidArgument(format!("{name} must not be empty")));
    }
    if bytes.len() > MAX_COMPONENT_BYTES {
        return Err(Error::InvalidArgument(format!(
            "{name} is {} bytes, exceeding the {MAX_COMPONENT_BYTES} limit",
            bytes.len()
        )));
    }
    Ok(())
}

fn push_escaped(output: &mut Vec<u8>, bytes: &[u8]) {
    for byte in bytes {
        if *byte == 0 {
            output.extend_from_slice(&[0, 0xff]);
        } else {
            output.push(*byte);
        }
    }
    output.extend_from_slice(&[0, 0]);
}

fn escaped_len(bytes: &[u8]) -> usize {
    bytes.len() + bytes.iter().filter(|byte| **byte == 0).count() + 2
}

fn encode_time(time: i64) -> [u8; 8] {
    ((time as u64) ^ (1_u64 << 63)).to_be_bytes()
}

fn decode_time(bytes: [u8; 8]) -> i64 {
    (u64::from_be_bytes(bytes) ^ (1_u64 << 63)) as i64
}

fn record_corruption(detail: impl Into<String>) -> Error {
    Error::Corruption {
        context: "feature-store record",
        detail: detail.into(),
    }
}
