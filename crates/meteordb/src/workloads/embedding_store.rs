use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{Engine, Error, Result, WriteBatch};

const KEY_PREFIX: &[u8] = b"\0meteordb:embedding-store\0";
const KEY_SCHEMA_VERSION: u8 = 1;
const VALUE_SCHEMA_VERSION: u8 = 1;
const MAX_COMPONENT_BYTES: usize = 16 * 1024;
const MAX_VECTOR_BYTES: usize = 64 * 1024 * 1024;
const MAX_METADATA_ENTRIES: usize = 1_024;
const MAX_METADATA_COMPONENT_BYTES: usize = 1024 * 1024;
const MAX_METADATA_TOTAL_BYTES: usize = 16 * 1024 * 1024;
const MAX_VALUE_BODY_BYTES: usize = MAX_VECTOR_BYTES + MAX_METADATA_TOTAL_BYTES + 64 * 1024;

/// The scalar representation used by an embedding vector.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ScalarType {
    /// IEEE-754 binary32, stored as portable little-endian bytes.
    F32,
    /// IEEE-754 binary16 bit patterns, stored as portable little-endian bytes.
    F16,
}

impl ScalarType {
    fn width(self) -> usize {
        match self {
            Self::F32 => 4,
            Self::F16 => 2,
        }
    }
}

/// The entity and optional model identity addressing one embedding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EmbeddingKey {
    entity_id: Vec<u8>,
    model: Option<Vec<u8>>,
    model_version: Option<Vec<u8>>,
}

impl EmbeddingKey {
    /// Creates a model-independent key for `entity_id`.
    pub fn new(entity_id: impl AsRef<[u8]>) -> Self {
        Self {
            entity_id: entity_id.as_ref().to_vec(),
            model: None,
            model_version: None,
        }
    }

    /// Attaches model and version identity to the key.
    pub fn with_model(mut self, model: impl AsRef<[u8]>, model_version: impl AsRef<[u8]>) -> Self {
        self.model = Some(model.as_ref().to_vec());
        self.model_version = Some(model_version.as_ref().to_vec());
        self
    }

    /// Borrows the entity identifier.
    pub fn entity_id(&self) -> &[u8] {
        &self.entity_id
    }

    /// Borrows the optional model name.
    pub fn model(&self) -> Option<&[u8]> {
        self.model.as_deref()
    }

    /// Borrows the optional model version.
    pub fn model_version(&self) -> Option<&[u8]> {
        self.model_version.as_deref()
    }
}

/// A checked embedding value with portable vector bytes and binary metadata.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Embedding {
    dimension: u32,
    scalar_type: ScalarType,
    vector_bytes: Vec<u8>,
    model: Option<Vec<u8>>,
    model_version: Option<Vec<u8>>,
    metadata: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl Embedding {
    /// Encodes finite binary32 values into portable little-endian bytes.
    pub fn from_f32(values: &[f32]) -> Result<Self> {
        if values.iter().any(|value| !value.is_finite()) {
            return Err(Error::InvalidArgument(
                "embedding floats must be finite".into(),
            ));
        }
        let mut bytes = Vec::with_capacity(values.len().saturating_mul(4));
        for value in values {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        Self::from_bytes(values.len(), ScalarType::F32, bytes)
    }

    /// Encodes finite IEEE-754 binary16 bit patterns as little-endian bytes.
    pub fn from_f16_bits(values: &[u16]) -> Result<Self> {
        if values.iter().any(|bits| !f16_is_finite(*bits)) {
            return Err(Error::InvalidArgument(
                "embedding floats must be finite".into(),
            ));
        }
        let mut bytes = Vec::with_capacity(values.len().saturating_mul(2));
        for value in values {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        Self::from_bytes(values.len(), ScalarType::F16, bytes)
    }

    /// Creates an embedding from checked portable little-endian vector bytes.
    pub fn from_bytes(
        dimension: usize,
        scalar_type: ScalarType,
        vector_bytes: impl Into<Vec<u8>>,
    ) -> Result<Self> {
        let dimension = u32::try_from(dimension)
            .map_err(|_| Error::InvalidArgument("embedding dimension exceeds u32::MAX".into()))?;
        let embedding = Self {
            dimension,
            scalar_type,
            vector_bytes: vector_bytes.into(),
            model: None,
            model_version: None,
            metadata: BTreeMap::new(),
        };
        validate_embedding(&embedding)?;
        Ok(embedding)
    }

    /// Sets the model and version stored with this embedding.
    pub fn set_model(
        &mut self,
        model: impl AsRef<[u8]>,
        model_version: impl AsRef<[u8]>,
    ) -> &mut Self {
        self.model = Some(model.as_ref().to_vec());
        self.model_version = Some(model_version.as_ref().to_vec());
        self
    }

    /// Adds or replaces binary user metadata.
    pub fn insert_metadata(
        &mut self,
        name: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<&mut Self> {
        let name = name.as_ref();
        let value = value.as_ref();
        validate_metadata_component("metadata name", name)?;
        validate_metadata_component("metadata value", value)?;
        if !self.metadata.contains_key(name) && self.metadata.len() >= MAX_METADATA_ENTRIES {
            return Err(Error::InvalidArgument(format!(
                "metadata entry count exceeds the {MAX_METADATA_ENTRIES} limit"
            )));
        }
        let projected_size = projected_metadata_bytes(&self.metadata, name, value)?;
        if projected_size > MAX_METADATA_TOTAL_BYTES {
            return Err(Error::InvalidArgument(format!(
                "embedding metadata is {projected_size} bytes, exceeding the {MAX_METADATA_TOTAL_BYTES} limit"
            )));
        }
        self.metadata.insert(name.to_vec(), value.to_vec());
        Ok(self)
    }

    /// Returns the number of vector elements.
    pub fn dimension(&self) -> usize {
        self.dimension as usize
    }

    /// Returns the scalar representation.
    pub fn scalar_type(&self) -> ScalarType {
        self.scalar_type
    }

    /// Borrows the checked portable little-endian vector bytes without copying.
    pub fn vector_bytes(&self) -> &[u8] {
        &self.vector_bytes
    }

    /// Borrows the optional model name.
    pub fn model(&self) -> Option<&[u8]> {
        self.model.as_deref()
    }

    /// Borrows the optional model version.
    pub fn model_version(&self) -> Option<&[u8]> {
        self.model_version.as_deref()
    }

    /// Borrows binary user metadata in deterministic name order.
    pub fn metadata(&self) -> &BTreeMap<Vec<u8>, Vec<u8>> {
        &self.metadata
    }

    /// Converts an F32 embedding's little-endian bytes into host `f32` values.
    pub fn to_f32(&self) -> Result<Vec<f32>> {
        if self.scalar_type != ScalarType::F32 {
            return Err(Error::InvalidArgument(
                "embedding scalar type is not F32".into(),
            ));
        }
        Ok(self
            .vector_bytes
            .chunks_exact(4)
            .map(|bytes| f32::from_le_bytes(bytes.try_into().expect("four-byte chunk")))
            .collect())
    }

    /// Converts an F16 embedding's little-endian bytes into host bit patterns.
    pub fn to_f16_bits(&self) -> Result<Vec<u16>> {
        if self.scalar_type != ScalarType::F16 {
            return Err(Error::InvalidArgument(
                "embedding scalar type is not F16".into(),
            ));
        }
        Ok(self
            .vector_bytes
            .chunks_exact(2)
            .map(|bytes| u16::from_le_bytes(bytes.try_into().expect("two-byte chunk")))
            .collect())
    }
}

/// A namespace-isolated embedding storage and retrieval adapter.
#[derive(Clone)]
pub struct EmbeddingStore {
    engine: Engine,
    namespace: Vec<u8>,
}

impl EmbeddingStore {
    /// Creates an embedding store with an isolated binary namespace.
    pub fn new(engine: Engine, namespace: impl AsRef<[u8]>) -> Result<Self> {
        validate_component("namespace", namespace.as_ref())?;
        Ok(Self {
            engine,
            namespace: namespace.as_ref().to_vec(),
        })
    }

    /// Atomically stores one embedding, optionally expiring it after `ttl_ms`.
    pub fn put(
        &self,
        key: &EmbeddingKey,
        embedding: &Embedding,
        ttl_ms: Option<i64>,
    ) -> Result<()> {
        validate_key_matches_embedding(key, embedding)?;
        let key = self.encode_key(key)?;
        let value = encode_embedding(embedding)?;
        match ttl_ms {
            Some(ttl_ms) => self.engine.put_with_ttl(key, value, ttl_ms),
            None => self.engine.put(key, value),
        }
    }

    /// Atomically stores a batch after validating every key and value.
    pub fn put_many<'a>(
        &self,
        entries: impl IntoIterator<Item = (&'a EmbeddingKey, &'a Embedding)>,
    ) -> Result<()> {
        let mut batch = WriteBatch::default();
        let limits = self.engine.write_batch_limits();
        for (key, embedding) in entries {
            validate_key_matches_embedding(key, embedding)?;
            let key = self.encode_key(key)?;
            let value = encode_embedding(embedding)?;
            limits.validate_next_put(&batch, &key, &value)?;
            batch.put_owned(key, value);
        }
        if batch.is_empty() {
            return Ok(());
        }
        self.engine.write(batch)
    }

    /// Reads one embedding by complete namespace, entity, and model identity.
    pub fn get(&self, key: &EmbeddingKey) -> Result<Option<Embedding>> {
        self.engine
            .get(self.encode_key(key)?)?
            .map(|bytes| decode_embedding(&bytes))
            .transpose()
    }

    /// Reads embeddings through one MVCC snapshot and preserves input order.
    pub fn get_many<'a>(
        &self,
        keys: impl IntoIterator<Item = &'a EmbeddingKey>,
    ) -> Result<Vec<Option<Embedding>>> {
        let keys = keys
            .into_iter()
            .map(|key| self.encode_key(key))
            .collect::<Result<Vec<_>>>()?;
        let snapshot = self.engine.snapshot()?;
        keys.iter()
            .map(|key| {
                snapshot
                    .get(key)?
                    .map(|bytes| decode_embedding(&bytes))
                    .transpose()
            })
            .collect()
    }

    /// Deletes one exact embedding identity.
    pub fn delete(&self, key: &EmbeddingKey) -> Result<()> {
        self.engine.delete(self.encode_key(key)?)
    }

    fn encode_key(&self, key: &EmbeddingKey) -> Result<Vec<u8>> {
        validate_component("entity_id", &key.entity_id)?;
        validate_optional_identity(&key.model, &key.model_version)?;
        let mut encoded = Vec::new();
        encoded.extend_from_slice(KEY_PREFIX);
        encoded.push(KEY_SCHEMA_VERSION);
        push_escaped(&mut encoded, &self.namespace);
        push_optional(&mut encoded, key.model.as_deref());
        push_optional(&mut encoded, key.model_version.as_deref());
        push_escaped(&mut encoded, &key.entity_id);
        Ok(encoded)
    }
}

fn encode_embedding(embedding: &Embedding) -> Result<Vec<u8>> {
    validate_embedding(embedding)?;
    let body = postcard::to_allocvec(embedding)
        .map_err(|error| Error::InvalidArgument(format!("could not encode embedding: {error}")))?;
    let mut encoded = Vec::with_capacity(body.len() + 1);
    encoded.push(VALUE_SCHEMA_VERSION);
    encoded.extend_from_slice(&body);
    Ok(encoded)
}

fn decode_embedding(bytes: &[u8]) -> Result<Embedding> {
    let Some((&version, body)) = bytes.split_first() else {
        return Err(value_corruption("missing schema version"));
    };
    if version != VALUE_SCHEMA_VERSION {
        return Err(Error::UnsupportedFormat {
            kind: "embedding-store value",
            version: u32::from(version),
        });
    }
    if body.len() > MAX_VALUE_BODY_BYTES {
        return Err(value_corruption("serialized embedding exceeds size limit"));
    }
    let (embedding, trailing) = postcard::take_from_bytes::<Embedding>(body)
        .map_err(|error| value_corruption(format!("invalid postcard body: {error}")))?;
    if !trailing.is_empty() {
        return Err(value_corruption("trailing bytes after postcard body"));
    }
    validate_embedding(&embedding).map_err(|error| value_corruption(error.to_string()))?;
    let canonical = postcard::to_allocvec(&embedding)
        .map_err(|error| value_corruption(format!("could not re-encode embedding: {error}")))?;
    if canonical != body {
        return Err(value_corruption("non-canonical postcard body"));
    }
    Ok(embedding)
}

fn validate_key_matches_embedding(key: &EmbeddingKey, embedding: &Embedding) -> Result<()> {
    if key.model != embedding.model || key.model_version != embedding.model_version {
        return Err(Error::InvalidArgument(
            "embedding model identity must match its key".into(),
        ));
    }
    Ok(())
}

fn validate_embedding(embedding: &Embedding) -> Result<()> {
    if embedding.dimension == 0 {
        return Err(Error::InvalidArgument(
            "embedding dimension must be greater than zero".into(),
        ));
    }
    let expected = (embedding.dimension as usize)
        .checked_mul(embedding.scalar_type.width())
        .ok_or_else(|| Error::InvalidArgument("embedding byte length overflow".into()))?;
    if embedding.vector_bytes.len() != expected {
        return Err(Error::InvalidArgument(format!(
            "{}-dimensional {:?} embedding requires {expected} vector bytes, got {}",
            embedding.dimension,
            embedding.scalar_type,
            embedding.vector_bytes.len()
        )));
    }
    if embedding.vector_bytes.len() > MAX_VECTOR_BYTES {
        return Err(Error::InvalidArgument(format!(
            "embedding vector exceeds the {MAX_VECTOR_BYTES} byte limit"
        )));
    }
    match embedding.scalar_type {
        ScalarType::F32 => {
            if embedding.vector_bytes.chunks_exact(4).any(|bytes| {
                !f32::from_le_bytes(bytes.try_into().expect("four-byte chunk")).is_finite()
            }) {
                return Err(Error::InvalidArgument(
                    "embedding floats must be finite".into(),
                ));
            }
        }
        ScalarType::F16 => {
            if embedding.vector_bytes.chunks_exact(2).any(|bytes| {
                !f16_is_finite(u16::from_le_bytes(
                    bytes.try_into().expect("two-byte chunk"),
                ))
            }) {
                return Err(Error::InvalidArgument(
                    "embedding floats must be finite".into(),
                ));
            }
        }
    }
    validate_optional_identity(&embedding.model, &embedding.model_version)?;
    if embedding.metadata.len() > MAX_METADATA_ENTRIES {
        return Err(Error::InvalidArgument(
            "embedding metadata has too many entries".into(),
        ));
    }
    for (name, value) in &embedding.metadata {
        validate_metadata_component("metadata name", name)?;
        validate_metadata_component("metadata value", value)?;
    }
    let metadata_bytes = metadata_bytes(&embedding.metadata)?;
    if metadata_bytes > MAX_METADATA_TOTAL_BYTES {
        return Err(Error::InvalidArgument(format!(
            "embedding metadata is {metadata_bytes} bytes, exceeding the {MAX_METADATA_TOTAL_BYTES} limit"
        )));
    }
    Ok(())
}

fn projected_metadata_bytes(
    metadata: &BTreeMap<Vec<u8>, Vec<u8>>,
    name: &[u8],
    value: &[u8],
) -> Result<usize> {
    let current = metadata_bytes(metadata)?;
    let without_replaced = match metadata.get(name) {
        Some(previous) => current
            .checked_sub(name.len())
            .and_then(|size| size.checked_sub(previous.len()))
            .ok_or_else(|| Error::InvalidArgument("embedding metadata size overflow".into()))?,
        None => current,
    };
    without_replaced
        .checked_add(name.len())
        .and_then(|size| size.checked_add(value.len()))
        .ok_or_else(|| Error::InvalidArgument("embedding metadata size overflow".into()))
}

fn metadata_bytes(metadata: &BTreeMap<Vec<u8>, Vec<u8>>) -> Result<usize> {
    metadata.iter().try_fold(0usize, |size, (name, value)| {
        size.checked_add(name.len())
            .and_then(|size| size.checked_add(value.len()))
            .ok_or_else(|| Error::InvalidArgument("embedding metadata size overflow".into()))
    })
}

fn validate_optional_identity(
    model: &Option<Vec<u8>>,
    model_version: &Option<Vec<u8>>,
) -> Result<()> {
    match (model, model_version) {
        (None, None) => Ok(()),
        (Some(model), Some(version)) => {
            validate_component("model", model)?;
            validate_component("model_version", version)
        }
        _ => Err(Error::InvalidArgument(
            "model and model_version must both be present or absent".into(),
        )),
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

fn validate_metadata_component(name: &str, bytes: &[u8]) -> Result<()> {
    if bytes.is_empty() {
        return Err(Error::InvalidArgument(format!("{name} must not be empty")));
    }
    if bytes.len() > MAX_METADATA_COMPONENT_BYTES {
        return Err(Error::InvalidArgument(format!(
            "{name} exceeds the {MAX_METADATA_COMPONENT_BYTES} byte limit"
        )));
    }
    Ok(())
}

fn push_optional(output: &mut Vec<u8>, bytes: Option<&[u8]>) {
    match bytes {
        Some(bytes) => {
            output.push(1);
            push_escaped(output, bytes);
        }
        None => output.push(0),
    }
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

fn f16_is_finite(bits: u16) -> bool {
    bits & 0x7c00 != 0x7c00
}

fn value_corruption(detail: impl Into<String>) -> Error {
    Error::Corruption {
        context: "embedding-store value",
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;
    use crate::Options;

    #[test]
    fn put_many_stops_before_retaining_the_first_operation_over_the_batch_limit() {
        let dir = tempfile::tempdir().unwrap();
        let key = EmbeddingKey::new(b"same-key");
        let original = Embedding::from_f32(&[1.0]).unwrap();
        let replacement = Embedding::from_f32(&[2.0]).unwrap();

        let initial = Engine::open(Options::new(dir.path())).unwrap();
        let initial_store = EmbeddingStore::new(initial, b"tenant").unwrap();
        initial_store.put(&key, &original, None).unwrap();
        let operation_bytes = initial_store.encode_key(&key).unwrap().len()
            + encode_embedding(&replacement).unwrap().len();
        drop(initial_store);

        let mut options = Options::new(dir.path());
        options.max_batch_bytes = operation_bytes;
        let store = EmbeddingStore::new(Engine::open(options).unwrap(), b"tenant").unwrap();
        let consumed = Cell::new(0);
        let entries = std::iter::repeat((&key, &replacement))
            .inspect(|_| consumed.set(consumed.get() + 1))
            .take(10_000);

        assert!(matches!(
            store.put_many(entries),
            Err(Error::InvalidArgument(message)) if message.contains("max_batch_bytes")
        ));
        assert_eq!(consumed.get(), 2);
        assert_eq!(store.get(&key).unwrap(), Some(original));
    }
}
