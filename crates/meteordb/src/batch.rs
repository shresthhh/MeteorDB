#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WriteOp {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        expires_at_unix_ms: Option<u64>,
    },
    Delete {
        key: Vec<u8>,
    },
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WriteBatch {
    operations: Vec<WriteOp>,
    approximate_bytes: usize,
}

impl WriteBatch {
    pub fn put(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> &mut Self {
        self.put_with_expiration(key, value, None)
    }

    pub fn put_with_expiration(
        &mut self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        expires_at_unix_ms: Option<u64>,
    ) -> &mut Self {
        let key = key.as_ref();
        let value = value.as_ref();
        self.add_bytes(key.len());
        self.add_bytes(value.len());
        self.operations.push(WriteOp::Put {
            key: key.to_vec(),
            value: value.to_vec(),
            expires_at_unix_ms,
        });
        self
    }

    pub fn delete(&mut self, key: impl AsRef<[u8]>) -> &mut Self {
        let key = key.as_ref();
        self.add_bytes(key.len());
        self.operations.push(WriteOp::Delete { key: key.to_vec() });
        self
    }

    pub fn len(&self) -> usize {
        self.operations.len()
    }

    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    pub fn approximate_bytes(&self) -> usize {
        self.approximate_bytes
    }

    pub fn operations(&self) -> &[WriteOp] {
        &self.operations
    }

    pub fn into_operations(self) -> Vec<WriteOp> {
        self.operations
    }

    fn add_bytes(&mut self, bytes: usize) {
        self.approximate_bytes = self
            .approximate_bytes
            .checked_add(bytes)
            .expect("write batch byte count overflow");
    }
}
