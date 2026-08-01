use std::cmp::Ordering;

use crate::{Error, Result};

const INTERNAL_KEY_TRAILER_BYTES: usize = 9;
const MAX_SEQUENCE_NUMBER: SequenceNumber = u64::MAX - 1;

/// The commit-order number attached to one stored version of a user key.
///
/// Sequence numbers increase as write batches commit. Reads compare this number
/// with their snapshot to decide which historical version is visible.
pub type SequenceNumber = u64;

/// Describes whether an internal record stores a value or a deletion marker.
///
/// Deletions need their own kind because removing a key cannot simply erase an
/// older value that may still be visible to an existing snapshot.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ValueKind {
    /// A tombstone stating that the user key was deleted at this sequence.
    Deletion,
    /// A record containing the user value for this sequence.
    Value,
}

impl ValueKind {
    fn byte(self) -> u8 {
        match self {
            Self::Deletion => 0,
            Self::Value => 1,
        }
    }

    fn from_byte(byte: u8) -> Result<Self> {
        match byte {
            0 => Ok(Self::Deletion),
            1 => Ok(Self::Value),
            _ => Err(Error::Corruption {
                context: "internal key",
                detail: format!("unknown value kind byte {byte}"),
            }),
        }
    }
}

/// An owned storage-engine key containing a user key, sequence, and value kind.
///
/// The encoded bytes are `user_key || big_endian(!sequence) || kind`. Reversing
/// the sequence bits makes newer versions compare before older versions when
/// their encoded bytes are compared. The maximum `u64` sequence is reserved so
/// later engine code has a sentinel above every valid committed sequence.
///
/// [`Ord`] compares user keys first, then sequences newest-first, then kinds.
/// It does not compare the whole encoding directly because a user key may be a
/// prefix of another user key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InternalKey {
    encoded: Vec<u8>,
}

impl InternalKey {
    /// Creates an internal key after validating the sequence number.
    ///
    /// The user key is copied, so the result remains valid if the caller later
    /// changes or drops its input buffer.
    pub fn try_new(
        user_key: impl AsRef<[u8]>,
        sequence: SequenceNumber,
        kind: ValueKind,
    ) -> Result<Self> {
        if sequence > MAX_SEQUENCE_NUMBER {
            return Err(Error::InvalidArgument(
                "sequence number must be less than u64::MAX".to_owned(),
            ));
        }

        let user_key = user_key.as_ref();
        let mut encoded = Vec::with_capacity(user_key.len() + INTERNAL_KEY_TRAILER_BYTES);
        encoded.extend_from_slice(user_key);
        encoded.extend_from_slice(&(!sequence).to_be_bytes());
        encoded.push(kind.byte());
        Ok(Self { encoded })
    }

    /// Creates a value key for a valid committed sequence number.
    ///
    /// This convenience constructor is useful on trusted engine paths. Use
    /// [`InternalKey::try_new`] when a sequence may come from untrusted input.
    ///
    /// # Panics
    ///
    /// Panics when `sequence` is [`u64::MAX`], which is reserved as a sentinel.
    pub fn value(user_key: impl AsRef<[u8]>, sequence: SequenceNumber) -> Self {
        Self::try_new(user_key, sequence, ValueKind::Value)
            .expect("u64::MAX is reserved and cannot be an internal-key sequence")
    }

    /// Creates a deletion-marker key for a valid committed sequence number.
    ///
    /// This convenience constructor is useful on trusted engine paths. Use
    /// [`InternalKey::try_new`] when a sequence may come from untrusted input.
    ///
    /// # Panics
    ///
    /// Panics when `sequence` is [`u64::MAX`], which is reserved as a sentinel.
    pub fn deletion(user_key: impl AsRef<[u8]>, sequence: SequenceNumber) -> Self {
        Self::try_new(user_key, sequence, ValueKind::Deletion)
            .expect("u64::MAX is reserved and cannot be an internal-key sequence")
    }

    /// Validates and copies an encoded internal key.
    ///
    /// Encodings shorter than the nine-byte trailer and encodings with unknown
    /// kind bytes are reported as [`Error::Corruption`]. The reversed sequence
    /// representation accepts every valid sequence except the reserved maximum.
    pub fn decode(encoded: impl AsRef<[u8]>) -> Result<Self> {
        let encoded = encoded.as_ref();
        if encoded.len() < INTERNAL_KEY_TRAILER_BYTES {
            return Err(Error::Corruption {
                context: "internal key",
                detail: "encoded key is shorter than 9 bytes".to_owned(),
            });
        }

        let kind_byte = encoded[encoded.len() - 1];
        ValueKind::from_byte(kind_byte)?;
        let sequence = decode_sequence(encoded);
        if sequence > MAX_SEQUENCE_NUMBER {
            return Err(Error::Corruption {
                context: "internal key",
                detail: "sequence number must be less than u64::MAX".to_owned(),
            });
        }

        Ok(Self {
            encoded: encoded.to_vec(),
        })
    }

    /// Borrows the original user-key portion of this internal key.
    pub fn user_key(&self) -> &[u8] {
        &self.encoded[..self.encoded.len() - INTERNAL_KEY_TRAILER_BYTES]
    }

    /// Returns the committed sequence number stored in this internal key.
    pub fn sequence(&self) -> SequenceNumber {
        decode_sequence(&self.encoded)
    }

    /// Returns whether this key represents a value or a deletion marker.
    pub fn kind(&self) -> ValueKind {
        ValueKind::from_byte(self.encoded[self.encoded.len() - 1])
            .expect("constructed internal keys always contain a known kind")
    }

    /// Borrows the encoded bytes used by tables and in-memory indexes.
    ///
    /// For equal user keys, ordinary byte comparison places newer sequences
    /// first. Use [`Ord`] on `InternalKey` when comparing different user keys.
    pub fn as_bytes(&self) -> &[u8] {
        &self.encoded
    }

    /// Consumes the key and returns its owned encoded bytes without copying.
    pub fn into_bytes(self) -> Vec<u8> {
        self.encoded
    }
}

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.user_key()
            .cmp(other.user_key())
            .then_with(|| other.sequence().cmp(&self.sequence()))
            .then_with(|| self.kind().cmp(&other.kind()))
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn decode_sequence(encoded: &[u8]) -> SequenceNumber {
    let sequence_start = encoded.len() - INTERNAL_KEY_TRAILER_BYTES;
    let reversed = u64::from_be_bytes(
        encoded[sequence_start..sequence_start + 8]
            .try_into()
            .expect("the internal-key trailer always contains eight sequence bytes"),
    );
    !reversed
}
