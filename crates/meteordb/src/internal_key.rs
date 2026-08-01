use std::cmp::Ordering;

use crate::{Error, Result};

const INTERNAL_KEY_TRAILER_BYTES: usize = 9;
const USER_KEY_TERMINATOR_BYTES: usize = 2;
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
/// The user key is escaped before the trailer: nonzero bytes are copied,
/// `0x00` becomes `0x00 0xff`, and `0x00 0x00` terminates the key. The trailer
/// is `big_endian(!sequence) || kind`. A terminator is required because simply
/// appending a trailer to a variable-length key lets trailer bytes affect the
/// ordering of prefix keys such as `a` and `aa`. Escaping reserves the
/// terminator while preserving ordinary byte order, including keys containing
/// zero bytes.
///
/// Every encoding costs eleven fixed bytes: the two-byte terminator and the
/// nine-byte trailer. Each zero byte in the user key costs one additional byte.
/// In return, ordinary encoded-byte comparison exactly implements [`Ord`]:
/// user keys ascend, sequences descend, and kinds break remaining ties. The
/// maximum `u64` sequence is reserved for future sentinel or seek-bound use.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InternalKey {
    encoded: Vec<u8>,
    user_key: Vec<u8>,
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

        let user_key = user_key.as_ref().to_vec();
        let mut encoded = Vec::with_capacity(
            user_key.len() + USER_KEY_TERMINATOR_BYTES + INTERNAL_KEY_TRAILER_BYTES,
        );
        encode_user_key(&user_key, &mut encoded);
        encoded.extend_from_slice(&(!sequence).to_be_bytes());
        encoded.push(kind.byte());
        Ok(Self { encoded, user_key })
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
    /// Validation rejects a missing user-key terminator, malformed zero-byte
    /// escapes, a trailer of any length other than nine bytes, unknown kind
    /// bytes, the reserved maximum sequence, and bytes trailing the trailer.
    pub fn decode(encoded: impl AsRef<[u8]>) -> Result<Self> {
        let encoded = encoded.as_ref();
        let (user_key, trailer_start) = decode_user_key(encoded)?;
        let trailer_len = encoded.len() - trailer_start;
        if trailer_len < INTERNAL_KEY_TRAILER_BYTES {
            return Err(Error::Corruption {
                context: "internal key",
                detail: format!("truncated trailer: expected 9 bytes, found {trailer_len}"),
            });
        }
        if trailer_len > INTERNAL_KEY_TRAILER_BYTES {
            return Err(Error::Corruption {
                context: "internal key",
                detail: format!(
                    "trailing bytes after trailer: expected 9 bytes, found {trailer_len}"
                ),
            });
        }

        let kind_byte = encoded[encoded.len() - 1];
        ValueKind::from_byte(kind_byte)?;
        let sequence = decode_sequence(&encoded[trailer_start..trailer_start + 8]);
        if sequence > MAX_SEQUENCE_NUMBER {
            return Err(Error::Corruption {
                context: "internal key",
                detail: "sequence number must be less than u64::MAX".to_owned(),
            });
        }

        Ok(Self {
            encoded: encoded.to_vec(),
            user_key,
        })
    }

    /// Borrows the decoded user key, with escaped zero bytes restored.
    pub fn user_key(&self) -> &[u8] {
        &self.user_key
    }

    /// Returns the committed sequence number stored in this internal key.
    pub fn sequence(&self) -> SequenceNumber {
        let sequence_start = self.encoded.len() - INTERNAL_KEY_TRAILER_BYTES;
        decode_sequence(&self.encoded[sequence_start..sequence_start + 8])
    }

    /// Returns whether this key represents a value or a deletion marker.
    pub fn kind(&self) -> ValueKind {
        ValueKind::from_byte(self.encoded[self.encoded.len() - 1])
            .expect("constructed internal keys always contain a known kind")
    }

    /// Borrows the encoded bytes used by tables and in-memory indexes.
    ///
    /// Ordinary byte comparison exactly matches [`InternalKey::cmp`], including
    /// for prefix keys and user keys containing zero bytes.
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
        self.encoded.cmp(&other.encoded)
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn encode_user_key(user_key: &[u8], encoded: &mut Vec<u8>) {
    for &byte in user_key {
        if byte == 0 {
            encoded.extend_from_slice(&[0x00, 0xff]);
        } else {
            encoded.push(byte);
        }
    }
    encoded.extend_from_slice(&[0x00, 0x00]);
}

fn decode_user_key(encoded: &[u8]) -> Result<(Vec<u8>, usize)> {
    let mut user_key = Vec::new();
    let mut index = 0;

    while index < encoded.len() {
        let byte = encoded[index];
        if byte != 0 {
            user_key.push(byte);
            index += 1;
            continue;
        }

        let Some(&follower) = encoded.get(index + 1) else {
            return Err(Error::Corruption {
                context: "internal key",
                detail: "zero byte at end of encoding has no escape follower".to_owned(),
            });
        };
        match follower {
            0x00 => return Ok((user_key, index + USER_KEY_TERMINATOR_BYTES)),
            0xff => {
                user_key.push(0);
                index += 2;
            }
            _ => {
                return Err(Error::Corruption {
                    context: "internal key",
                    detail: format!("invalid zero-byte escape follower {follower:#04x}"),
                });
            }
        }
    }

    Err(Error::Corruption {
        context: "internal key",
        detail: "missing user-key terminator".to_owned(),
    })
}

fn decode_sequence(encoded: &[u8]) -> SequenceNumber {
    let reversed = u64::from_be_bytes(
        encoded
            .try_into()
            .expect("validated internal keys always contain eight sequence bytes"),
    );
    !reversed
}
