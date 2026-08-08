use crate::{Error, Result};

const MINIMUM_FILTER_BITS: usize = 64;
const MAXIMUM_HASH_FUNCTIONS: u8 = 30;

/// A compact probabilistic set used to avoid unnecessary table reads.
///
/// A Bloom filter can answer “maybe present” or “definitely absent.” Hash
/// collisions can produce false positives, so a positive result still needs a
/// real lookup. It has no false negatives for keys used to build an intact
/// filter: every inserted key sets the same bits that lookup later checks.
///
/// More bits per key reduce false positives at the cost of memory and disk
/// space. More hash probes can improve accuracy up to a point, but each probe
/// also costs CPU. MeteorDB chooses the usual near-optimal probe count from the
/// requested bits per key and caps it to keep lookup work bounded.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BloomFilter {
    encoded: Vec<u8>,
    bit_count: usize,
}

impl BloomFilter {
    /// Builds a deterministic filter for `keys`.
    ///
    /// One stable 64-bit FNV-1a hash is split into two values. Probe `i` uses
    /// `first + i * second`, a technique called double hashing. It approximates
    /// several independent hashes while reading each key only once, saving CPU.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidArgument`] when `bits_per_key` is zero or the
    /// requested allocation overflows the platform's addressable size.
    pub fn from_keys<I, K>(keys: I, bits_per_key: u8) -> Result<Self>
    where
        I: IntoIterator<Item = K>,
        K: AsRef<[u8]>,
    {
        if bits_per_key == 0 {
            return Err(Error::InvalidArgument(
                "Bloom filter bits_per_key must be greater than zero".to_owned(),
            ));
        }

        let keys: Vec<K> = keys.into_iter().collect();
        let requested_bits = keys
            .len()
            .checked_mul(usize::from(bits_per_key))
            .ok_or_else(|| {
                Error::InvalidArgument("Bloom filter bit allocation overflows usize".to_owned())
            })?;
        let bit_count = requested_bits.max(MINIMUM_FILTER_BITS);
        let byte_count = bit_count.checked_add(7).ok_or_else(|| {
            Error::InvalidArgument("Bloom filter byte allocation overflows usize".to_owned())
        })? / 8;
        let bit_count = byte_count.checked_mul(8).ok_or_else(|| {
            Error::InvalidArgument("Bloom filter bit allocation overflows usize".to_owned())
        })?;
        let probes = ((u16::from(bits_per_key) * 69) / 100)
            .clamp(1, u16::from(MAXIMUM_HASH_FUNCTIONS)) as u8;

        let mut encoded = vec![0; byte_count + 1];
        encoded[byte_count] = probes;
        for key in keys {
            set_key_bits(&mut encoded[..byte_count], bit_count, probes, key.as_ref());
        }
        Ok(Self { encoded, bit_count })
    }

    /// Validates and copies a serialized Bloom filter.
    ///
    /// The final byte stores the number of double-hash probes; preceding bytes
    /// are the bit array.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Corruption`] for a missing bit array, an invalid probe
    /// count, or a bit-array length whose bit count overflows `usize`. The
    /// checked conversion matters on 32-bit targets, where a byte slice can be
    /// large enough that multiplying its length by eight would wrap.
    pub fn decode(encoded: impl AsRef<[u8]>) -> Result<Self> {
        let encoded = encoded.as_ref();
        let Some((&probes, bits)) = encoded.split_last() else {
            return Err(bloom_corruption("missing bit array and probe count"));
        };
        let bit_count = checked_bit_count(bits.len())?;
        if !(1..=MAXIMUM_HASH_FUNCTIONS).contains(&probes) {
            return Err(bloom_corruption(format!(
                "probe count {probes} is outside 1..={MAXIMUM_HASH_FUNCTIONS}"
            )));
        }
        Ok(Self {
            encoded: encoded.to_vec(),
            bit_count,
        })
    }

    /// Returns the serialized filter bytes.
    ///
    /// The returned representation is deterministic for the same ordered or
    /// unordered collection of keys because setting a bit is idempotent.
    pub fn as_bytes(&self) -> &[u8] {
        &self.encoded
    }

    /// Reports whether `key` may have been inserted.
    ///
    /// `false` is definitive for an intact filter. `true` may be a false
    /// positive and callers must still search the corresponding data.
    pub fn may_contain(&self, key: impl AsRef<[u8]>) -> bool {
        let (&probes, bits) = self
            .encoded
            .split_last()
            .expect("constructed Bloom filters always have a probe byte");
        key_bits_are_set(bits, self.bit_count, probes, key.as_ref())
    }
}

/// Converts a serialized bit-array byte length to a nonzero bit count.
///
/// Keeping this check separate permits boundary testing without allocating an
/// impractically large slice. The returned count is retained by decoded
/// filters so lookup never repeats an overflowing multiplication or takes a
/// modulo by zero.
fn checked_bit_count(byte_count: usize) -> Result<usize> {
    if byte_count == 0 {
        return Err(bloom_corruption("bit array is empty"));
    }
    byte_count
        .checked_mul(8)
        .ok_or_else(|| bloom_corruption("bit array bit count overflows usize"))
}

fn set_key_bits(bits: &mut [u8], bit_count: usize, probes: u8, key: &[u8]) {
    for bit in probe_bits(key, bit_count, probes) {
        bits[bit / 8] |= 1 << (bit % 8);
    }
}

fn key_bits_are_set(bits: &[u8], bit_count: usize, probes: u8, key: &[u8]) -> bool {
    probe_bits(key, bit_count, probes).all(|bit| bits[bit / 8] & (1 << (bit % 8)) != 0)
}

fn probe_bits(key: &[u8], bit_count: usize, probes: u8) -> impl Iterator<Item = usize> {
    let hash = stable_hash(key);
    let first = hash as u32;
    let second = ((hash >> 32) as u32).rotate_right(17) | 1;
    (0..u32::from(probes))
        .map(move |probe| first.wrapping_add(probe.wrapping_mul(second)) as usize % bit_count)
}

fn stable_hash(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn bloom_corruption(detail: impl Into<String>) -> Error {
    Error::Corruption {
        context: "Bloom filter",
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::checked_bit_count;
    use crate::Error;

    #[test]
    fn checked_bit_count_accepts_the_largest_safe_byte_length() {
        let largest_safe = usize::MAX / 8;

        assert_eq!(checked_bit_count(largest_safe).unwrap(), largest_safe * 8);
    }

    #[test]
    fn checked_bit_count_rejects_zero_and_platform_overflow() {
        for byte_count in [0, usize::MAX / 8 + 1] {
            assert!(matches!(
                checked_bit_count(byte_count),
                Err(Error::Corruption {
                    context: "Bloom filter",
                    ..
                })
            ));
        }
    }
}
