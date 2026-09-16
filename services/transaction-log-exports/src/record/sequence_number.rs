//! Sequence numbers within an individual transaction log stream.

use std::fmt;

use serde::{Deserialize, Serialize};

/// A stream-local sequence number. Every `u64` value is representable.
///
/// This type distinguishes sequence numbers from lengths and other integers;
/// it does not enforce ordering, contiguity, or uniqueness within a stream.
/// The command handler assigns the value; ingestion and replay must check it
/// against their previously known per-stream state. Neither the first expected
/// sequence nor behavior after [`Self::MAX`] is defined by this wrapper.
///
/// [`From<u64>`] and [`Self::new`] provide equivalent infallible construction.
/// Conversion back to `u64` preserves the value, and display uses decimal notation.
/// Serde represents it as an unsigned integer with the full `u64` range; loading
/// metadata does not establish sequence continuity.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SequenceNumber(u64);

impl SequenceNumber {
    /// Smallest representable value, zero; not a prescribed starting sequence.
    pub const MIN: Self = Self(u64::MIN);
    /// Largest representable value; no wraparound or successor policy is implied.
    pub const MAX: Self = Self(u64::MAX);

    /// Wraps a raw value without validation.
    #[inline]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the underlying integer without validation or allocation.
    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl From<u64> for SequenceNumber {
    #[inline]
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

impl From<SequenceNumber> for u64 {
    #[inline]
    fn from(value: SequenceNumber) -> Self {
        value.get()
    }
}

impl fmt::Display for SequenceNumber {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[cfg(test)]
mod tests {
    use super::SequenceNumber;
    use std::mem::{align_of, size_of};

    #[test]
    fn json_preserves_full_u64_precision_and_rejects_non_u64_values() {
        for (json, raw) in [
            ("0", 0),
            ("9007199254740993", 9_007_199_254_740_993),
            ("18446744073709551615", u64::MAX),
        ] {
            let sequence = SequenceNumber::new(raw);
            assert_eq!(
                serde_json::from_str::<SequenceNumber>(json).unwrap(),
                sequence
            );
            assert_eq!(serde_json::to_string(&sequence).unwrap(), json);
        }
        for json in ["18446744073709551616", "-1", "1.5", "null", "\"42\""] {
            assert!(
                serde_json::from_str::<SequenceNumber>(json).is_err(),
                "{json}"
            );
        }
    }

    #[test]
    fn preserves_u64_values_and_layout() {
        assert_eq!(size_of::<SequenceNumber>(), size_of::<u64>());
        assert_eq!(align_of::<SequenceNumber>(), align_of::<u64>());
        for value in [0, 1, u32::MAX as u64 + 1, u64::MAX] {
            let sequence = SequenceNumber::new(value);
            assert_eq!(sequence.get(), value);
            assert_eq!(SequenceNumber::from(value), sequence);
            assert_eq!(u64::from(sequence), value);
            assert_eq!(sequence.to_string(), value.to_string());
        }
        assert_eq!(SequenceNumber::MIN.get(), 0);
        assert_eq!(SequenceNumber::MAX.get(), u64::MAX);
    }
}
