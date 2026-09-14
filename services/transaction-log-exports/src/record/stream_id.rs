//! Validated identifiers for the transaction log's logical streams.

use std::fmt;

use super::StreamIdError;

/// A logical stream ID in `0..=4095`, stored as a `u16`.
///
/// Replicas of a stream share the same ID. The private field ensures callers
/// validate new IDs through [`new`](Self::new) or [`TryFrom<u16>`]. Records
/// already validated by [`RecordReader`](crate::RecordReader) expose this type
/// directly, without repeating the range check.
///
/// The ID names a logical virtual shard, not its current host or execution
/// context. Zero is valid. The supported stream count is a domain constraint,
/// distinct from the larger representable `u16` range. Conversion to `u16`
/// preserves the value, and display uses decimal notation.
///
/// ```
/// use transaction_log_exports::StreamId;
///
/// let stream = StreamId::new(42)?;
/// assert_eq!(stream.get(), 42);
/// assert!(StreamId::new(4096).is_err());
/// # Ok::<(), transaction_log_exports::StreamIdError>(())
/// ```
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId(u16);

impl StreamId {
    /// Number of logical streams; replicas do not allocate additional IDs.
    pub const COUNT: usize = 4096;
    /// Smallest supported ID, zero; it is not reserved as an invalid sentinel.
    pub const MIN: Self = Self(0);
    /// Largest supported ID, 4095, rather than the largest representable `u16`.
    pub const MAX: Self = Self((Self::COUNT - 1) as u16);

    /// Validates a raw ID before it enters the application.
    ///
    /// # Errors
    ///
    /// Returns [`StreamIdError`] containing `value` if it exceeds [`Self::MAX`].
    /// This performs no routing, authorization, or stream existence checks.
    pub const fn new(value: u16) -> Result<Self, StreamIdError> {
        if value > Self::MAX.0 {
            return Err(StreamIdError::new(value));
        }
        Ok(Self(value))
    }

    /// Returns the validated integer without checking its range again.
    #[inline]
    pub const fn get(self) -> u16 {
        self.0
    }

    /// Wraps an ID whose supported range has already been established.
    ///
    /// Used by record getters after reader validation, so repeated field access
    /// does not introduce another branch. This is not a public construction path.
    ///
    /// # Safety
    ///
    /// `value` must be no greater than [`Self::MAX`]. The rest of the crate may
    /// rely on this invariant when using stream IDs to address per-stream state.
    #[inline]
    pub(crate) const unsafe fn new_unchecked(value: u16) -> Self {
        Self(value)
    }
}

impl TryFrom<u16> for StreamId {
    type Error = StreamIdError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<StreamId> for u16 {
    #[inline]
    fn from(value: StreamId) -> Self {
        value.get()
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[cfg(test)]
mod tests {
    use super::StreamId;
    use std::mem::{align_of, size_of};

    #[test]
    fn validates_the_entire_u16_range() {
        for value in u16::MIN..=u16::MAX {
            if value < 4096 {
                let id = StreamId::new(value).unwrap();
                assert_eq!(id.get(), value);
                assert_eq!(u16::from(id), value);
                assert_eq!(StreamId::try_from(value), Ok(id));
            } else {
                let error = StreamId::new(value).unwrap_err();
                assert_eq!(error.value(), value);
                assert_eq!(StreamId::try_from(value), Err(error));
            }
        }
    }

    #[test]
    fn has_u16_layout_and_exposes_its_limits() {
        assert_eq!(size_of::<StreamId>(), size_of::<u16>());
        assert_eq!(align_of::<StreamId>(), align_of::<u16>());
        assert_eq!(StreamId::COUNT, 4096);
        assert_eq!(StreamId::MIN.get(), 0);
        assert_eq!(StreamId::MAX.get(), 4095);
        assert_eq!(StreamId::MAX.to_string(), "4095");
        assert_eq!(
            StreamId::new(4096).unwrap_err().to_string(),
            "stream ID 4096 is outside the supported range 0..=4095"
        );
    }
}
