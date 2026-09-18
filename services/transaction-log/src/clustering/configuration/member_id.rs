use getset::Getters;

use super::{MemberIdError, MemberIdErrorKind};

/// An opaque, case-sensitive configured member name, independent of its address.
///
/// Created by [`Self::parse`], which stores a trimmed, validated name. Equality
/// and hashing use that stored name. Name validity alone does not establish
/// cluster membership or authenticate a peer.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Getters)]
pub struct MemberId {
    /// The trimmed, validated name, preserving case.
    #[getset(get = "pub")]
    name: String,
}

impl MemberId {
    /// Minimum member-name length after trimming, in ASCII characters/bytes.
    pub const MIN_NAME_LENGTH: usize = 3;

    /// Maximum member-name length in bytes (also characters for valid ASCII names).
    pub const MAX_NAME_LENGTH: usize = 64;

    /// Trims surrounding whitespace and owns a name of 3–64 ASCII characters.
    ///
    /// Allowed characters are ASCII letters, digits, `-` and `_`. The first and
    /// last characters must be letters or digits. [`str::trim`] removes surrounding
    /// whitespace, including Unicode whitespace. Interior whitespace is rejected.
    /// Case is preserved and significant. Names are logical identities, not DNS
    /// names or storage paths. The untrimmed allocation is reused if no trimming
    /// is needed; otherwise the validated trimmed name is copied into a new string.
    ///
    /// # Errors
    ///
    /// Validation checks trimmed byte length, allowed characters, then
    /// boundary characters, returning the first failure with the original name.
    /// Empty or whitespace-only input returns [`MemberIdErrorKind::TooShort`].
    /// Invalid-character offsets are relative to the trimmed name, while
    /// [`MemberIdError::name`] retains the original, untrimmed input.
    pub fn parse(name: String) -> Result<Self, MemberIdError> {
        use MemberIdErrorKind::*;
        let trimmed = name.trim();
        if trimmed.len() < Self::MIN_NAME_LENGTH {
            return Err(MemberIdError::new(name, TooShort));
        }
        if trimmed.len() > Self::MAX_NAME_LENGTH {
            return Err(MemberIdError::new(name, TooLong));
        }
        if let Some(byte_index) = trimmed
            .bytes()
            .position(|byte| !byte.is_ascii_alphanumeric() && byte != b'-' && byte != b'_')
        {
            return Err(MemberIdError::new(name, InvalidCharacter { byte_index }));
        }
        let bytes = trimmed.as_bytes();
        // Nonempty ASCII input is established above; both indexes are in bounds.
        if !bytes[0].is_ascii_alphanumeric() || !bytes[bytes.len() - 1].is_ascii_alphanumeric() {
            return Err(MemberIdError::new(name, InvalidBoundary));
        }
        // Reuse the owned input when no trimming is necessary. Configuration
        // construction is outside the record hot path.
        let name = if trimmed.len() == name.len() {
            name
        } else {
            trimmed.to_owned()
        };
        Ok(Self { name })
    }
}

#[cfg(test)]
mod tests {
    use super::{MemberId, MemberIdErrorKind};

    #[test]
    fn accepts_valid_names_without_normalizing() {
        for name in [
            "abc",
            "012",
            "XYZ",
            "master",
            "replica-01",
            "Replica_A",
            "a--__b",
        ] {
            assert_eq!(MemberId::parse(name.into()).unwrap().name(), name);
        }
        assert_ne!(MemberId::parse("ABC".into()), MemberId::parse("abc".into()));
    }

    #[test]
    fn validates_length_boundaries_and_retains_rejected_input() {
        for name in ["", "a", "ab", " \tab\n", "\u{2003}  "] {
            let error = MemberId::parse(name.into()).unwrap_err();
            assert_eq!(error.kind(), MemberIdErrorKind::TooShort);
            assert_eq!(error.name(), name);
        }
        assert!(MemberId::parse("abc".into()).is_ok());
        assert!(MemberId::parse("a".repeat(64)).is_ok());
        let name = "a".repeat(65);
        let error = MemberId::parse(name.clone()).unwrap_err();
        assert_eq!(error.kind(), MemberIdErrorKind::TooLong);
        assert_eq!(error.name(), &name);
        // Length is checked in bytes before character validation.
        assert_eq!(
            MemberId::parse("é".repeat(33)).unwrap_err().kind(),
            MemberIdErrorKind::TooLong
        );
    }

    #[test]
    fn rejects_every_disallowed_ascii_byte() {
        for byte in 0u8..=127 {
            let name = format!("a{}b", char::from(byte));
            let result = MemberId::parse(name.clone());
            if byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_' {
                assert!(result.is_ok(), "{name:?}");
            } else {
                let error = result.unwrap_err();
                assert_eq!(error.name(), &name);
                assert_eq!(
                    error.kind(),
                    MemberIdErrorKind::InvalidCharacter { byte_index: 1 }
                );
            }
        }
    }

    #[test]
    fn rejects_non_ascii_and_reports_byte_offsets() {
        for (name, byte_index) in [("東京", 0), ("aé", 1), ("ab\u{2003}c", 2), ("replicа", 6)] {
            let error = MemberId::parse(name.into()).unwrap_err();
            assert_eq!(error.name(), name);
            assert_eq!(
                error.kind(),
                MemberIdErrorKind::InvalidCharacter { byte_index }
            );
        }
    }

    #[test]
    fn requires_alphanumeric_boundaries() {
        for name in ["---", "___", "-ab", "_ab", "ab-", "ab_", "-a_"] {
            let error = MemberId::parse(name.into()).unwrap_err();
            assert_eq!(error.name(), name);
            assert_eq!(error.kind(), MemberIdErrorKind::InvalidBoundary);
        }
    }

    #[test]
    fn stores_trimmed_names_and_checks_trimmed_length() {
        assert_eq!(
            MemberId::parse(" \tReplica_A\n\u{2003}".into())
                .unwrap()
                .name(),
            "Replica_A"
        );
        assert!(MemberId::parse(format!("  {}  ", "a".repeat(64))).is_ok());
        assert_eq!(
            MemberId::parse(" master ".into()),
            MemberId::parse("master".into())
        );
        let error = MemberId::parse("  ab c  ".into()).unwrap_err();
        assert_eq!(error.name(), "  ab c  ");
        assert_eq!(
            error.kind(),
            MemberIdErrorKind::InvalidCharacter { byte_index: 2 }
        );
    }
}
