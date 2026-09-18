use getset::Getters;
use serde::{Deserialize, Serialize};

use super::{HostnameError, HostnameErrorKind};

/// A validated configured DNS hostname, independent of member identity or port.
///
/// Stored in lowercase with surrounding whitespace removed. A final root dot
/// is preserved, so relative and explicitly absolute names remain distinct.
/// Valid syntax does not establish DNS existence, reachability or peer identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Getters, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Hostname {
    /// The trimmed, lowercase hostname, including a final root dot if supplied.
    #[getset(get = "pub")]
    name: String,
}

impl TryFrom<String> for Hostname {
    type Error = HostnameError;

    fn try_from(name: String) -> Result<Self, Self::Error> {
        Self::parse(name)
    }
}

impl From<Hostname> for String {
    fn from(hostname: Hostname) -> Self {
        hostname.name
    }
}

impl Hostname {
    /// Maximum length excluding an optional final root dot, in ASCII bytes.
    pub const MAX_NAME_LENGTH: usize = 253;

    /// Maximum length of an individual label, in ASCII bytes.
    pub const MAX_LABEL_LENGTH: usize = 63;

    /// Parses an owned hostname without resolving it or opening a connection.
    ///
    /// Trims using [`str::trim`], validates ASCII letters, digits and hyphens in
    /// nonempty dot-separated labels, then stores lowercase. Each label must
    /// start and end with a letter or digit. Single-label names are allowed;
    /// underscores, Unicode/IDNA conversion, URLs, ports and IP literals are not.
    /// One optional final dot is allowed and retained for resolver semantics.
    ///
    /// # Errors
    ///
    /// Checks empty input, overall length, IP literal, then each label in order:
    /// empty label, label length, characters and boundaries. Returns the first
    /// failure, retaining the original untrimmed input. Label indexes are
    /// zero-based; invalid-character byte offsets are relative to the trimmed
    /// input before lowercasing.
    pub fn parse(name: String) -> Result<Self, HostnameError> {
        use HostnameErrorKind::*;
        let trimmed = name.trim();
        if trimmed.is_empty() {
            return Err(HostnameError::new(name, Empty));
        }
        let labels = trimmed.strip_suffix('.').unwrap_or(trimmed);
        if labels.len() > Self::MAX_NAME_LENGTH {
            return Err(HostnameError::new(name, TooLong));
        }
        if labels.parse::<std::net::IpAddr>().is_ok() {
            return Err(HostnameError::new(name, IpLiteral));
        }
        let mut label_start = 0;
        for (label_index, label) in labels.split('.').enumerate() {
            if label.is_empty() {
                return Err(HostnameError::new(name, EmptyLabel { label_index }));
            }
            if label.len() > Self::MAX_LABEL_LENGTH {
                return Err(HostnameError::new(name, LabelTooLong { label_index }));
            }
            if let Some(offset) = label
                .bytes()
                .position(|byte| !byte.is_ascii_alphanumeric() && byte != b'-')
            {
                return Err(HostnameError::new(
                    name,
                    InvalidCharacter {
                        byte_index: label_start + offset,
                    },
                ));
            }
            let bytes = label.as_bytes();
            // Nonempty ASCII label established above; both indexes are in bounds.
            if !bytes[0].is_ascii_alphanumeric() || !bytes[bytes.len() - 1].is_ascii_alphanumeric()
            {
                return Err(HostnameError::new(name, InvalidBoundary { label_index }));
            }
            label_start += label.len() + 1;
        }
        let mut name = if trimmed.len() == name.len() {
            name
        } else {
            trimmed.to_owned()
        };
        name.make_ascii_lowercase();
        Ok(Self { name })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use HostnameErrorKind::*;

    fn rejects(input: &str, kind: HostnameErrorKind) {
        let error = Hostname::parse(input.into()).unwrap_err();
        assert_eq!(error.name(), input);
        assert_eq!(error.kind(), kind);
    }

    #[test]
    fn accepts_names_and_normalizes_case_and_whitespace() {
        for name in [
            "a",
            "0",
            "localhost",
            "9master",
            "master-0.db.svc.cluster.local",
            "xn--bcher-kva.example",
            "a--b.example.",
        ] {
            assert_eq!(Hostname::parse(name.into()).unwrap().name(), name);
        }
        assert_eq!(
            Hostname::parse(" \tMASTER.DB\n\u{2003}".into())
                .unwrap()
                .name(),
            "master.db"
        );
        assert_eq!(
            Hostname::parse("MASTER.db".into()),
            Hostname::parse("master.DB".into())
        );
        assert_ne!(
            Hostname::parse("master.db".into()),
            Hostname::parse("master.db.".into())
        );
    }

    #[test]
    fn validates_label_and_total_length_boundaries() {
        assert!(Hostname::parse("a".repeat(63)).is_ok());
        rejects(&"a".repeat(64), LabelTooLong { label_index: 0 });
        let maximum = format!(
            "{}.{}.{}.{}",
            "a".repeat(63),
            "b".repeat(63),
            "c".repeat(63),
            "d".repeat(61)
        );
        assert_eq!(maximum.len(), 253);
        assert!(Hostname::parse(maximum.clone()).is_ok());
        assert!(Hostname::parse(format!("  {maximum}.  ")).is_ok());
        rejects(&format!("{maximum}d"), TooLong);
        rejects(&format!("{maximum}d."), TooLong);
        rejects(
            &format!("ok.{}", "a".repeat(64)),
            LabelTooLong { label_index: 1 },
        );
    }

    #[test]
    fn rejects_empty_names_labels_and_invalid_boundaries() {
        for input in ["", " \t\u{2003}"] {
            rejects(input, Empty);
        }
        for (input, label_index) in [(".", 0), (".master", 0), ("master..db", 1), ("master..", 1)] {
            rejects(input, EmptyLabel { label_index });
        }
        for (input, label_index) in [("-a", 0), ("a-", 0), ("db.-master", 1), ("db.master-.", 1)] {
            rejects(input, InvalidBoundary { label_index });
        }
    }

    #[test]
    fn rejects_disallowed_ascii_and_unicode_with_trimmed_offsets() {
        for byte in 0u8..=127 {
            if byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'.' {
                continue;
            }
            rejects(
                &format!("a{}b", char::from(byte)),
                InvalidCharacter { byte_index: 1 },
            );
        }
        for (input, byte_index) in [
            ("  db.ab_c  ", 5),
            ("db.東京", 3),
            ("db.maѕter", 5),
            ("https://db", 5),
            ("db:1234", 2),
            ("db/path", 2),
            ("*.db", 0),
            ("[::1]", 0),
        ] {
            rejects(input, InvalidCharacter { byte_index });
        }
    }

    #[test]
    fn rejects_ip_literals_and_reports_first_failure() {
        for input in ["127.0.0.1", " 127.0.0.1. ", "::1", "2001:db8::1"] {
            rejects(input, IpLiteral);
        }
        rejects(
            &format!("{}.bad_", "a".repeat(64)),
            LabelTooLong { label_index: 0 },
        );
        rejects("bad_.-host", InvalidCharacter { byte_index: 3 });
    }
}
