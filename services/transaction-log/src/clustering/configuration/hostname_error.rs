use getset::{CopyGetters, Getters};
use thiserror::Error;

use super::HostnameErrorKind;

/// A rejected configured hostname and its first validation failure.
#[derive(Debug, Clone, PartialEq, Eq, Error, Getters, CopyGetters)]
#[error("invalid hostname {name:?}: {kind}")]
pub struct HostnameError {
    /// The original input, including surrounding whitespace and case.
    #[getset(get = "pub")]
    name: String,
    /// The first failed validation rule.
    #[getset(get_copy = "pub")]
    kind: HostnameErrorKind,
}

impl HostnameError {
    pub(super) fn new(name: String, kind: HostnameErrorKind) -> Self {
        Self { name, kind }
    }
}
