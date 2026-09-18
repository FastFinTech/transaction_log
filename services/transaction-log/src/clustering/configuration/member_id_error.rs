use getset::{CopyGetters, Getters};
use thiserror::Error;

use super::MemberIdErrorKind;

/// A rejected member name with its validation failure reason.
#[derive(Debug, Clone, PartialEq, Eq, Error, Getters, CopyGetters)]
#[error("invalid member name {name:?}: {kind}")]
pub struct MemberIdError {
    /// The original rejected input, including surrounding whitespace.
    #[getset(get = "pub")]
    name: String,
    /// The first failed validation rule.
    #[getset(get_copy = "pub")]
    kind: MemberIdErrorKind,
}

impl MemberIdError {
    pub(super) fn new(name: String, kind: MemberIdErrorKind) -> Self {
        Self { name, kind }
    }
}
