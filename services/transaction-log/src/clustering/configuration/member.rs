use getset::Getters;

use super::{Hostname, MemberId};

/// A logical configured member paired with its DNS hostname.
///
/// Equality compares both fields; cluster identity checks must compare [`Self::id`]
/// rather than complete members. This value establishes no network reachability,
/// peer authentication, role or runtime readiness.
#[derive(Debug, Clone, PartialEq, Eq, Getters)]
#[getset(get = "pub")]
pub struct Member {
    /// The member's logical, case-sensitive identity.
    id: MemberId,
    /// The member's validated configured network name, independent of its identity.
    hostname: Hostname,
}

impl Member {
    /// Takes ownership of an already-validated identity and hostname.
    ///
    /// No additional parsing, allocation, DNS lookup or I/O occurs. Relationship
    /// checks between members belong to cluster membership validation.
    pub fn new(id: MemberId, hostname: Hostname) -> Self {
        Self { id, hostname }
    }
}
