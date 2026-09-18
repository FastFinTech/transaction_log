use thiserror::Error;

use super::{Hostname, MemberId};

/// The first difference between an expected and another validated shared definition.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ClusterDefinitionMismatch {
    /// Different permanent masters were configured.
    #[error("master ID differs: expected {expected:?}, actual {actual:?}")]
    Master {
        /// Expected master ID.
        expected: MemberId,
        /// Actual master ID.
        actual: MemberId,
    },
    /// A master or replica ID has a different configured hostname.
    #[error("hostname for {member_id:?} differs: expected {expected:?}, actual {actual:?}")]
    Hostname {
        /// Identity whose location differs.
        member_id: MemberId,
        /// Expected hostname.
        expected: Hostname,
        /// Actual hostname.
        actual: Hostname,
    },
    /// An expected replica ID is absent from the other definition.
    #[error("expected replica is missing: {member_id:?}")]
    MissingReplica {
        /// The absent identity.
        member_id: MemberId,
    },
    /// The other definition includes an additional replica ID.
    #[error("unexpected replica: {member_id:?}")]
    UnexpectedReplica {
        /// The additional identity.
        member_id: MemberId,
    },
}
