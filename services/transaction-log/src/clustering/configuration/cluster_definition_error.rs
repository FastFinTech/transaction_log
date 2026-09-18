use thiserror::Error;

use super::{Hostname, MINIMUM_REPLICA_COUNT, MemberId};

/// A violation of the complete configured membership contract.
///
/// Validation returns the first failure: count, then replica conflicts in input
/// order (master ID, duplicate ID, then duplicate hostname).
/// Retained identities are parsed member names, not original untrimmed inputs.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ClusterDefinitionError {
    /// Fewer replicas than the fixed operating minimum.
    #[error("at least {MINIMUM_REPLICA_COUNT} replicas are required; configured {count}")]
    TooFewReplicas {
        /// Number of supplied replica entries.
        count: usize,
    },
    /// The permanent master was also listed as a replica.
    #[error("master is also configured as a replica: {member_id:?}")]
    MasterIsReplica {
        /// The conflicting identity.
        member_id: MemberId,
    },
    /// One replica identity was supplied more than once.
    #[error("replica is configured more than once: {member_id:?}")]
    DuplicateReplica {
        /// The repeated identity.
        member_id: MemberId,
    },
    /// Two distinct members use the same parsed hostname.
    #[error("hostname {hostname:?} is shared by members {first_member_id:?} and {member_id:?}")]
    DuplicateHostname {
        /// The trimmed, lowercase hostname shared by the members.
        hostname: Hostname,
        /// The master or earlier replica that first used this hostname.
        first_member_id: MemberId,
        /// The conflicting replica identity.
        member_id: MemberId,
    },
}
