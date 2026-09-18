use thiserror::Error;

use super::MemberId;

/// A local configuration that does not belong to its validated shared definition.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ClusteringConfigurationError {
    /// This process's identity is absent from the master/replica set.
    #[error("local member is not in the configured cluster: {member_id:?}")]
    UnknownLocalMember {
        /// The unmatched parsed identity.
        member_id: MemberId,
    },
}
