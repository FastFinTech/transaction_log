use super::{ClusterDefinition, ClusterMembership, ClusteringConfigurationError, MemberId};

/// Explicit deployment mode with validated membership for clustered operation.
///
/// This value does not persist the mode or check it against existing storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusteringConfiguration {
    /// Standalone operation without cluster membership.
    Singleton,
    /// Fixed cluster membership, constructible only through validation.
    Clustered(ClusterMembership),
}

impl ClusteringConfiguration {
    /// Assigns a local identity within an already-validated shared definition.
    ///
    /// Requires the local ID to equal the master or one replica ID. Takes ownership
    /// without repeating shared-definition validation or performing I/O.
    pub fn clustered(
        local_member_id: MemberId,
        definition: ClusterDefinition,
    ) -> Result<Self, ClusteringConfigurationError> {
        ClusterMembership::validate(local_member_id, definition).map(Self::Clustered)
    }
}
