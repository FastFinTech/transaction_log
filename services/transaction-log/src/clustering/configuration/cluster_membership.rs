use getset::Getters;

use super::{ClusterDefinition, ClusteringConfigurationError, MemberId};

/// A validated shared definition paired with this process's configured local ID.
///
/// Created through [`super::ClusteringConfiguration::clustered`]. The local ID
/// belongs to the definition; role is derived rather than separately configured.
#[derive(Debug, Clone, PartialEq, Eq, Getters)]
#[getset(get = "pub")]
pub struct ClusterMembership {
    /// This process's logical member identity.
    local_member_id: MemberId,
    /// The shared definition, excluding local identity and runtime state.
    definition: ClusterDefinition,
}

impl ClusterMembership {
    pub(super) fn validate(
        local_member_id: MemberId,
        definition: ClusterDefinition,
    ) -> Result<Self, ClusteringConfigurationError> {
        if !definition.contains_member(&local_member_id) {
            return Err(ClusteringConfigurationError::UnknownLocalMember {
                member_id: local_member_id,
            });
        }
        Ok(Self {
            local_member_id,
            definition,
        })
    }

    /// Whether the local identity is the permanent master; false means a replica.
    ///
    /// Establishes no exclusive execution, authentication or runtime readiness.
    pub fn is_master(&self) -> bool {
        &self.local_member_id == self.definition.master_member().id()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clustering::configuration::{ClusteringConfiguration, Hostname, Member};

    fn id(name: &str) -> MemberId {
        MemberId::parse(name.into()).unwrap()
    }
    fn definition() -> ClusterDefinition {
        let member =
            |name: &str| Member::new(id(name), Hostname::parse(format!("{name}.db.")).unwrap());
        ClusterDefinition::validate(member("master"), vec![member("r02"), member("r01")]).unwrap()
    }

    #[test]
    fn shared_definition_is_identical_across_distinct_local_roles() {
        let expected = definition();
        for local in [" master ", "r01", "r02"] {
            let ClusteringConfiguration::Clustered(membership) =
                ClusteringConfiguration::clustered(id(local), expected.clone()).unwrap()
            else {
                panic!()
            };
            assert_eq!(membership.local_member_id(), &id(local));
            assert_eq!(membership.definition(), &expected);
            assert_eq!(membership.is_master(), local.trim() == "master");
        }
    }

    #[test]
    fn rejects_unlisted_and_wrong_case_local_identities() {
        for local in ["outsider", "R01"] {
            assert_eq!(
                ClusterMembership::validate(id(local), definition()),
                Err(ClusteringConfigurationError::UnknownLocalMember {
                    member_id: id(local)
                })
            );
        }
    }
}
