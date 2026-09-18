use getset::Getters;

use super::{ClusterDefinitionError, ClusterDefinitionMismatch, Member, MemberId};

/// The fixed minimum number of replicas, excluding the master.
pub const MINIMUM_REPLICA_COUNT: usize = 2;

/// The validated shared definition that all members must agree on.
///
/// Owns one permanent master and at least two replicas, with unique IDs and
/// parsed hostnames. Replica order is canonicalized by case-sensitive member ID,
/// so equality ignores supplied order. No local identity or runtime state is stored.
#[derive(Debug, Clone, PartialEq, Eq, Getters)]
pub struct ClusterDefinition {
    /// Identity and hostname of the permanent master.
    #[getset(get = "pub")]
    master_member: Member,
    replica_members: Vec<Member>,
}

impl ClusterDefinition {
    /// Owns validated members after checking their complete shared relationship.
    ///
    /// Checks count, then each replica in supplied order for master ID, duplicate
    /// ID and duplicate hostname. Names are already parsed and are not revalidated.
    /// After validation, sorts replicas by member ID without cloning their strings.
    /// No DNS lookup or I/O occurs.
    pub fn validate(
        master_member: Member,
        mut replica_members: Vec<Member>,
    ) -> Result<Self, ClusterDefinitionError> {
        use ClusterDefinitionError::*;
        if replica_members.len() < MINIMUM_REPLICA_COUNT {
            return Err(TooFewReplicas {
                count: replica_members.len(),
            });
        }
        for (index, member) in replica_members.iter().enumerate() {
            let member_id = member.id();
            if member_id == master_member.id() {
                return Err(MasterIsReplica {
                    member_id: member_id.clone(),
                });
            }
            if replica_members[..index]
                .iter()
                .any(|earlier| earlier.id() == member_id)
            {
                return Err(DuplicateReplica {
                    member_id: member_id.clone(),
                });
            }
            let first_with_hostname = if member.hostname() == master_member.hostname() {
                Some(&master_member)
            } else {
                replica_members[..index]
                    .iter()
                    .find(|earlier| earlier.hostname() == member.hostname())
            };
            if let Some(first) = first_with_hostname {
                return Err(DuplicateHostname {
                    hostname: member.hostname().clone(),
                    first_member_id: first.id().clone(),
                    member_id: member_id.clone(),
                });
            }
        }
        replica_members.sort_unstable_by(|left, right| left.id().name().cmp(right.id().name()));
        Ok(Self {
            master_member,
            replica_members,
        })
    }

    /// Borrows replicas in canonical member-ID order, excluding the master.
    pub fn replica_members(&self) -> &[Member] {
        &self.replica_members
    }

    /// Whether an identity belongs to the master or replica set.
    ///
    /// Compares case-sensitive parsed IDs independently of hostnames.
    pub fn contains_member(&self, member_id: &MemberId) -> bool {
        self.master_member.id() == member_id
            || self
                .replica_members
                .iter()
                .any(|member| member.id() == member_id)
    }

    /// Checks another definition against this expected definition, ignoring input order.
    ///
    /// Returns the first mismatch: master ID, master hostname, then missing or
    /// changed expected replicas in canonical ID order, then unexpected replicas
    /// in the other's canonical order. Expected/actual context is retained in the
    /// typed error. Uses parsed names, preserves root-dot differences and performs
    /// no DNS resolution, authentication or live-session checks.
    pub fn verify_matches(&self, actual: &Self) -> Result<(), ClusterDefinitionMismatch> {
        use ClusterDefinitionMismatch::*;
        if self.master_member.id() != actual.master_member.id() {
            return Err(Master {
                expected: self.master_member.id().clone(),
                actual: actual.master_member.id().clone(),
            });
        }
        if self.master_member.hostname() != actual.master_member.hostname() {
            return Err(Hostname {
                member_id: self.master_member.id().clone(),
                expected: self.master_member.hostname().clone(),
                actual: actual.master_member.hostname().clone(),
            });
        }
        for expected in &self.replica_members {
            let Some(member) = actual
                .replica_members
                .iter()
                .find(|member| member.id() == expected.id())
            else {
                return Err(MissingReplica {
                    member_id: expected.id().clone(),
                });
            };
            if expected.hostname() != member.hostname() {
                return Err(Hostname {
                    member_id: expected.id().clone(),
                    expected: expected.hostname().clone(),
                    actual: member.hostname().clone(),
                });
            }
        }
        for member in &actual.replica_members {
            if !self
                .replica_members
                .iter()
                .any(|expected| expected.id() == member.id())
            {
                return Err(UnexpectedReplica {
                    member_id: member.id().clone(),
                });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clustering::configuration::Hostname;
    use ClusterDefinitionError::*;

    fn id(name: &str) -> MemberId {
        MemberId::parse(name.into()).unwrap()
    }
    fn host(name: &str) -> Hostname {
        Hostname::parse(name.into()).unwrap()
    }
    fn member(name: &str) -> Member {
        Member::new(id(name), host(&format!("{}.db.", name.trim())))
    }
    fn definition(replicas: &[&str]) -> ClusterDefinition {
        ClusterDefinition::validate(
            member("master"),
            replicas.iter().map(|name| member(name)).collect(),
        )
        .unwrap()
    }

    #[test]
    fn validates_minimum_and_canonicalizes_replica_order() {
        for (replicas, count) in [(vec![], 0), (vec![member("r01")], 1)] {
            assert_eq!(
                ClusterDefinition::validate(member("master"), replicas),
                Err(TooFewReplicas { count })
            );
        }
        let expected = definition(&["r02", "r01", "r03"]);
        assert_eq!(
            expected.replica_members(),
            &[member("r01"), member("r02"), member("r03")]
        );
        for order in [
            ["r01", "r02", "r03"],
            ["r03", "r02", "r01"],
            ["r02", "r03", "r01"],
        ] {
            let actual = definition(&order);
            assert_eq!(expected, actual);
            assert_eq!(expected.verify_matches(&actual), Ok(()));
        }
        assert!(expected.contains_member(&id("master")));
        assert!(expected.contains_member(&id("r03")));
        assert!(!expected.contains_member(&id("R03")));
        assert!(!expected.contains_member(&id("outsider")));
    }

    #[test]
    fn rejects_conflicting_ids_independently_of_hostnames() {
        assert_eq!(
            ClusterDefinition::validate(
                member("master"),
                vec![
                    member("r01"),
                    Member::new(id(" r01 "), host("different.db."))
                ]
            ),
            Err(DuplicateReplica {
                member_id: id("r01")
            })
        );
        assert_eq!(
            ClusterDefinition::validate(
                member("master"),
                vec![member("r01"), member("r02"), member("r01")]
            ),
            Err(DuplicateReplica {
                member_id: id("r01")
            })
        );
        assert_eq!(
            ClusterDefinition::validate(
                member("master"),
                vec![
                    member("r01"),
                    Member::new(id(" master "), host("different.db."))
                ]
            ),
            Err(MasterIsReplica {
                member_id: id("master")
            })
        );
    }

    #[test]
    fn rejects_normalized_hostnames_and_preserves_validation_precedence() {
        for (first_id, hostname, replicas) in [
            (
                "master",
                "master.db.",
                vec![Member::new(id("r01"), host(" MASTER.DB. ")), member("r02")],
            ),
            (
                "r01",
                "r01.db.",
                vec![
                    member("r01"),
                    member("r02"),
                    Member::new(id("r03"), host(" R01.DB. ")),
                ],
            ),
        ] {
            let last_id = if first_id == "master" { "r01" } else { "r03" };
            assert_eq!(
                ClusterDefinition::validate(member("master"), replicas),
                Err(DuplicateHostname {
                    hostname: host(hostname),
                    first_member_id: id(first_id),
                    member_id: id(last_id)
                })
            );
        }
        assert_eq!(
            ClusterDefinition::validate(member("master"), vec![member("master")]),
            Err(TooFewReplicas { count: 1 })
        );
        assert_eq!(
            ClusterDefinition::validate(
                member("master"),
                vec![member("r01"), member("r01"), member("master")]
            ),
            Err(DuplicateReplica {
                member_id: id("r01")
            })
        );
        assert_eq!(
            ClusterDefinition::validate(
                member("master"),
                vec![Member::new(id("r01"), host("master.db.")), member("master")]
            ),
            Err(DuplicateHostname {
                hostname: host("master.db."),
                first_member_id: id("master"),
                member_id: id("r01")
            })
        );
    }

    #[test]
    fn reports_master_and_hostname_mismatches_with_expected_and_actual_context() {
        use ClusterDefinitionMismatch as M;
        let expected = definition(&["r01", "r02"]);
        let actual =
            ClusterDefinition::validate(member("other"), vec![member("r01"), member("r02")])
                .unwrap();
        assert_eq!(
            expected.verify_matches(&actual),
            Err(M::Master {
                expected: id("master"),
                actual: id("other")
            })
        );
        let actual = ClusterDefinition::validate(
            Member::new(id("master"), host("other.db.")),
            vec![member("r01"), member("r02")],
        )
        .unwrap();
        assert_eq!(
            expected.verify_matches(&actual),
            Err(M::Hostname {
                member_id: id("master"),
                expected: host("master.db."),
                actual: host("other.db.")
            })
        );
        let actual = ClusterDefinition::validate(
            member("master"),
            vec![member("r02"), Member::new(id("r01"), host("r01.db"))],
        )
        .unwrap();
        assert_eq!(
            expected.verify_matches(&actual),
            Err(M::Hostname {
                member_id: id("r01"),
                expected: host("r01.db."),
                actual: host("r01.db")
            })
        );
    }

    #[test]
    fn reports_missing_and_unexpected_replicas_in_canonical_order() {
        use ClusterDefinitionMismatch as M;
        let small = definition(&["r01", "r02"]);
        let large = definition(&["r04", "r02", "r03", "r01"]);
        assert_eq!(
            large.verify_matches(&small),
            Err(M::MissingReplica {
                member_id: id("r03")
            })
        );
        assert_eq!(
            small.verify_matches(&large),
            Err(M::UnexpectedReplica {
                member_id: id("r03")
            })
        );
        let changed = definition(&["r03", "r02"]);
        assert_eq!(
            small.verify_matches(&changed),
            Err(M::MissingReplica {
                member_id: id("r01")
            })
        );
        assert_ne!(small, changed);
    }

    #[test]
    fn comparison_uses_parsed_names_and_case_sensitive_ids() {
        let expected = definition(&["r01", "r02"]);
        let normalized = ClusterDefinition::validate(
            Member::new(id(" master "), host(" MASTER.DB. ")),
            vec![member("r02"), Member::new(id(" r01 "), host(" R01.DB. "))],
        )
        .unwrap();
        assert_eq!(expected, normalized);
        assert_eq!(expected.verify_matches(&normalized), Ok(()));
        let changed_case = ClusterDefinition::validate(
            member("master"),
            vec![Member::new(id("R01"), host("r01.db.")), member("r02")],
        )
        .unwrap();
        assert_eq!(
            expected.verify_matches(&changed_case),
            Err(ClusterDefinitionMismatch::MissingReplica {
                member_id: id("r01")
            })
        );
    }

    #[test]
    fn reports_master_difference_before_replica_differences() {
        let expected = definition(&["r01", "r02"]);
        let actual =
            ClusterDefinition::validate(member("other"), vec![member("r03"), member("r04")])
                .unwrap();
        assert_eq!(
            expected.verify_matches(&actual),
            Err(ClusterDefinitionMismatch::Master {
                expected: id("master"),
                actual: id("other")
            })
        );
        let actual = ClusterDefinition::validate(
            member("master"),
            vec![Member::new(id("r02"), host("other.db.")), member("r03")],
        )
        .unwrap();
        assert_eq!(
            expected.verify_matches(&actual),
            Err(ClusterDefinitionMismatch::MissingReplica {
                member_id: id("r01")
            })
        );
    }
}
