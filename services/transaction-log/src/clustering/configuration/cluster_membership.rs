use getset::Getters;
use serde::{Deserialize, Serialize};

use super::{Hostname, NodeName};

/// This process's fixed node slot and the validated shared cluster domain.
#[derive(Debug, Clone, PartialEq, Eq, Getters, Serialize, Deserialize)]
#[getset(get = "pub")]
#[serde(deny_unknown_fields)]
pub struct ClusterMembership {
    /// This node's permanent slot, which must not change after storage initialization.
    my_node_name: NodeName,
    /// Shared domain validated by the hostname type.
    cluster_domain: Hostname,
}

impl ClusterMembership {
    /// Combines a fixed node slot and parsed domain without further validation.
    pub fn new(my_node_name: NodeName, cluster_domain: Hostname) -> Self {
        Self {
            my_node_name,
            cluster_domain,
        }
    }

    /// Whether this slot is the permanent master.
    pub fn is_master(&self) -> bool {
        self.my_node_name == NodeName::Master
    }

    /// Formats an owned hostname for a fixed slot when a connection needs it.
    ///
    /// Formats without checking the resulting hostname length. Preserves
    /// a trailing root dot; does not resolve DNS or revalidate the stored domain.
    pub fn hostname(&self, node: NodeName) -> String {
        format!("{}.{}", node.name(), self.cluster_domain.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_validates_membership_boundaries_and_object_fields() {
        let domain = format!(
            "{}.{}.{}.{}",
            "a".repeat(63),
            "b".repeat(63),
            "c".repeat(63),
            "d".repeat(61)
        );
        for suffix in ["", "."] {
            let value = serde_json::json!({"my_node_name": "master", "cluster_domain": format!("{domain}{suffix}")});
            let membership: ClusterMembership = serde_json::from_value(value.clone()).unwrap();
            assert_eq!(serde_json::to_value(membership).unwrap(), value);
            let too_long = serde_json::json!({"my_node_name": "master", "cluster_domain": format!("{domain}d{suffix}")});
            assert!(serde_json::from_value::<ClusterMembership>(too_long).is_err());
        }
        for fixture in [
            r#"{}"#,
            r#"{"my_node_name":"master"}"#,
            r#"{"my_node_name":"replica-3","cluster_domain":"example"}"#,
            r#"{"my_node_name":"master","cluster_domain":"example.."}"#,
            r#"{"my_node_name":"master","cluster_domain":"127.0.0.1"}"#,
            r#"{"my_node_name":"master","cluster_domain":"example","extra":true}"#,
            r#"{"my_node_name":"master","my_node_name":"replica-1","cluster_domain":"example"}"#,
            r#"{"my_node_name":"master","cluster_domain":"example","cluster_domain":"other"}"#,
        ] {
            assert!(
                serde_json::from_str::<ClusterMembership>(fixture).is_err(),
                "{fixture}"
            );
        }
    }

    #[test]
    fn all_roles_share_domain_and_derive_fixed_hostnames() {
        let domain = Hostname::parse(" DEV-CLUSTER.TEST. ".into()).unwrap();
        for node in [NodeName::Master, NodeName::Replica1, NodeName::Replica2] {
            let membership = ClusterMembership::new(node, domain.clone());
            assert_eq!(*membership.my_node_name(), node);
            assert_eq!(membership.cluster_domain(), &domain);
            assert_eq!(membership.is_master(), node == NodeName::Master);
            for (peer, expected) in [
                (NodeName::Master, "master.dev-cluster.test."),
                (NodeName::Replica1, "replica-1.dev-cluster.test."),
                (NodeName::Replica2, "replica-2.dev-cluster.test."),
            ] {
                assert_eq!(membership.hostname(peer), expected);
            }
        }
    }

    #[test]
    fn domain_agreement_normalizes_but_preserves_root_dot() {
        let make = |node, domain: &str| {
            ClusterMembership::new(node, Hostname::parse(domain.into()).unwrap())
        };
        let master = make(NodeName::Master, " PAYMENTS.EXAMPLE.COM ");
        let replica = make(NodeName::Replica1, "payments.example.com");
        assert_eq!(master.cluster_domain(), replica.cluster_domain());
        assert_ne!(master, replica);
        assert_ne!(
            master.cluster_domain(),
            make(NodeName::Replica1, "payments.example.com.").cluster_domain()
        );
        assert_ne!(
            master.cluster_domain(),
            make(NodeName::Replica1, "other.example.com").cluster_domain()
        );
    }
}
