use getset::Getters;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::ClusteringConfiguration;

/// Deployment configuration paired with its permanent, internally established UUID.
///
/// Both singleton and clustered configurations have a UUID. Construction and Serde
/// loading do not read, initialize or establish exclusive ownership of storage.
#[derive(Debug, Clone, PartialEq, Eq, Getters, Serialize, Deserialize)]
#[getset(get = "pub")]
#[serde(deny_unknown_fields)]
pub struct EstablishedClusteringConfiguration {
    /// UUID supplied by establishment and retained; shared by members in cluster mode.
    cluster_id: Uuid,
    /// Permanent mode and, in cluster mode, shared domain and local node slot.
    clustering_configuration: ClusteringConfiguration,
}

impl EstablishedClusteringConfiguration {
    /// Combines a supplied UUID with validated configuration in either mode.
    ///
    /// Does not generate the UUID, establish cluster agreement, persist metadata
    /// or enforce permanent assignment. No UUID version restriction is imposed.
    pub fn new(cluster_id: Uuid, clustering_configuration: ClusteringConfiguration) -> Self {
        Self {
            cluster_id,
            clustering_configuration,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clustering::configuration::{ClusterMembership, Hostname, NodeName};

    const UUID: &str = "12345678-9abc-def0-1234-56789abcdef0";

    fn configuration(node: NodeName) -> ClusteringConfiguration {
        ClusteringConfiguration::Cluster(ClusterMembership::new(
            node,
            Hostname::parse(" CLUSTER.EXAMPLE. ".into()).unwrap(),
        ))
    }

    #[test]
    fn json_matches_independent_fixtures_in_both_modes_and_all_slots() {
        let id = Uuid::parse_str(UUID).unwrap();
        let single = EstablishedClusteringConfiguration::new(id, ClusteringConfiguration::Single);
        let fixture = format!(r#"{{"cluster_id":"{UUID}","clustering_configuration":"single"}}"#);
        assert_eq!(serde_json::to_string(&single).unwrap(), fixture);
        assert_eq!(
            serde_json::from_str::<EstablishedClusteringConfiguration>(&fixture).unwrap(),
            single
        );
        assert_eq!(*single.cluster_id(), id);
        assert_eq!(
            single.clustering_configuration(),
            &ClusteringConfiguration::Single
        );
        for node in [NodeName::Master, NodeName::Replica1, NodeName::Replica2] {
            let expected = serde_json::json!({
                "cluster_id": UUID,
                "clustering_configuration": {"cluster": {
                    "my_node_name": node.name(), "cluster_domain": "cluster.example."
                }}
            });
            let identity = EstablishedClusteringConfiguration::new(id, configuration(node));
            assert_eq!(serde_json::to_value(&identity).unwrap(), expected);
            assert_eq!(
                serde_json::from_value::<EstablishedClusteringConfiguration>(expected).unwrap(),
                identity
            );
        }
    }

    #[test]
    fn loading_normalizes_configuration_and_equality_includes_id_and_slot() {
        let value = serde_json::json!({
            "cluster_id": UUID,
            "clustering_configuration": {"cluster": {
                "my_node_name": " master ", "cluster_domain": " CLUSTER.EXAMPLE. "
            }}
        });
        let loaded: EstablishedClusteringConfiguration = serde_json::from_value(value).unwrap();
        assert_eq!(
            loaded,
            EstablishedClusteringConfiguration::new(
                Uuid::parse_str(UUID).unwrap(),
                configuration(NodeName::Master)
            )
        );
        assert_ne!(
            loaded,
            EstablishedClusteringConfiguration::new(
                *loaded.cluster_id(),
                configuration(NodeName::Replica1)
            )
        );
        for config in [
            ClusteringConfiguration::Single,
            configuration(NodeName::Master),
        ] {
            assert_ne!(
                EstablishedClusteringConfiguration::new(Uuid::from_u128(1), config.clone()),
                EstablishedClusteringConfiguration::new(Uuid::from_u128(2), config)
            );
        }
    }

    #[test]
    fn rejects_malformed_identity_and_invalid_nested_configuration() {
        for value in [
            serde_json::json!({}),
            serde_json::json!({"cluster_id": UUID}),
            serde_json::json!({"clustering_configuration": "single"}),
            serde_json::json!({"cluster_id": "bad-uuid", "clustering_configuration": "single"}),
            serde_json::json!({"cluster_id": UUID, "clustering_configuration": "single", "local_node_name": "master"}),
            serde_json::json!({"cluster_id": UUID, "clustering_configuration": {"cluster": {"my_node_name": "replica-3", "cluster_domain": "example"}}}),
            serde_json::json!({"cluster_id": UUID, "clustering_configuration": {"cluster": {"my_node_name": "master", "cluster_domain": "example.."}}}),
        ] {
            assert!(serde_json::from_value::<EstablishedClusteringConfiguration>(value).is_err());
        }
        for fixture in [
            format!(
                r#"{{"cluster_id":"{UUID}","cluster_id":"{UUID}","clustering_configuration":"single"}}"#
            ),
            format!(
                r#"{{"cluster_id":"{UUID}","clustering_configuration":"single","clustering_configuration":"single"}}"#
            ),
        ] {
            assert!(
                serde_json::from_str::<EstablishedClusteringConfiguration>(&fixture)
                    .unwrap_err()
                    .to_string()
                    .contains("duplicate field")
            );
        }
        let trailing =
            format!(r#"{{"cluster_id":"{UUID}","clustering_configuration":"single"}} {{}}"#);
        assert!(serde_json::from_str::<EstablishedClusteringConfiguration>(&trailing).is_err());
    }
}
