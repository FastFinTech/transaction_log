use crate::configuration::RawClusteringConfiguration;
use serde::{Deserialize, Serialize};

use super::{ClusterMembership, ClusteringConfigurationError, NodeName};

/// Validated deployment mode; persisted mode and runtime readiness are separate.
/// Mode must remain permanent for initialized storage; startup enforcement is deferred.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ClusteringConfiguration {
    /// Standalone operation without membership.
    Single,
    /// One permanent master and exactly two required replicas.
    Cluster(ClusterMembership),
}

impl TryFrom<RawClusteringConfiguration> for ClusteringConfiguration {
    type Error = ClusteringConfigurationError;

    /// Validates mode, conditional inputs, fixed node slot and domain syntax.
    ///
    /// Trims mode/node names without case folding. Single forbids cluster inputs;
    /// cluster requires the complete group. Does not perform I/O or validate persisted identities.
    fn try_from(raw: RawClusteringConfiguration) -> Result<Self, Self::Error> {
        use ClusteringConfigurationError::*;
        match raw.mode().trim() {
            "single" => {
                if raw.cluster().is_some() {
                    return Err(UnexpectedField { field: "cluster" });
                }
                Ok(Self::Single)
            }
            "cluster" => {
                let cluster = raw
                    .cluster()
                    .as_ref()
                    .ok_or(MissingField { field: "cluster" })?;
                let node = NodeName::parse(cluster.my_node_name().clone())?;
                let domain = super::Hostname::parse(cluster.cluster_domain().clone())?;
                Ok(Self::Cluster(ClusterMembership::new(node, domain)))
            }
            _ => Err(InvalidMode {
                value: raw.mode().clone(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use conf::Conf;

    fn raw(mode: &str, node: Option<&str>, domain: Option<&str>) -> RawClusteringConfiguration {
        let mut env = vec![("TL_CLUSTER_MODE", mode)];
        if let Some(node) = node {
            env.push(("TL_CLUSTER_MY_NODE_NAME", node));
        }
        if let Some(domain) = domain {
            env.push(("TL_CLUSTER_DOMAIN", domain));
        }
        RawClusteringConfiguration::try_parse_from(["test"], env).unwrap()
    }

    #[test]
    fn converts_single_and_each_cluster_role() {
        assert_eq!(
            ClusteringConfiguration::try_from(raw(" single ", None, None)),
            Ok(ClusteringConfiguration::Single)
        );
        for node in ["master", "replica-1", "replica-2"] {
            let ClusteringConfiguration::Cluster(membership) =
                ClusteringConfiguration::try_from(raw(
                    " cluster ",
                    Some(&format!(" {node} ")),
                    Some(" PAYMENTS.EXAMPLE.COM. "),
                ))
                .unwrap()
            else {
                panic!("wrong mode")
            };
            assert_eq!(membership.my_node_name().name(), node);
            assert_eq!(
                membership.hostname(*membership.my_node_name()),
                format!("{node}.payments.example.com.")
            );
            assert_eq!(membership.is_master(), node == "master");
        }
    }

    #[test]
    fn rejects_invalid_modes_missing_and_conflicting_inputs() {
        use ClusteringConfigurationError::*;
        for mode in ["", " ", "Single", "CLUSTER", "singleton", "clustered"] {
            assert_eq!(
                ClusteringConfiguration::try_from(raw(mode, None, None)),
                Err(InvalidMode { value: mode.into() })
            );
        }
        assert_eq!(
            ClusteringConfiguration::try_from(raw("cluster", None, None)),
            Err(MissingField { field: "cluster" })
        );
        assert_eq!(
            ClusteringConfiguration::try_from(raw("single", Some(""), Some(""))),
            Err(UnexpectedField { field: "cluster" })
        );
        assert!(matches!(
            ClusteringConfiguration::try_from(raw("cluster", Some("replica-10"), Some("db.test"))),
            Err(InvalidNodeName { .. })
        ));
        assert!(matches!(
            ClusteringConfiguration::try_from(raw("cluster", Some("master"), Some(""))),
            Err(Hostname(_))
        ));
    }
}
