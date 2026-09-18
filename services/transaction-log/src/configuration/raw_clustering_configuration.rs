use conf::Conf;
use getset::Getters;

use super::RawClusterConfiguration;

/// Raw deployment inputs; domain validation is performed by the typed conversion.
#[derive(Debug, Clone, PartialEq, Eq, Getters, Conf)]
#[getset(get = "pub")]
#[conf(serde)]
pub struct RawClusteringConfiguration {
    /// Deployment mode: single or cluster.
    /// Permanent once initialized; cannot change for this database's storage.
    #[conf(long = "cluster-mode", env = "TL_CLUSTER_MODE")]
    mode: String,
    /// Optional cluster settings; supplying either field requires both.
    #[conf(flatten)]
    cluster: Option<RawClusterConfiguration>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_raw_env_without_domain_validation() {
        let raw = RawClusteringConfiguration::try_parse_from(
            ["test"],
            [
                ("TL_CLUSTER_MODE", " cluster "),
                ("TL_CLUSTER_MY_NODE_NAME", " replica-10 "),
                ("TL_CLUSTER_DOMAIN", " INVALID.DOMAIN.. "),
            ],
        )
        .unwrap();
        assert_eq!(raw.mode(), " cluster ");
        let cluster = raw.cluster().as_ref().unwrap();
        assert_eq!(cluster.my_node_name(), " replica-10 ");
        assert_eq!(cluster.cluster_domain(), " INVALID.DOMAIN.. ");
    }

    #[test]
    fn cli_overrides_env_and_env_overrides_nested_file_document() {
        let raw = RawClusteringConfiguration::conf_builder()
            .args(["test", "--cluster-mode", "cluster", "--cluster-my-node-name", "replica-2"])
            .env([("TL_CLUSTER_MODE", "single"), ("TL_CLUSTER_DOMAIN", "env.test")])
            .doc("cluster.json", serde_json::json!({"mode":"single", "cluster":{"my_node_name":"master", "cluster_domain":"file.test"}}))
            .try_parse().unwrap();
        assert_eq!(raw.mode(), "cluster");
        let cluster = raw.cluster().as_ref().unwrap();
        assert_eq!(cluster.my_node_name(), "replica-2");
        assert_eq!(cluster.cluster_domain(), "env.test");
        let raw = RawClusteringConfiguration::conf_builder().args(["test"])
            .env([] as [(&str, &str); 0])
            .doc("cluster.json", serde_json::json!({"mode":"cluster", "cluster":{"my_node_name":"master", "cluster_domain":"file.test"}}))
            .try_parse().unwrap();
        let cluster = raw.cluster().as_ref().unwrap();
        assert_eq!(cluster.my_node_name(), "master");
        assert_eq!(cluster.cluster_domain(), "file.test");
    }

    #[test]
    fn absent_group_is_none_and_mode_is_required() {
        let empty_env = [] as [(&str, &str); 0];
        assert!(RawClusteringConfiguration::try_parse_from(["test"], empty_env).is_err());
        for mode in ["single", "cluster"] {
            let raw = RawClusteringConfiguration::try_parse_from(
                ["test", "--cluster-mode", mode],
                empty_env,
            )
            .unwrap();
            assert_eq!(raw.cluster(), &None);
        }
        let raw = RawClusteringConfiguration::conf_builder()
            .args(["test"])
            .env(empty_env)
            .doc("single.json", serde_json::json!({"mode":"single"}))
            .try_parse()
            .unwrap();
        assert_eq!(raw.cluster(), &None);
    }

    #[test]
    fn partial_groups_are_loading_errors_in_each_source() {
        for field in ["TL_CLUSTER_MY_NODE_NAME", "TL_CLUSTER_DOMAIN"] {
            assert!(
                RawClusteringConfiguration::try_parse_from(
                    ["test"],
                    [("TL_CLUSTER_MODE", "cluster"), (field, "value")]
                )
                .is_err()
            );
        }
        for flag in ["--cluster-my-node-name", "--cluster-domain"] {
            assert!(
                RawClusteringConfiguration::try_parse_from(
                    ["test", "--cluster-mode", "cluster", flag, "value"],
                    [] as [(&str, &str); 0]
                )
                .is_err()
            );
        }
        for cluster in [
            serde_json::json!({}),
            serde_json::json!({"my_node_name":"master"}),
            serde_json::json!({"cluster_domain":"file.test"}),
        ] {
            assert!(
                RawClusteringConfiguration::conf_builder()
                    .args(["test"])
                    .env([] as [(&str, &str); 0])
                    .doc(
                        "cluster.json",
                        serde_json::json!({"mode":"cluster", "cluster":cluster})
                    )
                    .try_parse()
                    .is_err()
            );
        }
    }

    #[test]
    fn group_can_be_completed_across_sources_and_preserves_empty_values() {
        let raw = RawClusteringConfiguration::try_parse_from(
            [
                "test",
                "--cluster-mode",
                "cluster",
                "--cluster-my-node-name",
                "",
            ],
            [("TL_CLUSTER_DOMAIN", "")],
        )
        .unwrap();
        let cluster = raw.cluster().as_ref().unwrap();
        assert_eq!(cluster.my_node_name(), "");
        assert_eq!(cluster.cluster_domain(), "");
        let raw = RawClusteringConfiguration::conf_builder()
            .args(["test"])
            .env([("TL_CLUSTER_DOMAIN", "env.test")])
            .doc(
                "cluster.json",
                serde_json::json!({"mode":"cluster", "cluster":{"my_node_name":"master"}}),
            )
            .try_parse()
            .unwrap();
        assert_eq!(raw.cluster().as_ref().unwrap().cluster_domain(), "env.test");
    }
}
