use conf::Conf;
use getset::Getters;

/// Raw clustering inputs loaded through field attributes, without domain validation.
#[derive(Debug, Clone, PartialEq, Eq, Getters, Conf)]
#[getset(get = "pub")]
#[conf(serde)]
pub struct RawClusteringConfiguration {
    /// Deployment mode: singleton or clustered.
    #[conf(long = "cluster-mode", env = "TL_CLUSTER_MODE")]
    mode: String,
    /// Local member ID; its hostname comes from the shared membership.
    #[conf(long = "cluster-local-member-id", env = "TL_CLUSTER_LOCAL_MEMBER_ID")]
    local_member_id: Option<String>,
    /// Master as a member_id=hostname pair.
    #[conf(long = "cluster-master", env = "TL_CLUSTER_MASTER")]
    master: Option<String>,
    /// Replicas as comma-separated member_id=hostname pairs.
    #[conf(long = "cluster-replicas", env = "TL_CLUSTER_REPLICAS")]
    replicas: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_environment_values_without_normalizing_or_validating() {
        let raw = RawClusteringConfiguration::try_parse_from(
            ["test"],
            [
                ("TL_CLUSTER_MODE", " clustered "),
                ("TL_CLUSTER_LOCAL_MEMBER_ID", " replica-01 "),
                ("TL_CLUSTER_MASTER", "master-01=MASTER.DB"),
                ("TL_CLUSTER_REPLICAS", "replica-01=one.db,replica-02=two.db"),
            ],
        )
        .unwrap();
        assert_eq!(raw.mode(), " clustered ");
        assert_eq!(raw.local_member_id().as_deref(), Some(" replica-01 "));
        assert_eq!(raw.master().as_deref(), Some("master-01=MASTER.DB"));
        assert_eq!(
            raw.replicas().as_deref(),
            Some("replica-01=one.db,replica-02=two.db")
        );
    }

    #[test]
    fn loads_cli_values_and_cli_overrides_environment() {
        let raw = RawClusteringConfiguration::try_parse_from(
            [
                "test",
                "--cluster-mode",
                "clustered",
                "--cluster-local-member-id",
                "replica-01",
                "--cluster-master",
                "master-01=master.db",
                "--cluster-replicas",
                "bad-input",
            ],
            [
                ("TL_CLUSTER_MODE", "singleton"),
                ("TL_CLUSTER_REPLICAS", "env-input"),
            ],
        )
        .unwrap();
        assert_eq!(raw.mode(), "clustered");
        assert_eq!(raw.local_member_id().as_deref(), Some("replica-01"));
        assert_eq!(raw.master().as_deref(), Some("master-01=master.db"));
        assert_eq!(raw.replicas().as_deref(), Some("bad-input"));
    }

    #[test]
    fn loads_file_document_with_cli_and_environment_overrides() {
        let document = serde_json::json!({
            "mode": "singleton", "local_member_id": "master-01",
            "master": "master-01=master.db", "replicas": "file-input"
        });
        let raw = RawClusteringConfiguration::conf_builder()
            .args(["test", "--cluster-mode", "clustered"])
            .env([("TL_CLUSTER_REPLICAS", "env-input")])
            .doc("cluster.json", document)
            .try_parse()
            .unwrap();
        assert_eq!(raw.mode(), "clustered");
        assert_eq!(raw.local_member_id().as_deref(), Some("master-01"));
        assert_eq!(raw.master().as_deref(), Some("master-01=master.db"));
        assert_eq!(raw.replicas().as_deref(), Some("env-input"));
    }

    #[test]
    fn requires_mode_preserves_empty_strings_and_leaves_other_fields_optional() {
        let empty_env: [(&str, &str); 0] = [];
        assert!(RawClusteringConfiguration::try_parse_from(["test"], empty_env).is_err());
        let raw = RawClusteringConfiguration::try_parse_from(
            ["test"],
            [("TL_CLUSTER_MODE", "singleton"), ("TL_CLUSTER_MASTER", "")],
        )
        .unwrap();
        assert_eq!(raw.local_member_id(), &None);
        assert_eq!(raw.master().as_deref(), Some(""));
        assert_eq!(raw.replicas(), &None);
    }
}
