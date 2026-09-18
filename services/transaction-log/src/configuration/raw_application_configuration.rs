use std::path::PathBuf;

use conf::Conf;
use getset::Getters;

use super::RawClusteringConfiguration;

/// Raw service startup inputs; subsystem conversion establishes domain validity.
#[derive(Debug, Getters, Conf)]
#[getset(get = "pub")]
#[conf(serde)]
pub struct RawApplicationConfiguration {
    /// Storage directory for this node; loading does not initialize storage.
    #[conf(long = "storage-directory", env = "TL_STORAGE_DIRECTORY")]
    #[cfg_attr(windows, conf(default_value = "./data"))]
    #[cfg_attr(not(windows), conf(default_value = "/data"))]
    storage_directory: PathBuf,
    /// Deployment mode and optional cluster settings.
    #[conf(flatten, serde(flatten))]
    clustering: RawClusteringConfiguration,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_storage_and_preserves_clustering_cli_names() {
        let env = [] as [(&str, &str); 0];
        let raw =
            RawApplicationConfiguration::try_parse_from(["test", "--cluster-mode", "single"], env)
                .unwrap();
        assert_eq!(
            raw.storage_directory(),
            &PathBuf::from(if cfg!(windows) { "./data" } else { "/data" })
        );
        let raw = RawApplicationConfiguration::try_parse_from(
            [
                "test",
                "--cluster-mode",
                "single",
                "--storage-directory",
                "node-data",
            ],
            env,
        )
        .unwrap();
        assert_eq!(raw.storage_directory(), &PathBuf::from("node-data"));
        assert_eq!(raw.clustering().mode(), "single");
    }

    #[test]
    fn loads_storage_environment_and_file_key() {
        let raw = RawApplicationConfiguration::try_parse_from(
            ["test"],
            [
                ("TL_STORAGE_DIRECTORY", "node-data"),
                ("TL_CLUSTER_MODE", "single"),
            ],
        )
        .unwrap();
        assert_eq!(raw.storage_directory(), &PathBuf::from("node-data"));
        let raw = RawApplicationConfiguration::conf_builder()
            .args(["test"])
            .env([] as [(&str, &str); 0])
            .doc(
                "app.json",
                serde_json::json!({"storage_directory":"file-data", "mode":"single"}),
            )
            .try_parse()
            .unwrap();
        assert_eq!(raw.storage_directory(), &PathBuf::from("file-data"));
    }
}
