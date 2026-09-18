use conf::Conf;
use getset::Getters;

/// Complete raw cluster settings; individual values still require domain validation.
#[derive(Debug, Clone, PartialEq, Eq, Getters, Conf)]
#[getset(get = "pub")]
#[conf(serde)]
pub struct RawClusterConfiguration {
    /// Local node name: master, replica-1 or replica-2. Required in cluster mode.
    /// Permanent once initialized; cannot change for this node's storage.
    #[conf(long = "cluster-my-node-name", env = "TL_CLUSTER_MY_NODE_NAME")]
    my_node_name: String,
    /// Cluster DNS domain; node addresses are node-name.domain. Required in cluster mode.
    #[conf(long = "cluster-domain", env = "TL_CLUSTER_DOMAIN")]
    cluster_domain: String,
}
