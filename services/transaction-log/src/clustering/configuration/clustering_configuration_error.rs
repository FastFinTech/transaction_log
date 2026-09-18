use thiserror::Error;

use super::HostnameError;

/// Failure to establish a validated deployment configuration from raw inputs.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ClusteringConfigurationError {
    /// Mode must be `single` or `cluster` after trimming.
    #[error("invalid clustering mode: {value:?}; expected single or cluster")]
    InvalidMode {
        /// Original rejected mode.
        value: String,
    },
    /// Node must be one of the three fixed slots.
    #[error("invalid node name: {value:?}; expected master, replica-1 or replica-2")]
    InvalidNodeName {
        /// Original rejected node name.
        value: String,
    },
    /// Cluster mode requires this input.
    #[error("cluster mode requires {field}")]
    MissingField {
        /// Raw field name.
        field: &'static str,
    },
    /// Single mode forbids cluster-only inputs, including empty strings.
    #[error("single mode must not specify {field}")]
    UnexpectedField {
        /// Raw field name.
        field: &'static str,
    },
    /// Domain or derived node hostname fails the existing DNS-name contract.
    #[error(transparent)]
    Hostname(#[from] HostnameError),
}
