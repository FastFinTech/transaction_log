#![doc = include_str!("README.md")]
#![deny(missing_docs)]

mod cluster_definition;
mod cluster_definition_error;
mod cluster_definition_mismatch;
mod cluster_membership;
mod clustering_configuration;
mod clustering_configuration_error;
mod hostname;
mod hostname_error;
mod hostname_error_kind;
mod member;
mod member_id;
mod member_id_error;
mod member_id_error_kind;

pub use cluster_definition::{ClusterDefinition, MINIMUM_REPLICA_COUNT};
pub use cluster_definition_error::ClusterDefinitionError;
pub use cluster_definition_mismatch::ClusterDefinitionMismatch;
pub use cluster_membership::ClusterMembership;
pub use clustering_configuration::ClusteringConfiguration;
pub use clustering_configuration_error::ClusteringConfigurationError;
pub use hostname::Hostname;
pub use hostname_error::HostnameError;
pub use hostname_error_kind::HostnameErrorKind;
pub use member::Member;
pub use member_id::MemberId;
pub use member_id_error::MemberIdError;
pub use member_id_error_kind::MemberIdErrorKind;
