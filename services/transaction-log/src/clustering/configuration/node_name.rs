use super::ClusteringConfigurationError;
use serde::{Deserialize, Serialize};

/// One permanent slot in the fixed three-node cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum NodeName {
    /// The permanent master; no election or promotion is supported.
    Master,
    /// The first required replica.
    Replica1,
    /// The second required replica.
    Replica2,
}

impl TryFrom<String> for NodeName {
    type Error = ClusteringConfigurationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<NodeName> for String {
    fn from(node: NodeName) -> Self {
        node.name().into()
    }
}

impl NodeName {
    /// Parses a trimmed, case-sensitive `master`, `replica-1` or `replica-2`.
    pub fn parse(value: String) -> Result<Self, ClusteringConfigurationError> {
        match value.trim() {
            "master" => Ok(Self::Master),
            "replica-1" => Ok(Self::Replica1),
            "replica-2" => Ok(Self::Replica2),
            _ => Err(ClusteringConfigurationError::InvalidNodeName { value }),
        }
    }

    /// The fixed DNS label and handshake name for this slot.
    pub fn name(self) -> &'static str {
        match self {
            Self::Master => "master",
            Self::Replica1 => "replica-1",
            Self::Replica2 => "replica-2",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_only_fixed_slots_and_preserves_rejected_input() {
        for node in [NodeName::Master, NodeName::Replica1, NodeName::Replica2] {
            assert_eq!(NodeName::parse(format!("  {}  ", node.name())), Ok(node));
        }
        for value in [
            "",
            " ",
            "Master",
            "replica-0",
            "replica-3",
            "replica-10",
            "replica-01",
            "réplica-1",
        ] {
            assert_eq!(
                NodeName::parse(value.into()),
                Err(ClusteringConfigurationError::InvalidNodeName {
                    value: value.into()
                })
            );
        }
    }
}
