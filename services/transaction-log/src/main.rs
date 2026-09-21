#![doc = include_str!("../README.md")]

pub mod clustering;
pub mod configuration;
pub mod messages;
pub mod storage;
pub mod streams;

use anyhow::Result;
use clustering::configuration::ClusteringConfiguration;
use conf::Conf;
use configuration::RawApplicationConfiguration;

#[tokio::main]
async fn main() -> Result<()> {
    let raw = RawApplicationConfiguration::parse();
    ClusteringConfiguration::try_from(raw.clustering().clone())?;
    println!("Transaction Log v{}", env!("CARGO_PKG_VERSION"));
    Ok(())
}
