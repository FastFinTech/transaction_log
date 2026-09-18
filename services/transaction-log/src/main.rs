pub mod clustering;
pub mod configuration;
pub mod storage;
pub mod streams;

use anyhow::Result;
use conf::Conf;
use configuration::clustering::RawClusteringConfiguration;

#[tokio::main]
async fn main() -> Result<()> {
    let _configuration = RawClusteringConfiguration::parse();
    println!("Transaction Log v{}", env!("CARGO_PKG_VERSION"));
    Ok(())
}
