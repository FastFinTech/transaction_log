pub mod log_file_provider;

use anyhow::Result;
use transaction_log_exports as _;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Hello world");
    Ok(())
}
