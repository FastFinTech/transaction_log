pub mod storage;
pub mod streams;

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Hello world");
    Ok(())
}
