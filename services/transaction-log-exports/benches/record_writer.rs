//! Opt-in writer throughput over TCP, with a receiver that only drains bytes.
//! See README.md and support/README.md for workload and timing contracts.

mod environment;
mod support;

fn main() -> anyhow::Result<()> {
    support::run(support::ReceiverKind::Bytes)
}
