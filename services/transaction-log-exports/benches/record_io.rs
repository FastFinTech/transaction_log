//! Opt-in combined RecordWriter -> TCP -> RecordReader throughput.
//! See README.md and support/README.md for workload and timing contracts.

mod environment;
mod support;

fn main() -> anyhow::Result<()> {
    support::run(support::ReceiverKind::Records)
}
