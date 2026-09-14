//! Opt-in TCP throughput measurement. See README.md for timing and workload rules.

use std::{
    collections::VecDeque,
    hint::black_box,
    io::Write,
    net::{Shutdown, TcpListener, TcpStream},
    sync::{Arc, mpsc},
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use tokio::runtime::{Builder, Runtime};
use transaction_log_exports::{
    Record, RecordReader, StreamId, record::record_protocol as protocol,
};

const HELP: &str = "\
RecordReader TCP loopback benchmark (explicit runs only)

cargo bench -p transaction-log-exports --bench record_reader -- [options]

  --records N         Total records per measured run (default 100000000)
  --connections N     Concurrent connections/read threads (default 1)
  --payload-bytes N   Payload bytes per record, 0..=65519 (default 2048)
  --batch-records N   Records in the reusable sender batch (default 8192)
  --retain-records N  Keep the most recent N records per reader (default 0)
  --warmup-records N  Total warmup records, 0 disables (default 1000000)
  --runs N            Measured transfers (default 1)
  --help              Show this help

Cargo supplies --bench to authorize measurement. Without it, or with --test,
this executable exits without building fixtures or opening sockets.
";

struct Config {
    records: u64,
    connections: usize,
    payload_bytes: usize,
    batch_records: usize,
    retain_records: usize,
    warmup_records: u64,
    runs: usize,
}

impl Config {
    fn parse(args: &[String]) -> Result<Self> {
        let mut config = Self {
            records: 100_000_000,
            connections: 1,
            payload_bytes: 2048,
            batch_records: 8192,
            retain_records: 0,
            warmup_records: 1_000_000,
            runs: 1,
        };
        let mut args = args.iter();
        while let Some(flag) = args.next() {
            if flag == "--bench" {
                continue;
            }
            let value = args
                .next()
                .with_context(|| format!("missing value for {flag}"))?;
            match flag.as_str() {
                "--records" => config.records = value.parse().context("invalid --records")?,
                "--connections" => {
                    config.connections = value.parse().context("invalid --connections")?
                }
                "--payload-bytes" => {
                    config.payload_bytes = value.parse().context("invalid --payload-bytes")?
                }
                "--batch-records" => {
                    config.batch_records = value.parse().context("invalid --batch-records")?
                }
                "--retain-records" => {
                    config.retain_records = value.parse().context("invalid --retain-records")?
                }
                "--warmup-records" => {
                    config.warmup_records = value.parse().context("invalid --warmup-records")?
                }
                "--runs" => config.runs = value.parse().context("invalid --runs")?,
                _ => bail!("unknown option {flag}; use --help"),
            }
        }
        ensure!(config.records > 0, "--records must be positive");
        ensure!(config.connections > 0, "--connections must be positive");
        ensure!(
            config.records >= config.connections as u64,
            "--records must be at least --connections"
        );
        ensure!(
            config.warmup_records == 0 || config.warmup_records >= config.connections as u64,
            "--warmup-records must be zero or at least --connections"
        );
        config
            .connections
            .checked_mul(2)
            .context("worker count overflow")?;
        ensure!(config.batch_records > 0, "--batch-records must be positive");
        ensure!(config.runs > 0, "--runs must be positive");
        ensure!(
            config.payload_bytes <= protocol::MAX_PAYLOAD_LEN,
            "payload exceeds protocol maximum {}",
            protocol::MAX_PAYLOAD_LEN
        );
        config.expected_bytes(config.records)?;
        config.expected_bytes(config.warmup_records)?;
        Ok(config)
    }

    fn record_len(&self) -> usize {
        protocol::MIN_RECORD_LEN + self.payload_bytes
    }

    fn expected_bytes(&self, records: u64) -> Result<u64> {
        records
            .checked_mul(self.record_len() as u64)
            .context("total byte count exceeds u64")
    }

    /// Distribute a total exactly, giving the first remainder readers one more.
    fn records_for(&self, total: u64, connection: usize) -> u64 {
        let connections = self.connections as u64;
        total / connections + u64::from((connection as u64) < total % connections)
    }
}

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        print!("{HELP}");
        return Ok(());
    }
    // cargo test --all-targets can select a harness=false benchmark despite
    // test=false. It does not supply Cargo's benchmark-mode flag. Check before
    // parsing workload options, allocating input, creating a runtime, or doing I/O.
    if !args.iter().any(|arg| arg == "--bench") || args.iter().any(|arg| arg == "--test") {
        println!(
            "Socket benchmark skipped; run cargo bench -p transaction-log-exports --bench record_reader"
        );
        return Ok(());
    }
    let config = Config::parse(&args)?;
    ensure!(
        !cfg!(debug_assertions),
        "use cargo bench with the optimized bench profile"
    );
    let batch = make_batch(&config)?;

    println!(
        "RecordReader TCP loopback | {}-{} | logical CPUs={}",
        std::env::consts::ARCH,
        std::env::consts::OS,
        thread::available_parallelism()?.get()
    );
    println!(
        "records={} connections={} payload_bytes={} record_bytes={} total_bytes={} batch_records={} batch_bytes={} retain_records={} warmup_records={} runs={}",
        config.records,
        config.connections,
        config.payload_bytes,
        config.record_len(),
        config.expected_bytes(config.records)?,
        config.batch_records,
        batch.len(),
        config.retain_records,
        config.warmup_records,
        config.runs
    );
    println!(
        "{} blocking sender threads; {} Tokio reader threads; TCP_NODELAY; default OS socket buffers.",
        config.connections, config.connections
    );
    println!(
        "Timer: common worker release through the last reader's clean EOF. Input generation and connection setup excluded."
    );

    if config.warmup_records > 0 {
        println!("Warming up with {} records...", config.warmup_records);
        transfer(&config, &batch, config.warmup_records)?;
    }
    for run in 1..=config.runs {
        println!("Starting measured run {run}/{}...", config.runs);
        let measurements = transfer(&config, &batch, config.records)?;
        let mut aggregate = Measurement::default();
        for (connection, measurement) in measurements.iter().enumerate() {
            if config.connections > 1 {
                measurement.report(run, Some(connection + 1), &config);
            }
            // Every reader uses the same start Instant. Aggregate throughput is
            // total work / last completion, not a sum of per-reader rates.
            aggregate.elapsed = aggregate.elapsed.max(measurement.elapsed);
            aggregate.sender_elapsed = aggregate.sender_elapsed.max(measurement.sender_elapsed);
            aggregate.records += measurement.records;
            aggregate.bytes += measurement.bytes;
            aggregate.batches += measurement.batches;
        }
        ensure!(
            aggregate.records == config.records,
            "aggregate record count mismatch"
        );
        ensure!(
            aggregate.bytes == config.expected_bytes(config.records)?,
            "aggregate byte count mismatch"
        );
        aggregate.report(run, None, &config);
    }
    Ok(())
}

/// Prepare only one bounded batch. Replaying it avoids timing sender-side CRC
/// generation or allocating the whole workload. IDs repeat between batches;
/// this measures the generic reader, which does not enforce sequence continuity.
fn make_batch(config: &Config) -> Result<Arc<[u8]>> {
    let capacity = config
        .batch_records
        .checked_mul(config.record_len())
        .context("sender batch size overflow")?;
    let mut batch = Vec::new();
    batch
        .try_reserve_exact(capacity)
        .context("allocating sender batch")?;
    let length = u16::try_from(config.record_len())?;
    for index in 0..config.batch_records {
        let start = batch.len();
        // Benchmark-local encoding until a writer exists. Use the public wire
        // contract, never private access or a native RecordHeader memory cast.
        batch.extend_from_slice(&length.to_le_bytes());
        batch.extend_from_slice(&((index % StreamId::COUNT) as u16).to_le_bytes());
        batch.extend_from_slice(&((index / StreamId::COUNT) as u64).to_le_bytes());
        for offset in 0..config.payload_bytes {
            batch.push(index.wrapping_add(offset) as u8);
        }
        let crc = crc32c::crc32c(&batch[start..]);
        batch.extend_from_slice(&crc.to_le_bytes());
    }
    Ok(batch.into())
}

#[derive(Default)]
struct Measurement {
    elapsed: Duration,
    sender_elapsed: Duration,
    records: u64,
    bytes: u64,
    batches: u64,
}

impl Measurement {
    fn report(&self, run: usize, connection: Option<usize>, config: &Config) {
        let seconds = self.elapsed.as_secs_f64();
        let records_per_second = self.records as f64 / seconds;
        let label = match connection {
            Some(connection) => format!("CONNECTION run={run} connection={connection}"),
            None => format!("RESULT run={run} connections={}", config.connections),
        };
        println!(
            "{label} records={} bytes={} read_seconds={seconds:.6} records_per_second={records_per_second:.0} million_records_per_minute={:.3} mib_per_second={:.3} ns_per_record={:.2} batches={} records_per_batch={:.2} sender_seconds_max={:.6} payload_bytes={} batch_records={} retain_records={}",
            self.records,
            self.bytes,
            records_per_second * 60.0 / 1_000_000.0,
            self.bytes as f64 / seconds / 1_048_576.0,
            seconds * 1_000_000_000.0 / self.records as f64,
            self.batches,
            self.records as f64 / self.batches as f64,
            self.sender_elapsed.as_secs_f64(),
            config.payload_bytes,
            config.batch_records,
            config.retain_records
        );
    }
}

/// Resources are prepared before worker creation and timing. Each reader owns
/// a separate current-thread runtime; readers never share an executor or buffer.
struct PreparedReader {
    runtime: Runtime,
    reader: RecordReader<tokio::net::TcpStream>,
    retained: VecDeque<Record>,
}

impl PreparedReader {
    fn connect(retain_records: usize) -> Result<(TcpStream, Self)> {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?;
        let sender = TcpStream::connect(listener.local_addr()?)?;
        let (receiver, _) = listener.accept()?;
        sender.set_nodelay(true)?;
        receiver.set_nodelay(true)?;
        receiver.set_nonblocking(true)?;
        let runtime = Builder::new_current_thread().enable_io().build()?;
        let reader = {
            let _entered = runtime.enter();
            RecordReader::new(tokio::net::TcpStream::from_std(receiver)?)
        };
        let mut retained = VecDeque::new();
        retained
            .try_reserve(retain_records)
            .context("allocating retention window")?;
        Ok((
            sender,
            Self {
                runtime,
                reader,
                retained,
            },
        ))
    }

    fn receive(mut self, start: Instant, retain_records: usize) -> Result<Measurement> {
        // Monomorphized consumers keep the default free of a per-record
        // retention-mode branch or deque operations.
        let received = if retain_records == 0 {
            self.runtime.block_on(drain(&mut self.reader, drop))
        } else {
            self.runtime.block_on(drain(&mut self.reader, |record| {
                if self.retained.len() == retain_records {
                    self.retained.pop_front();
                }
                self.retained.push_back(record);
            }))
        };
        // Capture completion on this worker, before cleanup or coordinator joins.
        let elapsed = start.elapsed();
        // Closing the socket also unblocks the paired sender on validation error.
        drop(self.reader);
        let (records, bytes, batches) = received?;
        for record in &self.retained {
            black_box(record.get_header());
            black_box(record.body());
        }
        Ok(Measurement {
            elapsed,
            records,
            bytes,
            batches,
            ..Measurement::default()
        })
    }
}

/// Start channels are cancellation-safe during setup: dropping them releases
/// waiting workers with an error. A fixed-size Barrier would strand workers if
/// a later thread could not be created. These channels are used only at startup.
fn await_start(ready: mpsc::Sender<()>, start: mpsc::Receiver<Instant>) -> Result<Instant> {
    ready.send(())?;
    start.recv().context("benchmark setup cancelled")
}

fn send_records(
    mut socket: TcpStream,
    batch: &[u8],
    record_len: usize,
    records: u64,
) -> Result<Duration> {
    let start = Instant::now();
    let batch_records = (batch.len() / record_len) as u64;
    let mut remaining = records;
    while remaining > 0 {
        let count = remaining.min(batch_records);
        let end = count as usize * record_len;
        socket
            .write_all(&batch[..end])
            .context("sending record batch")?;
        remaining -= count;
    }
    socket.shutdown(Shutdown::Write)?;
    Ok(start.elapsed())
}

/// Prepare every socket, runtime and retention window, wait for all 2*N workers
/// to be ready, then release them with one shared start Instant. The coordinator
/// only joins workers during the transfer: no global per-record synchronization.
fn transfer(config: &Config, batch: &Arc<[u8]>, records: u64) -> Result<Vec<Measurement>> {
    let connections: Vec<_> = (0..config.connections)
        .map(|_| PreparedReader::connect(config.retain_records))
        .collect::<Result<_>>()?;
    thread::scope(|scope| {
        let (ready_tx, ready_rx) = mpsc::channel();
        let mut starts = Vec::new();
        let mut workers = Vec::new();
        for (connection, (sender_socket, reader)) in connections.into_iter().enumerate() {
            let expected = config.records_for(records, connection);
            let (start_tx, start_rx) = mpsc::channel();
            starts.push(start_tx);
            let ready = ready_tx.clone();
            let sender = thread::Builder::new()
                .name(format!("record-bench-sender-{connection}"))
                .spawn_scoped(scope, move || {
                    await_start(ready, start_rx)?;
                    send_records(sender_socket, batch, config.record_len(), expected)
                })?;
            let (start_tx, start_rx) = mpsc::channel();
            starts.push(start_tx);
            let ready = ready_tx.clone();
            let receiver = thread::Builder::new()
                .name(format!("record-bench-reader-{connection}"))
                .spawn_scoped(scope, move || {
                    let start = await_start(ready, start_rx)?;
                    reader.receive(start, config.retain_records)
                })?;
            workers.push((expected, sender, receiver));
        }
        drop(ready_tx);
        for _ in 0..starts.len() {
            ready_rx
                .recv()
                .context("worker exited before becoming ready")?;
        }
        let start = Instant::now();
        for worker in &starts {
            worker.send(start).context("starting worker")?;
        }
        let mut measurements = Vec::new();
        for (connection, (expected, sender, receiver)) in workers.into_iter().enumerate() {
            // Both workers are joined before propagating either returned error.
            let sent = sender
                .join()
                .map_err(|_| anyhow::anyhow!("sender {connection} panicked"))?;
            let received = receiver
                .join()
                .map_err(|_| anyhow::anyhow!("reader {connection} panicked"))?;
            let mut measurement =
                received.with_context(|| format!("reading connection {connection}"))?;
            measurement.sender_elapsed = sent?;
            ensure!(
                measurement.records == expected,
                "connection {connection}: expected {expected} records, received {}",
                measurement.records
            );
            ensure!(
                measurement.bytes == config.expected_bytes(expected)?,
                "connection {connection}: received an unexpected byte count: {}",
                measurement.bytes
            );
            measurements.push(measurement);
        }
        Ok(measurements)
    })
}

/// Exercise the actual public API, including validation on every batch and
/// constructing/consuming every Record. Counters and black_box are measured;
/// there are no per-record clocks, output calls, or payload copies here.
async fn drain(
    source: &mut RecordReader<tokio::net::TcpStream>,
    mut consume: impl FnMut(Record),
) -> Result<(u64, u64, u64)> {
    let mut records = 0;
    let mut bytes = 0;
    let mut batches = 0;
    while source.wait_to_read().await? {
        batches += 1;
        while let Some(record) = source.try_read_next()? {
            records += 1;
            bytes += u64::from(record.length());
            consume(black_box(record));
        }
    }
    Ok((records, bytes, batches))
}
