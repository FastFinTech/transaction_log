//! Shared implementation of the explicit writer and combined TCP benchmarks.
//! This module belongs only to benchmark binaries, never the production library.
//! Its maintained specification is in README.md.

use std::{
    collections::VecDeque,
    hint::black_box,
    io::{self, Read, Write},
    net::{Ipv4Addr, TcpListener, TcpStream},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use tokio::{io::AsyncWriteExt, runtime::Builder};
use transaction_log_exports::{
    Record, RecordId, RecordReader, RecordWriter, SequenceNumber, StreamId,
    record::record_protocol as protocol,
};

/// Selected once by the executable, not on each record in a measured loop.
#[derive(Clone, Copy)]
#[allow(dead_code)] // Each benchmark binary selects one variant of this shared enum.
pub enum ReceiverKind {
    Bytes,
    Records,
}

impl ReceiverKind {
    fn target(self) -> &'static str {
        match self {
            Self::Bytes => "record_writer",
            Self::Records => "record_io",
        }
    }
}

#[derive(Clone, Copy)]
enum Workload {
    Serialize,
    Copy,
}

impl Workload {
    fn name(self) -> &'static str {
        match self {
            Self::Serialize => "serialize",
            Self::Copy => "copy",
        }
    }
}

struct Config {
    records: u64,
    connections: usize,
    payload_bytes: usize,
    batch_records: usize,
    write_chunk_bytes: usize,
    retain_records: usize,
    warmup_records: u64,
    runs: usize,
    workload: Workload,
}

impl Config {
    fn parse(args: &[String], receiver: ReceiverKind) -> Result<Self> {
        let chunk_size_supplied = args.iter().any(|arg| arg == "--write-chunk-bytes");
        let mut config = Self {
            records: 100_000_000,
            connections: 1,
            payload_bytes: 2048,
            batch_records: 8192,
            write_chunk_bytes: 8,
            retain_records: 0,
            warmup_records: 1_000_000,
            runs: 1,
            workload: Workload::Serialize,
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
                "--write-chunk-bytes" => {
                    config.write_chunk_bytes =
                        value.parse().context("invalid --write-chunk-bytes")?
                }
                "--retain-records" => {
                    config.retain_records = value.parse().context("invalid --retain-records")?
                }
                "--warmup-records" => {
                    config.warmup_records = value.parse().context("invalid --warmup-records")?
                }
                "--runs" => config.runs = value.parse().context("invalid --runs")?,
                "--workload" => {
                    config.workload = match value.as_str() {
                        "serialize" => Workload::Serialize,
                        "copy" => Workload::Copy,
                        _ => bail!("--workload must be serialize or copy"),
                    }
                }
                _ => bail!("unknown option {flag}; use --help"),
            }
        }
        ensure!(config.connections > 0, "--connections must be positive");
        config
            .connections
            .checked_mul(2)
            .context("worker count overflow")?;
        ensure!(
            config.records >= config.connections as u64,
            "--records must be at least --connections"
        );
        ensure!(
            config.warmup_records == 0 || config.warmup_records >= config.connections as u64,
            "--warmup-records must be zero or at least --connections"
        );
        ensure!(config.batch_records > 0, "--batch-records must be positive");
        ensure!(
            config.write_chunk_bytes > 0,
            "--write-chunk-bytes must be positive"
        );
        ensure!(config.runs > 0, "--runs must be positive");
        ensure!(
            config.payload_bytes <= protocol::MAX_PAYLOAD_LEN,
            "payload exceeds protocol maximum {}",
            protocol::MAX_PAYLOAD_LEN
        );
        ensure!(
            matches!(receiver, ReceiverKind::Records) || config.retain_records == 0,
            "--retain-records requires the record_io benchmark"
        );
        ensure!(
            matches!(config.workload, Workload::Serialize) || !chunk_size_supplied,
            "--write-chunk-bytes applies only to serialization"
        );
        config
            .batch_records
            .checked_mul(config.record_len())
            .context("batch byte count overflow")?;
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

    fn records_for(&self, total: u64, connection: usize) -> u64 {
        let count = self.connections as u64;
        total / count + u64::from((connection as u64) < total % count)
    }
}

/// All payload/fixture construction is outside the measured interval. A copy
/// fixture is validated once so the writer uses only the public Record API.
struct Fixtures {
    payload: Vec<u8>,
    records: Vec<Record>,
}

impl Fixtures {
    fn new(config: &Config) -> Result<Self> {
        let payload = (0..config.payload_bytes).map(|index| index as u8).collect();
        let mut fixtures = Self {
            payload,
            records: Vec::new(),
        };
        if matches!(config.workload, Workload::Copy) {
            let mut encoded = Vec::new();
            encoded.try_reserve_exact(config.batch_records * config.record_len())?;
            for index in 0..config.batch_records {
                let start = encoded.len();
                encoded.extend_from_slice(&(config.record_len() as u16).to_le_bytes());
                encoded.extend_from_slice(&((index % StreamId::COUNT) as u16).to_le_bytes());
                encoded.extend_from_slice(&((index / StreamId::COUNT) as u64).to_le_bytes());
                encoded.extend_from_slice(&fixtures.payload);
                let crc = crc32c::crc32c(&encoded[start..]);
                encoded.extend_from_slice(&crc.to_le_bytes());
            }
            fixtures.records.try_reserve_exact(config.batch_records)?;
            let runtime = Builder::new_current_thread().build()?;
            runtime.block_on(async {
                let mut reader = RecordReader::new(encoded.as_slice());
                while reader.wait_to_read().await? {
                    while let Some(record) = reader.try_read_next()? {
                        fixtures.records.push(record);
                    }
                }
                Ok::<_, anyhow::Error>(())
            })?;
            ensure!(
                fixtures.records.len() == config.batch_records,
                "copy fixture count mismatch"
            );
        }
        Ok(fixtures)
    }
}

#[derive(Default)]
struct Measurement {
    sender_elapsed: Duration,
    receiver_elapsed: Duration,
    records: u64,
    bytes: u64,
    sent_batches: u64,
    receive_batches: u64,
}

impl Measurement {
    fn report(
        &self,
        config: &Config,
        receiver: ReceiverKind,
        run: usize,
        connection: Option<usize>,
    ) {
        // Receiver EOF establishes complete delivery. Include sender completion
        // too: the sender may be descheduled after shutdown before timestamping.
        let seconds = self.sender_elapsed.max(self.receiver_elapsed).as_secs_f64();
        let label = match connection {
            Some(connection) => format!("CONNECTION run={run} connection={connection}"),
            None => format!("RESULT run={run} connections={}", config.connections),
        };
        println!(
            "{label} benchmark={} workload={} records={} bytes={} elapsed_seconds={seconds:.6} records_per_second={:.0} million_records_per_minute={:.3} mib_per_second={:.3} ns_per_record={:.2} sender_seconds_max={:.6} receiver_seconds_max={:.6} sent_batches={} receive_batches={} payload_bytes={} batch_records={} write_chunk_bytes={} retain_records={}",
            receiver.target(),
            config.workload.name(),
            self.records,
            self.bytes,
            self.records as f64 / seconds,
            self.records as f64 * 60.0 / seconds / 1_000_000.0,
            self.bytes as f64 / seconds / 1_048_576.0,
            seconds * 1_000_000_000.0 / self.records as f64,
            self.sender_elapsed.as_secs_f64(),
            self.receiver_elapsed.as_secs_f64(),
            self.sent_batches,
            self.receive_batches,
            config.payload_bytes,
            config.batch_records,
            config.write_chunk_bytes,
            config.retain_records,
        );
    }
}

/// Startup channels are used once per worker. If any setup fails, dropping the
/// senders releases already-ready workers instead of stranding a fixed barrier.
fn await_start(ready: mpsc::Sender<()>, start: mpsc::Receiver<Instant>) -> Result<Instant> {
    ready.send(()).context("coordinator exited during setup")?;
    // Do not retain a ready sender while waiting. If another worker fails setup,
    // the coordinator must observe disconnection instead of waiting forever.
    drop(ready);
    start.recv().context("benchmark setup cancelled")
}

/// One generic loop shares batching/output policy without erasing the concrete
/// writer mode or callback. Selection happens before the worker becomes ready.
async fn send<Mode>(
    mut writer: RecordWriter<tokio::net::TcpStream, Mode>,
    config: &Config,
    count: u64,
    mut append: impl FnMut(&mut RecordWriter<tokio::net::TcpStream, Mode>, u64) -> Result<()>,
) -> Result<u64> {
    let mut written = 0;
    let mut batches = 0;
    while written < count {
        let batch = (count - written).min(config.batch_records as u64);
        for index in written..written + batch {
            append(&mut writer, index)?;
        }
        writer.flush_buffer().await?;
        written += batch;
        batches += 1;
    }
    writer.flush().await?;
    // Finish the write half explicitly so the receiver verifies clean EOF. This
    // is transport completion, not application acknowledgement or durable sync.
    writer.into_inner().shutdown().await?;
    Ok(batches)
}

fn produce(
    socket: TcpStream,
    config: &Config,
    fixtures: &Fixtures,
    count: u64,
    ready: mpsc::Sender<()>,
    start: mpsc::Receiver<Instant>,
) -> Result<(Duration, u64)> {
    let runtime = Builder::new_current_thread().enable_io().build()?;
    socket.set_nonblocking(true)?;
    let socket = {
        let _entered = runtime.enter();
        tokio::net::TcpStream::from_std(socket)?
    };
    match config.workload {
        Workload::Serialize => {
            let writer = RecordWriter::for_serialization(socket);
            // Validate stream IDs once, outside timing. Timed work includes ID
            // selection/sequence generation but does not revalidate stream IDs.
            let streams = (0..StreamId::COUNT)
                .map(|index| StreamId::new(index as u16).unwrap())
                .collect::<Vec<_>>();
            let start = await_start(ready, start)?;
            let batches = runtime.block_on(send(writer, config, count, |writer, index| {
                let id = RecordId::new(
                    streams[(index % StreamId::COUNT as u64) as usize],
                    SequenceNumber::new(index / StreamId::COUNT as u64),
                );
                writer.write(id, |body| {
                    for field in fixtures.payload.chunks(config.write_chunk_bytes) {
                        body.write_all(field)?;
                    }
                    Ok::<_, io::Error>(())
                })?;
                Ok(())
            }))?;
            Ok((start.elapsed(), batches))
        }
        Workload::Copy => {
            let writer = RecordWriter::for_records(socket);
            let start = await_start(ready, start)?;
            let batches = runtime.block_on(send(writer, config, count, |writer, index| {
                writer.write_record(
                    &fixtures.records[(index % fixtures.records.len() as u64) as usize],
                )?;
                Ok(())
            }))?;
            Ok((start.elapsed(), batches))
        }
    }
}

async fn drain_records(
    reader: &mut RecordReader<tokio::net::TcpStream>,
    mut consume: impl FnMut(Record),
) -> Result<(u64, u64, u64)> {
    let mut records = 0;
    let mut bytes = 0;
    let mut batches = 0;
    while reader.wait_to_read().await? {
        batches += 1;
        while let Some(record) = reader.try_read_next()? {
            records += 1;
            bytes += u64::from(record.length());
            consume(black_box(record));
        }
    }
    Ok((records, bytes, batches))
}

fn receive(
    mut socket: TcpStream,
    config: &Config,
    receiver: ReceiverKind,
    ready: mpsc::Sender<()>,
    start: mpsc::Receiver<Instant>,
) -> Result<Measurement> {
    match receiver {
        ReceiverKind::Bytes => {
            // A fixed raw byte drain excludes RecordReader and CRC work from the
            // writer benchmark. Count bytes; never pretend to have parsed records.
            let mut buffer = vec![0_u8; 256 * 1024];
            let start = await_start(ready, start)?;
            let mut measurement = Measurement::default();
            loop {
                match socket.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(read) => {
                        measurement.bytes += read as u64;
                        measurement.receive_batches += 1;
                    }
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(error) => return Err(error.into()),
                }
            }
            measurement.receiver_elapsed = start.elapsed();
            Ok(measurement)
        }
        ReceiverKind::Records => {
            let runtime = Builder::new_current_thread().enable_io().build()?;
            socket.set_nonblocking(true)?;
            let mut reader = {
                let _entered = runtime.enter();
                RecordReader::new(tokio::net::TcpStream::from_std(socket)?)
            };
            let mut retained = VecDeque::new();
            retained.try_reserve(config.retain_records)?;
            let start = await_start(ready, start)?;
            // Select the consumption loop once, matching the reader-only harness.
            let result = if config.retain_records == 0 {
                runtime.block_on(drain_records(&mut reader, drop))
            } else {
                runtime.block_on(drain_records(&mut reader, |record| {
                    if retained.len() == config.retain_records {
                        retained.pop_front();
                    }
                    retained.push_back(record);
                }))
            };
            let elapsed = start.elapsed();
            drop(reader); // Also closes the connection on validation failure.
            let (records, bytes, batches) = result?;
            for record in &retained {
                black_box(record.get_header());
                black_box(record.body());
            }
            Ok(Measurement {
                receiver_elapsed: elapsed,
                records,
                bytes,
                receive_batches: batches,
                ..Measurement::default()
            })
        }
    }
}

fn transfer(
    config: &Config,
    fixtures: &Fixtures,
    receiver: ReceiverKind,
    count: u64,
) -> Result<Vec<Measurement>> {
    let sockets = (0..config.connections)
        .map(|_| {
            let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
            let sender = TcpStream::connect(listener.local_addr()?)?;
            let (receiver, _) = listener.accept()?;
            sender.set_nodelay(true)?;
            receiver.set_nodelay(true)?;
            Ok((sender, receiver))
        })
        .collect::<Result<Vec<_>>>()?;

    thread::scope(|scope| {
        let (ready_tx, ready_rx) = mpsc::channel();
        let mut starts = Vec::new();
        let mut workers = Vec::new();
        for (connection, (sender_socket, receiver_socket)) in sockets.into_iter().enumerate() {
            let expected = config.records_for(count, connection);
            let (start_tx, start_rx) = mpsc::channel();
            starts.push(start_tx);
            let ready = ready_tx.clone();
            let sender = thread::Builder::new()
                .name(format!("record-writer-{connection}"))
                .spawn_scoped(scope, move || {
                    produce(sender_socket, config, fixtures, expected, ready, start_rx)
                })?;
            let (start_tx, start_rx) = mpsc::channel();
            starts.push(start_tx);
            let ready = ready_tx.clone();
            let reader = thread::Builder::new()
                .name(format!("record-receiver-{connection}"))
                .spawn_scoped(scope, move || {
                    receive(receiver_socket, config, receiver, ready, start_rx)
                })?;
            workers.push((expected, sender, reader));
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
        for (connection, (expected, sender, reader)) in workers.into_iter().enumerate() {
            // Join both before returning a worker error; the peer's socket closes
            // on exit, releasing pending socket I/O rather than masking a failure.
            let sent = sender
                .join()
                .map_err(|_| anyhow::anyhow!("sender {connection} panicked"));
            let received = reader
                .join()
                .map_err(|_| anyhow::anyhow!("receiver {connection} panicked"));
            let (sender_elapsed, sent_batches) = sent??;
            let mut measurement = received??;
            ensure!(
                measurement.bytes == config.expected_bytes(expected)?,
                "connection {connection}: byte count mismatch"
            );
            if matches!(receiver, ReceiverKind::Records) {
                ensure!(
                    measurement.records == expected,
                    "connection {connection}: record count mismatch"
                );
            }
            ensure!(
                sent_batches == expected.div_ceil(config.batch_records as u64),
                "connection {connection}: sender batch count mismatch"
            );
            // In byte-drain mode the completed producer supplies record count;
            // the independent byte count/EOF are what the receiver verifies.
            measurement.records = expected;
            measurement.sender_elapsed = sender_elapsed;
            measurement.sent_batches = sent_batches;
            measurements.push(measurement);
        }
        Ok(measurements)
    })
}

pub fn run(receiver: ReceiverKind) -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let target = receiver.target();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        println!(
            "{target}: explicit TCP loopback throughput benchmark\n\ncargo bench -p transaction-log-exports --bench {target} -- [options]\n\n  --records N          Total measured records (default 100000000)\n  --connections N      Sender/receiver pairs (default 1)\n  --payload-bytes N    Payload size, 0..=65519 (default 2048)\n  --batch-records N    Records per flush_buffer (default 8192)\n  --workload MODE      serialize (default) or copy\n  --write-chunk-bytes N  Bytes per serialization Write call (default 8)\n  --retain-records N   Retained records per reader, record_io only (default 0)\n  --warmup-records N   Separate warmup transfer, 0 disables (default 1000000)\n  --runs N             Measured transfers (default 1)\n\nWithout Cargo's --bench flag, or with --test, execution is skipped."
        );
        return Ok(());
    }
    // Keep this before parsing, allocations and networking: cargo test with
    // --all-targets selects harness=false binaries despite test=false.
    if !args.iter().any(|arg| arg == "--bench") || args.iter().any(|arg| arg == "--test") {
        println!("{target} skipped; run cargo bench -p transaction-log-exports --bench {target}");
        return Ok(());
    }
    let config = Config::parse(&args, receiver)?;
    ensure!(
        !cfg!(debug_assertions),
        "use cargo bench with the optimized bench profile"
    );
    let fixtures = Fixtures::new(&config)?;
    crate::environment::print();
    println!(
        "{target} TCP loopback | {}-{} | logical CPUs={}",
        std::env::consts::ARCH,
        std::env::consts::OS,
        thread::available_parallelism()?
    );
    println!(
        "records={} connections={} payload_bytes={} record_bytes={} total_bytes={} batch_records={} workload={} write_chunk_bytes={} retain_records={} warmup_records={} runs={}",
        config.records,
        config.connections,
        config.payload_bytes,
        config.record_len(),
        config.expected_bytes(config.records)?,
        config.batch_records,
        config.workload.name(),
        config.write_chunk_bytes,
        config.retain_records,
        config.warmup_records,
        config.runs
    );
    println!(
        "{} sender threads/runtimes; {} receiver threads; TCP_NODELAY; default OS socket buffers.",
        config.connections, config.connections
    );
    println!(
        "Timer: common worker release through last sender/receiver completion. Setup/fixture generation excluded; writer encoding/copying, buffer growth, CRC (serialize), transport and receiver work included."
    );
    if config.warmup_records > 0 {
        println!("Warming up with {} records...", config.warmup_records);
        transfer(&config, &fixtures, receiver, config.warmup_records)?;
    }
    for run in 1..=config.runs {
        println!("Starting measured run {run}/{}...", config.runs);
        let mut aggregate = Measurement::default();
        for (connection, measurement) in transfer(&config, &fixtures, receiver, config.records)?
            .into_iter()
            .enumerate()
        {
            if config.connections > 1 {
                measurement.report(&config, receiver, run, Some(connection + 1));
            }
            aggregate.sender_elapsed = aggregate.sender_elapsed.max(measurement.sender_elapsed);
            aggregate.receiver_elapsed =
                aggregate.receiver_elapsed.max(measurement.receiver_elapsed);
            aggregate.records += measurement.records;
            aggregate.bytes += measurement.bytes;
            aggregate.sent_batches += measurement.sent_batches;
            aggregate.receive_batches += measurement.receive_batches;
        }
        ensure!(
            aggregate.records == config.records
                && aggregate.bytes == config.expected_bytes(config.records)?,
            "aggregate count mismatch"
        );
        aggregate.report(&config, receiver, run, None);
    }
    Ok(())
}
