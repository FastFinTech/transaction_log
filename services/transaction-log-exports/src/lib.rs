//! Shared public types and APIs for the Transaction Log service.
//!
//! [`record`] defines immutable record values and the wire/file contract;
//! [`RecordReader`] validates incoming bytes before exposing them. [`record_writer`]
//! owns an output destination and buffers complete records synchronously. Its
//! constructor selects either serialization callbacks or existing-record copies;
//! the type system prevents mixing those append APIs on one writer.
//! Sending its buffer, flushing the destination and optional
//! [`AsyncSyncData`] synchronization are separate, caller-driven operations.
//! Read the module specifications before changing these ownership and validation
//! boundaries. Application scheduling, sequence acceptance and replication are
//! outside this crate's record I/O layer.

pub mod record;
pub mod record_reader;
pub mod record_writer;

pub use record::{Record, RecordHeader, RecordId, SequenceNumber, StreamId, StreamIdError};
pub use record_reader::{RecordReadError, RecordReader};
pub use record_writer::{
    AsyncSyncData, ExistingRecords, RecordBuildError, RecordBuilder, RecordOutputError,
    RecordWriteError, RecordWriter, SerializeRecords,
};
