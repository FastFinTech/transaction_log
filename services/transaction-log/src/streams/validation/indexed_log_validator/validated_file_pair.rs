use tokio::fs::File;

use crate::streams::RecordEndLocation;

/// The outcome of successfully validating and synchronizing one log/index pair.
///
/// Endpoints refer only to the requested file. An empty partial file has no end;
/// a caller walking several files retains the preceding file's stream endpoint.
/// This outcome does not publish a checkpoint or synchronize directory entries.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)] // One recovery result at a time; keep both File handles unboxed.
pub enum ValidatedFilePair {
    /// All record positions assigned to this file are occupied; handles are closed.
    Complete {
        /// Last accepted record, covered by both synchronized files.
        end: RecordEndLocation,
    },
    /// This file has room for more records; both handles are positioned to append.
    Partial {
        /// Last accepted record, or `None` when no record remains in this file.
        end: Option<RecordEndLocation>,
        /// Recovered log, positioned at `end.position()` or zero for an empty file.
        log: File,
        /// Recovered index, positioned immediately after its last complete entry.
        index: File,
    },
}
