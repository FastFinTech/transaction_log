/// Which bytes to read from the file paired with this value by a range iterator.
///
/// [`super::RecordRangeLocation::iter`] returns `(LogFileId, LogFileRange)` items.
/// Explicit starts are inclusive and explicit ends exclusive, measured in bytes
/// from the beginning of that log file. Zero is the implicit file start.
///
/// A postfix or entire file ends at the complete, validated end of the file's
/// assigned records. The future reader must resolve that boundary from trusted
/// file/index metadata. It must not substitute a guessed maximum byte size or
/// an unvalidated physical EOF. This enum performs no file I/O or validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFileRange {
    /// Both boundaries are known because the requested records share one file.
    FileRange {
        /// Inclusive offset of the first requested record.
        start_position: u64,
        /// Exclusive offset immediately after the last requested record's CRC.
        end_position: u64,
    },
    /// First file of a multi-file range, from the supplied start through its end.
    FilePostfix {
        /// Inclusive offset of the first requested record.
        start_position: u64,
    },
    /// Intermediate file, from byte zero through its validated record end.
    EntireFile,
    /// Last file of a multi-file range, from byte zero through the supplied end.
    FilePrefix {
        /// Exclusive offset immediately after the last requested record's CRC.
        end_position: u64,
    },
}
