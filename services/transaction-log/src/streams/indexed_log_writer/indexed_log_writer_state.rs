/// Local permission to perform another append or output operation.
pub(super) enum IndexedLogWriterState {
    /// Accepting records and owner-driven output operations.
    Open,
    /// Output failed, panicked or was cancelled; the pair requires recovery.
    Unusable,
    /// All assigned records are durable and appending has ended.
    Finalized,
}
