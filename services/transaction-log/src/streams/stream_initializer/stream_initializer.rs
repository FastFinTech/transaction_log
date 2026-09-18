use anyhow::{Result, ensure};
use tokio::fs::File;
use transaction_log_exports::StreamId;

use crate::storage::StorageProvider;
use crate::streams::{IndexedLogWriter, StreamCheckpoint};

/// Scaffold for startup recovery of one stream.
///
/// Checkpoint loading is implemented; subsequent recovery and writer handover
/// remain placeholders.
pub struct StreamInitializer<'a> {
    provider: &'a StorageProvider,
    stream_id: StreamId,
    // Set only after a successful load and stream-identity check.
    checkpoint: Option<StreamCheckpoint>,
}

impl<'a> StreamInitializer<'a> {
    /// Selects one stream and borrows its storage provider without performing I/O.
    pub fn new(provider: &'a StorageProvider, stream_id: StreamId) -> Self {
        Self {
            provider,
            stream_id,
            checkpoint: None,
        }
    }

    /// Runs the five recovery steps for one stream and returns its active writer.
    ///
    /// Loads the checkpoint; the remaining steps are placeholders.
    ///
    /// # Panics
    ///
    /// Currently panics at file discovery after successful checkpoint loading.
    pub async fn initialize(&mut self) -> Result<IndexedLogWriter<File>> {
        self.load_checkpoint().await?;
        self.discover_log_files().await?;
        self.validate_and_repair().await?;
        self.advance_checkpoint().await?;
        self.prepare_active_writer().await
    }

    /// Load the checkpoint belonging to this stream, or establish its absence.
    async fn load_checkpoint(&mut self) -> Result<()> {
        let checkpoint = self.provider.read_checkpoint(self.stream_id).await?;
        if let Some(checkpoint) = checkpoint {
            ensure!(
                checkpoint.end().record_id().stream_id() == self.stream_id,
                "checkpoint at {} belongs to stream {}, expected {}",
                self.provider.checkpoint_file_path(self.stream_id).display(),
                checkpoint.end().record_id().stream_id(),
                self.stream_id,
            );
        }
        self.checkpoint = checkpoint;
        Ok(())
    }

    /// Discover consecutive log files starting at the recovery boundary.
    async fn discover_log_files(&mut self) -> Result<()> {
        todo!("discover this stream's log files")
    }

    /// Validate logs, truncate corrupt tails, discard files beyond a gap or
    /// incomplete file, and repair corresponding indexes.
    async fn validate_and_repair(&mut self) -> Result<()> {
        todo!("validate and repair this stream's file pairs")
    }

    /// Synchronize recovered pairs before publishing an advanced checkpoint.
    async fn advance_checkpoint(&mut self) -> Result<()> {
        todo!("synchronize recovered data and advance the checkpoint")
    }

    /// Hand over a partial pair or create a fresh pair after a full or empty stream.
    async fn prepare_active_writer(&mut self) -> Result<IndexedLogWriter<File>> {
        todo!("return this stream's active writer")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{StorageConfig, StorageError};

    #[tokio::test]
    async fn loads_only_the_requested_stream_and_preserves_the_boundary() {
        let directory = tempfile::tempdir().unwrap();
        let provider = StorageProvider::new(StorageConfig::new(directory.path().into()).unwrap());
        let stream = StreamId::new(42).unwrap();
        let path = provider.checkpoint_file_path(stream);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let json = r#"{"end":{"record_id":{"stream_id":42,"sequence_number":99999},"position":6553500000}}"#;
        std::fs::write(&path, json).unwrap();
        let mut initializer = StreamInitializer::new(&provider, stream);
        initializer.load_checkpoint().await.unwrap();
        let checkpoint = initializer.checkpoint.unwrap();
        assert_eq!(checkpoint.end().record_id().sequence_number().get(), 99_999);
        assert_eq!(checkpoint.end().position(), 6_553_500_000);
        assert_eq!(std::fs::read_to_string(path).unwrap(), json);

        let mut other = StreamInitializer::new(&provider, StreamId::MIN);
        other.load_checkpoint().await.unwrap();
        assert_eq!(other.checkpoint, None);
    }

    #[tokio::test]
    async fn absence_is_not_a_sequence_zero_sentinel_and_creates_nothing() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("absent");
        let provider = StorageProvider::new(StorageConfig::new(root.clone()).unwrap());
        let mut initializer = StreamInitializer::new(&provider, StreamId::MIN);
        initializer.load_checkpoint().await.unwrap();
        assert_eq!(initializer.checkpoint, None);
        assert!(!root.exists());
    }

    #[tokio::test]
    async fn rejects_wrong_stream_and_malformed_metadata_without_accepting_or_changing_it() {
        let directory = tempfile::tempdir().unwrap();
        let provider = StorageProvider::new(StorageConfig::new(directory.path().into()).unwrap());
        let path = provider.checkpoint_file_path(StreamId::MIN);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut initializer = StreamInitializer::new(&provider, StreamId::MIN);
        let valid = r#"{"end":{"record_id":{"stream_id":0,"sequence_number":0},"position":16}}"#;
        std::fs::write(&path, valid).unwrap();
        initializer.load_checkpoint().await.unwrap();
        let previous = initializer.checkpoint;
        assert_eq!(
            previous.unwrap().end().record_id().sequence_number().get(),
            0
        );
        let json = r#"{"end":{"record_id":{"stream_id":1,"sequence_number":0},"position":16}}"#;
        std::fs::write(&path, json).unwrap();
        let error = initializer.load_checkpoint().await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("belongs to stream 1, expected 0")
        );
        assert_eq!(initializer.checkpoint, previous);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), json);

        std::fs::write(&path, "{}").unwrap();
        let error = initializer.load_checkpoint().await.unwrap_err();
        assert!(
            matches!(error.downcast_ref::<StorageError>(), Some(StorageError::Json { path: failed, .. }) if failed == &path)
        );
        assert_eq!(initializer.checkpoint, previous);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "{}");
    }
}
