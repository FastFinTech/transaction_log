//! Shared filesystem fixtures for service tests. See the storage README.

use std::{
    fs,
    io::{self, Write},
    path::Path,
    sync::atomic::{AtomicU16, Ordering},
};

use tokio::sync::OnceCell;
use transaction_log_exports::StreamId;

use super::{StorageConfig, StorageError, StorageProvider};

/// Owns one test stream's contents, keeping its base directory on drop.
/// Declare before file handles and await all I/O before leaving its scope.
#[derive(getset::CopyGetters)]
#[getset(get_copy = "pub(crate)")]
#[must_use = "keep the fixture alive until its files and I/O are finished"]
pub(crate) struct StorageFixture {
    /// Shared provider; the guard owns only the stream identified below.
    provider: &'static StorageProvider,
    /// Unique stream whose files and nested directories this guard removes.
    stream_id: StreamId,
}

impl Drop for StorageFixture {
    fn drop(&mut self) {
        let checkpoint = self.provider.checkpoint_file_path(self.stream_id);
        let directory = checkpoint.parent().unwrap();
        let cleanup = || -> Result<(), StorageError> {
            // Never follow a replaced base directory outside the owned stream.
            let metadata = fs::symlink_metadata(directory)
                .map_err(|error| StorageError::io(directory.to_owned(), error))?;
            if !metadata.is_dir() {
                return Err(StorageError::io(
                    directory.to_owned(),
                    io::Error::new(
                        io::ErrorKind::NotADirectory,
                        "test stream base is not a directory",
                    ),
                ));
            }
            for entry in fs::read_dir(directory)
                .map_err(|error| StorageError::io(directory.to_owned(), error))?
            {
                let entry = entry.map_err(|error| StorageError::io(directory.to_owned(), error))?;
                remove_test_path(&entry.path())?;
            }
            Ok(())
        };
        if let Err(error) = cleanup() {
            report_cleanup_error(error);
        }
    }
}

/// Claims a unique stream from the shared provider, with cleanup tied to scope.
/// Claim another fixture when a case needs additional streams; IDs are never reused.
pub(crate) async fn storage_fixture() -> StorageFixture {
    static NEXT: AtomicU16 = AtomicU16::new(0);
    let provider = shared_storage().await;
    // This counter assigns IDs only; the OnceCell publishes the provider.
    let stream_id = StreamId::new(NEXT.fetch_add(1, Ordering::Relaxed))
        .expect("storage tests exhausted the available stream IDs");
    StorageFixture {
        provider,
        stream_id,
    }
}

/// Initializes storage once. Root-wide metadata tests provide their own guard.
pub(super) async fn shared_storage() -> &'static StorageProvider {
    // Keep an OS lock for the process lifetime: separate test processes sharing
    // this build directory must not clear each other's data.
    static FIXTURE: OnceCell<(StorageProvider, fs::File)> = OnceCell::const_new();

    let fixture = FIXTURE
        .get_or_init(|| async {
            // Include spaces and Unicode so ordinary tests exercise these paths.
            let executable = std::env::current_exe().unwrap();
            let directory = executable.parent().unwrap();
            let root = directory
                .join("test-storage")
                .join("log data 東京")
                .join("replica");
            let lock_path = directory.join("test-storage.lock");
            let lock = tokio::task::spawn_blocking(move || {
                let lock = fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(lock_path)
                    .unwrap();
                lock.lock().unwrap();
                lock
            })
            .await
            .unwrap();
            let mut provider = StorageProvider::new(StorageConfig::new(root).unwrap())
                .await
                .unwrap();
            provider.clear_all().await.unwrap();
            (provider, lock)
        })
        .await;
    &fixture.0
}

/// Removes an owned test path without following links. Absence is already clean.
pub(super) fn remove_test_path(path: &Path) -> Result<(), StorageError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(StorageError::io(path.to_owned(), error)),
    };
    let result = if metadata.is_dir() {
        fs::remove_dir_all(path)
    } else {
        fs::remove_file(path)
    };
    result.map_err(|error| StorageError::io(path.to_owned(), error))
}

/// Fail a passing test, or report cleanup failure without double-panicking when
/// an assertion is already unwinding. The next process retries stale cleanup.
pub(super) fn report_cleanup_error(error: StorageError) {
    if std::thread::panicking() {
        let _ = writeln!(io::stderr().lock(), "test storage cleanup failed: {error}");
    } else {
        panic!("test storage cleanup failed: {error}");
    }
}

/// Retargets a literal record without using the production encoder. Bytes 2..4
/// hold its little-endian stream ID; the final four hold CRC-32C of the preceding
/// header and payload. Other bytes and expected lengths remain unchanged.
pub(crate) fn record_fixture(stream_id: StreamId, template: &[u8]) -> Vec<u8> {
    let mut bytes = template.to_vec();
    bytes[2..4].copy_from_slice(&stream_id.get().to_le_bytes());
    let trailer = bytes.len() - 4;
    let crc = crc32c::crc32c(&bytes[..trailer]);
    bytes[trailer..].copy_from_slice(&crc.to_le_bytes());
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_fixtures_match_independent_encodings() {
        const FIRST: &[u8] = &[16, 0, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 215, 226, 50, 73];
        const SECOND: &[u8] = &[
            19, 0, 7, 0, 1, 0, 0, 0, 0, 0, 0, 0, 97, 98, 99, 18, 247, 123, 204,
        ];
        let stream = StreamId::new(7).unwrap();
        assert_eq!(record_fixture(stream, FIRST), FIRST);
        assert_eq!(record_fixture(stream, SECOND), SECOND);
        // The high-byte ID's trailer was computed independently using the
        // reflected CRC-32C polynomial 0x82f63b78.
        assert_eq!(
            record_fixture(StreamId::new(263).unwrap(), FIRST),
            [16, 0, 7, 1, 0, 0, 0, 0, 0, 0, 0, 0, 159, 52, 12, 189]
        );
    }

    #[tokio::test]
    async fn fixture_cleanup_preserves_its_base_and_other_live_streams() {
        let first = storage_fixture().await;
        let first_path = first.provider().checkpoint_file_path(first.stream_id());
        fs::write(&first_path, b"first case").unwrap();
        let nested = first_path.parent().unwrap().join("nested/empty-directory");
        fs::create_dir_all(&nested).unwrap();
        fs::write(nested.join("log"), b"generated log").unwrap();
        let second = storage_fixture().await;
        assert_eq!(
            first.provider().root_directory(),
            second.provider().root_directory()
        );
        assert_ne!(first.stream_id(), second.stream_id());
        let second_path = second.provider().checkpoint_file_path(second.stream_id());
        assert!(!second_path.exists());
        fs::write(&second_path, b"second case").unwrap();
        assert_eq!(fs::read(&first_path).unwrap(), b"first case");
        drop(first);
        assert_eq!(
            fs::read_dir(first_path.parent().unwrap()).unwrap().count(),
            0
        );
        assert_eq!(fs::read(&second_path).unwrap(), b"second case");
        drop(second);
        assert_eq!(
            fs::read_dir(second_path.parent().unwrap()).unwrap().count(),
            0
        );
    }

    #[tokio::test]
    async fn fixture_cleanup_runs_during_assertion_unwinding() {
        let fixture = storage_fixture().await;
        let path = fixture.provider().checkpoint_file_path(fixture.stream_id());
        fs::write(&path, b"must not survive the panic").unwrap();
        let result = std::panic::catch_unwind(move || {
            let _fixture = fixture;
            panic!("original test failure");
        });
        assert_eq!(
            *result.unwrap_err().downcast::<&str>().unwrap(),
            "original test failure"
        );
        assert_eq!(fs::read_dir(path.parent().unwrap()).unwrap().count(), 0);
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn locked_file_errors_fail_normal_cleanup_without_aborting_an_existing_panic() {
        use std::os::windows::fs::OpenOptionsExt;

        for already_panicking in [false, true] {
            let fixture = storage_fixture().await;
            let path = fixture.provider().checkpoint_file_path(fixture.stream_id());
            let held = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .share_mode(0)
                .open(&path)
                .unwrap();
            let result = std::panic::catch_unwind(move || {
                let _fixture = fixture;
                if already_panicking {
                    panic!("original test failure");
                }
            });
            let error = result.unwrap_err();
            if already_panicking {
                assert_eq!(*error.downcast::<&str>().unwrap(), "original test failure");
            } else {
                assert!(
                    error
                        .downcast::<String>()
                        .unwrap()
                        .contains("test storage cleanup failed")
                );
            }
            drop(held);
            remove_test_path(&path).unwrap();
            assert_eq!(fs::read_dir(path.parent().unwrap()).unwrap().count(), 0);
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn fixture_cleanup_removes_links_without_touching_their_targets() {
        let fixture = storage_fixture().await;
        let path = fixture.provider().checkpoint_file_path(fixture.stream_id());
        let outside = tempfile::tempdir().unwrap();
        let sentinel = outside.path().join("keep");
        fs::write(&sentinel, b"outside").unwrap();
        std::os::unix::fs::symlink(outside.path(), &path).unwrap();
        drop(fixture);
        assert_eq!(fs::read(sentinel).unwrap(), b"outside");
        assert_eq!(fs::read_dir(path.parent().unwrap()).unwrap().count(), 0);
    }
}
