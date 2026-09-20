#![doc = include_str!("README.md")]
#![deny(missing_docs)]

mod storage_config;
mod storage_error;
mod storage_provider;

#[cfg(test)]
pub(crate) mod test_support;

pub use storage_config::StorageConfig;
pub use storage_error::StorageError;
pub use storage_provider::StorageProvider;
