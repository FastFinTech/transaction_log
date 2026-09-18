#![doc = include_str!("README.md")]

#[allow(clippy::module_inception)] // Keep StreamInitializer in its own type-named file.
mod stream_initializer;

pub use stream_initializer::StreamInitializer;
