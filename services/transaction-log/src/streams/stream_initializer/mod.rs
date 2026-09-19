#![doc = include_str!("README.md")]

mod initialized_stream;
#[allow(clippy::module_inception)] // Keep StreamInitializer in its own type-named file.
mod stream_initializer;

pub use initialized_stream::InitializedStream;
pub use stream_initializer::StreamInitializer;
