/// Length-prefix width in bytes: a fixed-width little-endian `u32` body length.
pub const LENGTH_PREFIX_LEN: usize = 4;

/// Maximum encoded Postcard body length, excluding the prefix: 1 MiB.
///
/// This is a fixed wire-contract limit shared by readers and writers, not a
/// guarantee about allocations made by arbitrary Serde implementations.
pub const MAX_MESSAGE_BODY_LEN: usize = 1024 * 1024;
