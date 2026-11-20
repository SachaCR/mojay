//! Conversions between JavaScript values and Mojay types.
//! Every value coming from JavaScript is checked here before going any further.

use napi::bindgen_prelude::{Buffer, Either};

use crate::error::BindingError;

/// Bytes coming from JavaScript: a string (encoded as UTF-8) or a `Buffer`.
pub type JsBytes = Either<String, Buffer>;

/// Copies the bytes: a `Buffer` cannot leave the Node thread.
pub fn bytes(value: JsBytes) -> Vec<u8> {
    match value {
        Either::A(text) => text.into_bytes(),
        Either::B(buffer) => buffer.to_vec(),
    }
}

/// A JavaScript number that must be a non-negative integer.
pub fn non_negative(name: &str, value: i64) -> Result<u64, BindingError> {
    u64::try_from(value)
        .map_err(|_| BindingError::InvalidArgument(format!("{name} must be zero or positive")))
}

/// A Mojay offset as a JavaScript number. Exact up to 2^53 messages per
/// partition: beyond what a partition can hold in practice.
pub fn offset(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}
