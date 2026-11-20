//! Errors as seen from JavaScript: a real `Error`, with a stable `code`,
//! like Node's own errors (`err.code === 'MOJAY_BUSY'`).

use napi::bindgen_prelude::JsObjectValue;
use napi::{Env, JsValue, Status};

/// A binding error, which can be sent from one thread to another.
#[derive(Debug)]
pub enum BindingError {
    Mojay(mojay::Error),
    /// Method called after `close()`.
    Closed,
    /// Invalid JavaScript argument (unknown option, negative number...).
    InvalidArgument(String),
}

impl From<mojay::Error> for BindingError {
    fn from(error: mojay::Error) -> Self {
        Self::Mojay(error)
    }
}

impl BindingError {
    /// The code exposed to JavaScript in `err.code`.
    pub fn code(&self) -> &'static str {
        use mojay::Error as E;
        match self {
            Self::Closed => "MOJAY_CLOSED",
            Self::InvalidArgument(_) => "MOJAY_INVALID_ARGUMENT",
            Self::Mojay(error) => match error {
                E::Io(_) => "MOJAY_IO",
                E::Corrupt { .. } => "MOJAY_CORRUPT",
                E::OffsetOutOfRange { .. } => "MOJAY_OFFSET_OUT_OF_RANGE",
                E::RecordTooLarge { .. } => "MOJAY_RECORD_TOO_LARGE",
                E::InvalidRecord(_) => "MOJAY_INVALID_RECORD",
                E::InvalidConfig(_) => "MOJAY_INVALID_CONFIG",
                E::Busy(_) => "MOJAY_BUSY",
                E::TopicExists(_) => "MOJAY_TOPIC_EXISTS",
                E::UnknownTopic(_) => "MOJAY_UNKNOWN_TOPIC",
                E::InvalidName { .. } => "MOJAY_INVALID_NAME",
                E::UnknownPartition { .. } => "MOJAY_UNKNOWN_PARTITION",
            },
        }
    }

    pub fn message(&self) -> String {
        match self {
            Self::Mojay(error) => error.to_string(),
            Self::Closed => "already closed: close() was called".to_string(),
            Self::InvalidArgument(reason) => reason.clone(),
        }
    }

    /// Builds the JavaScript `Error` object. Must be called on the Node thread.
    pub fn into_js(self, env: &Env) -> napi::Error {
        let message = self.message();
        let build = || -> napi::Result<napi::Error> {
            let mut error = env.create_error(napi::Error::new(Status::GenericFailure, &message))?;
            error.set_named_property("code", self.code())?;
            Ok(napi::Error::from(error.to_unknown()))
        };
        // If even building it fails, at least return the message.
        build().unwrap_or_else(|_| napi::Error::new(Status::GenericFailure, message))
    }
}

/// Converts the result of a synchronous method for JavaScript.
pub fn to_js<T>(env: &Env, result: Result<T, impl Into<BindingError>>) -> napi::Result<T> {
    result.map_err(|e| e.into().into_js(env))
}
