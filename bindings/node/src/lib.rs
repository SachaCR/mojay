//! Node.js binding for Mojay, built with napi-rs.
//!
//! This crate contains no business logic: it converts values between Rust
//! and JavaScript, and runs each producer and each consumer on its own
//! thread (see `worker`).

mod consumer;
mod convert;
mod db;
mod error;
mod producer;
mod worker;
