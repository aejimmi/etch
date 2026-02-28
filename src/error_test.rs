//! Tests for error types.

use crate::error::Error;

#[test]
fn not_found_display() {
    let err = Error::not_found("User", "u123");
    assert_eq!(err.to_string(), "User not found: u123");
}

#[test]
fn already_exists_display() {
    let err = Error::already_exists("User", "u123");
    assert_eq!(err.to_string(), "User already exists: u123");
}

#[test]
fn invalid_display() {
    let err = Error::invalid("email", "missing @");
    assert_eq!(err.to_string(), "invalid email: missing @");
}

#[test]
fn not_found_accepts_string() {
    let id = String::from("dynamic_id");
    let err = Error::not_found("Item", id);
    assert_eq!(err.to_string(), "Item not found: dynamic_id");
}

#[test]
fn wal_corrupted_display() {
    let err = Error::WalCorrupted {
        offset: 42,
        reason: "bad checksum".into(),
    };
    assert_eq!(err.to_string(), "WAL corrupted at offset 42: bad checksum");
}

#[test]
fn io_error_from_conversion() {
    let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "gone");
    let err: Error = io_err.into();
    assert!(matches!(err, Error::Io(_)));
    assert!(err.to_string().contains("gone"));
}

#[test]
fn error_is_send_and_sync() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}
    // These are compile-time checks.
    assert_send::<Error>();
    assert_sync::<Error>();
}
