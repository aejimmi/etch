//! Derive macros for etchdb.
//!
//! Generates `Replayable` and `Transactable` implementations from annotated
//! structs, eliminating ~60 lines of boilerplate per state type.

mod fingerprint;
mod parse;
mod replayable;
mod transactable;

use proc_macro::TokenStream;
use syn::{DeriveInput, parse_macro_input};

/// Derive `Replayable` for a struct with `#[etch(collection = N)]` fields.
///
/// Generates an `apply` method that routes ops to the correct field based
/// on the collection id, using `apply_op` for BTreeMap fields and
/// `apply_op_hash` for HashMap fields.
#[proc_macro_derive(Replayable, attributes(etch))]
pub fn derive_replayable(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match replayable::derive_replayable_inner(&input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// Derive `Transactable` for a struct with `#[etch(collection = N)]` fields.
///
/// Generates:
/// - A transaction struct (`{Name}Tx`) with `Collection` fields
/// - An overlay struct (`{Name}Overlay`) with `Overlay` fields
/// - The full `Transactable` trait implementation
#[proc_macro_derive(Transactable, attributes(etch))]
pub fn derive_transactable(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match transactable::derive_transactable_inner(&input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}
