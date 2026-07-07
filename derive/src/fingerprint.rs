//! Compile-time schema-fingerprint computation for `#[derive(Replayable)]`.

use quote::quote;

use crate::parse::EtchField;

/// Compute the schema fingerprint at macro-expand time: xxh3 of the
/// sorted per-collection list of `(collection_id, version, key-type
/// tokens, value-type tokens)`. This becomes a const in the generated
/// `schema_fingerprint()` method.
///
/// Folding the key/value *type token strings* in (not just the version)
/// means swapping a field's value type — e.g. `BTreeMap<K, ItemV1>` to
/// `BTreeMap<K, ItemV2>` — flips the fingerprint even when the developer
/// forgot to bump the version, so `check_schema_drift` can fire for its
/// target "forgot to bump" case.
///
/// Stability tradeoff: the token string is stable across compilations of
/// the same source and differs whenever the *named* type at the field
/// changes. It does NOT see through a type whose name is unchanged but
/// whose fields evolved (that shape lives in another type's `derive`, out
/// of reach here) — that class still relies on a version bump.
pub(crate) fn schema_fingerprint(fields: &[EtchField]) -> u64 {
    let mut items: Vec<(u8, u16, String, String)> = fields
        .iter()
        .map(|f| {
            let key_ty = &f.key_ty;
            let value_ty = &f.value_ty;
            (
                f.collection_id,
                f.schema_version,
                quote!(#key_ty).to_string(),
                quote!(#value_ty).to_string(),
            )
        })
        .collect();
    items.sort();
    let mut bytes = Vec::new();
    for (c, v, k, val) in &items {
        bytes.push(*c);
        bytes.extend_from_slice(&v.to_le_bytes());
        bytes.extend_from_slice(k.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(val.as_bytes());
        bytes.push(0);
    }
    xxhash_rust::xxh3::xxh3_64(&bytes)
}
