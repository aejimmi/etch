//! Derive macros for etchdb.
//!
//! Generates `Replayable` and `Transactable` implementations from annotated
//! structs, eliminating ~60 lines of boilerplate per state type.

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::spanned::Spanned;
use syn::{DeriveInput, Fields, PathSegment, parse_macro_input};

/// Parsed info about one `#[etch(collection = N, version = V)]` field.
struct EtchField {
    ident: syn::Ident,
    collection_id: u8,
    /// Current schema version for this collection's value type.
    /// Defaults to 1. Version 0 is reserved for legacy unversioned (pre-0.4.0)
    /// postcard values.
    schema_version: u16,
    map_kind: MapKind,
    key_ty: syn::Type,
    value_ty: syn::Type,
}

#[derive(Clone, Copy, PartialEq)]
enum MapKind {
    BTreeMap,
    HashMap,
}

fn parse_etch_fields(input: &DeriveInput) -> syn::Result<Vec<EtchField>> {
    let data = match &input.data {
        syn::Data::Struct(s) => s,
        _ => {
            return Err(syn::Error::new_spanned(
                input,
                "etch derives only work on structs",
            ));
        }
    };
    let fields = match &data.fields {
        Fields::Named(f) => &f.named,
        _ => {
            return Err(syn::Error::new_spanned(
                input,
                "etch derives require named fields",
            ));
        }
    };

    let mut result = Vec::new();

    for field in fields {
        let mut collection_id: Option<u8> = None;
        let mut schema_version: u16 = 1;

        for attr in &field.attrs {
            if !attr.path().is_ident("etch") {
                continue;
            }
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("collection") {
                    let value = meta.value()?;
                    let lit: syn::LitInt = value.parse()?;
                    collection_id = Some(lit.base10_parse()?);
                    Ok(())
                } else if meta.path.is_ident("version") {
                    let value = meta.value()?;
                    let lit: syn::LitInt = value.parse()?;
                    schema_version = lit.base10_parse()?;
                    if schema_version == 0 {
                        return Err(meta.error(
                            "version 0 is reserved for legacy unversioned data; \
                             start new schemas at version 1",
                        ));
                    }
                    Ok(())
                } else {
                    Err(meta.error("expected `collection = N` or `version = V`"))
                }
            })?;
        }

        let Some(id) = collection_id else {
            continue;
        };

        let ident = field.ident.clone().unwrap();
        let (map_kind, key_ty, value_ty) = parse_map_type(&field.ty).ok_or_else(|| {
            syn::Error::new(field.ty.span(), "expected BTreeMap<K, V> or HashMap<K, V>")
        })?;

        result.push(EtchField {
            ident,
            collection_id: id,
            schema_version,
            map_kind,
            key_ty,
            value_ty,
        });
    }

    if result.is_empty() {
        return Err(syn::Error::new_spanned(
            input,
            "no fields annotated with #[etch(collection = N)]",
        ));
    }

    // Check for duplicate collection IDs.
    let mut seen = std::collections::HashSet::new();
    for f in &result {
        if !seen.insert(f.collection_id) {
            return Err(syn::Error::new_spanned(
                &f.ident,
                format!("duplicate collection id {}", f.collection_id),
            ));
        }
    }

    Ok(result)
}

/// Extract (MapKind, K, V) from `BTreeMap<K, V>` or `HashMap<K, V>`.
fn parse_map_type(ty: &syn::Type) -> Option<(MapKind, syn::Type, syn::Type)> {
    let path = match ty {
        syn::Type::Path(p) => &p.path,
        _ => return None,
    };
    let seg: &PathSegment = path.segments.last()?;
    let kind = match seg.ident.to_string().as_str() {
        "BTreeMap" => MapKind::BTreeMap,
        "HashMap" => MapKind::HashMap,
        _ => return None,
    };
    let args = match &seg.arguments {
        syn::PathArguments::AngleBracketed(a) => a,
        _ => return None,
    };
    let mut types = args.args.iter().filter_map(|a| match a {
        syn::GenericArgument::Type(t) => Some(t.clone()),
        _ => None,
    });
    let key = types.next()?;
    let val = types.next()?;
    Some((kind, key, val))
}

/// Derive `Replayable` for a struct with `#[etch(collection = N)]` fields.
///
/// Generates an `apply` method that routes ops to the correct field based
/// on the collection id, using `apply_op` for BTreeMap fields and
/// `apply_op_hash` for HashMap fields.
#[proc_macro_derive(Replayable, attributes(etch))]
pub fn derive_replayable(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match derive_replayable_inner(&input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn derive_replayable_inner(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let fields = parse_etch_fields(input)?;
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    // Compute the schema fingerprint at macro-expand time: xxh3 of the
    // sorted `(collection_id, version)` list. This becomes a const in
    // the generated `schema_fingerprint()` method.
    let schema_fingerprint_value: u64 = {
        let mut pairs: Vec<(u8, u16)> = fields
            .iter()
            .map(|f| (f.collection_id, f.schema_version))
            .collect();
        pairs.sort();
        let mut bytes = Vec::with_capacity(pairs.len() * 3);
        for (c, v) in &pairs {
            bytes.push(*c);
            bytes.extend_from_slice(&v.to_le_bytes());
        }
        xxhash_rust::xxh3::xxh3_64(&bytes)
    };

    // Simple dispatch arms for `apply_with_format` (no migration).
    let simple_arms: Vec<_> = fields
        .iter()
        .map(|f| {
            let id = f.collection_id;
            let version = f.schema_version;
            let field = &f.ident;
            let field_name = f.ident.to_string();
            let key_ty = &f.key_ty;
            let apply_fn = match f.map_kind {
                MapKind::BTreeMap => quote! { etchdb::apply_op_versioned_with },
                MapKind::HashMap => quote! { etchdb::apply_op_versioned_hash_with },
            };
            quote! {
                #id => {
                    if let Err(e) = #apply_fn(
                        &mut self.#field,
                        op,
                        format,
                        #version,
                        |bytes| <#key_ty as etchdb::EtchKey>::from_bytes(bytes),
                    ) {
                        eprintln!(
                            "etchdb: skipped op on '{}' (collection {}, version {}), key={}: {}",
                            #field_name,
                            #id,
                            #version,
                            etchdb::format_op_key(op),
                            e,
                        );
                    }
                }
            }
        })
        .collect();

    // Context-aware dispatch arms (with migration).
    let ctx_arms: Vec<_> = fields
        .iter()
        .map(|f| {
            let id = f.collection_id;
            let version = f.schema_version;
            let field = &f.ident;
            let field_name = f.ident.to_string();
            let key_ty = &f.key_ty;
            let apply_fn = match f.map_kind {
                MapKind::BTreeMap => quote! { etchdb::apply_op_versioned_with_ctx },
                MapKind::HashMap => quote! { etchdb::apply_op_versioned_hash_with_ctx },
            };
            quote! {
                #id => {
                    if let Err(e) = #apply_fn(
                        &mut self.#field,
                        op,
                        #id,
                        #version,
                        ctx,
                        |bytes| <#key_ty as etchdb::EtchKey>::from_bytes(bytes),
                    ) {
                        eprintln!(
                            "etchdb: skipped op on '{}' (collection {}, version {}), key={}: {}",
                            #field_name,
                            #id,
                            #version,
                            etchdb::format_op_key(op),
                            e,
                        );
                    }
                }
            }
        })
        .collect();

    // to_snapshot: serialize each collection's entries with per-value version tag.
    let to_snapshot_sections: Vec<_> = fields
        .iter()
        .map(|f| {
            let id = f.collection_id;
            let version = f.schema_version;
            let field = &f.ident;
            quote! {
                {
                    let mut entries = ::std::vec::Vec::with_capacity(self.#field.len());
                    for (k, v) in &self.#field {
                        let value_bytes = etchdb::encode_msgpack_value(v)?;
                        entries.push(etchdb::SnapshotEntry {
                            key: etchdb::EtchKey::to_bytes(k),
                            version: #version,
                            value: value_bytes,
                        });
                    }
                    collections.push(etchdb::CollectionSection {
                        collection_id: #id,
                        current_version: #version,
                        entries,
                    });
                }
            }
        })
        .collect();

    // from_snapshot: per-collection dispatch using load_snapshot_entry.
    let from_snapshot_arms: Vec<_> = fields
        .iter()
        .map(|f| {
            let id = f.collection_id;
            let version = f.schema_version;
            let field = &f.ident;
            let field_name = f.ident.to_string();
            let key_ty = &f.key_ty;
            quote! {
                #id => {
                    for entry in &section.entries {
                        if let Some(v) = etchdb::load_snapshot_entry(
                            entry,
                            #id,
                            #version,
                            ctx,
                        ) {
                            match <#key_ty as etchdb::EtchKey>::from_bytes(&entry.key) {
                                Ok(k) => {
                                    state.#field.insert(k, v);
                                }
                                Err(e) => {
                                    eprintln!(
                                        "etchdb: dropping snapshot entry in '{}' (collection {}): bad key: {}",
                                        #field_name, #id, e,
                                    );
                                }
                            }
                        }
                    }
                }
            }
        })
        .collect();

    Ok(quote! {
        impl #impl_generics etchdb::Replayable for #name #ty_generics #where_clause {
            fn apply_with_format(
                &mut self,
                ops: &[etchdb::Op],
                format: etchdb::ReplayFormat,
            ) -> etchdb::Result<()> {
                for op in ops {
                    match op.collection() {
                        #(#simple_arms)*
                        _ => {}
                    }
                }
                Ok(())
            }

            fn apply_with_ctx(
                &mut self,
                ops: &[etchdb::Op],
                ctx: &mut etchdb::ReplayContext<'_>,
            ) -> etchdb::Result<()> {
                for op in ops {
                    match op.collection() {
                        #(#ctx_arms)*
                        _ => {}
                    }
                }
                Ok(())
            }

            fn schema_fingerprint() -> u64 {
                #schema_fingerprint_value
            }

            fn to_snapshot(&self) -> etchdb::Result<etchdb::SnapshotPayload> {
                let mut collections = ::std::vec::Vec::new();
                #(#to_snapshot_sections)*
                Ok(etchdb::SnapshotPayload {
                    schema_fingerprint: #schema_fingerprint_value,
                    collections,
                })
            }

            fn from_snapshot(
                payload: etchdb::SnapshotPayload,
                ctx: &mut etchdb::ReplayContext<'_>,
            ) -> etchdb::Result<Self>
            where
                Self: Sized,
            {
                let mut state = <Self as ::std::default::Default>::default();
                for section in &payload.collections {
                    match section.collection_id {
                        #(#from_snapshot_arms)*
                        _ => {
                            // Unknown collection id in snapshot — was removed
                            // or renamed in this binary. Silently ignore for
                            // now (could quarantine in the future).
                        }
                    }
                }
                Ok(state)
            }
        }
    })
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
    match derive_transactable_inner(&input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn derive_transactable_inner(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let fields = parse_etch_fields(input)?;
    let name = &input.ident;
    let tx_name = format_ident!("{}Tx", name);
    let overlay_name = format_ident!("{}Overlay", name);
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    // Overlay struct fields.
    let overlay_fields: Vec<_> = fields
        .iter()
        .map(|f| {
            let ident = &f.ident;
            let k = &f.key_ty;
            let v = &f.value_ty;
            quote! { pub #ident: etchdb::Overlay<#k, #v> }
        })
        .collect();

    // Tx struct fields: one Collection per annotated field.
    let tx_fields: Vec<_> = fields
        .iter()
        .map(|f| {
            let ident = &f.ident;
            let k = &f.key_ty;
            let v = &f.value_ty;
            let m = map_type_tokens(f);
            quote! { pub #ident: etchdb::Collection<'a, #k, #v, #m> }
        })
        .collect();

    // begin_tx: construct Collection for each field.
    let begin_fields: Vec<_> = fields
        .iter()
        .map(|f| {
            let ident = &f.ident;
            let id = f.collection_id;
            let version = f.schema_version;
            quote! {
                #ident: etchdb::Collection::new(&self.#ident, #id, #version)
            }
        })
        .collect();

    // finish_tx: destructure each Collection into ops + overlay.
    let finish_lets: Vec<_> = fields
        .iter()
        .map(|f| {
            let ident = &f.ident;
            let ops_name = format_ident!("{}_ops", ident);
            let ov_name = format_ident!("{}_ov", ident);
            quote! {
                let (#ops_name, #ov_name) = tx.#ident.into_parts();
                ops.extend(#ops_name);
            }
        })
        .collect();

    let finish_overlay_fields: Vec<_> = fields
        .iter()
        .map(|f| {
            let ident = &f.ident;
            let ov_name = format_ident!("{}_ov", ident);
            quote! { #ident: #ov_name }
        })
        .collect();

    // apply_overlay: merge each overlay into committed state.
    let apply_stmts: Vec<_> = fields
        .iter()
        .map(|f| {
            let ident = &f.ident;
            let merge_fn = match f.map_kind {
                MapKind::BTreeMap => quote! { etchdb::apply_overlay_btree },
                MapKind::HashMap => quote! { etchdb::apply_overlay_hash },
            };
            quote! { #merge_fn(&mut self.#ident, overlay.#ident); }
        })
        .collect();

    Ok(quote! {
        pub struct #overlay_name {
            #(#overlay_fields,)*
        }

        pub struct #tx_name<'a> {
            #(#tx_fields,)*
        }

        impl #impl_generics etchdb::Transactable for #name #ty_generics #where_clause {
            type Tx<'a> = #tx_name<'a>;
            type Overlay = #overlay_name;

            fn begin_tx(&self) -> #tx_name<'_> {
                #tx_name {
                    #(#begin_fields,)*
                }
            }

            fn finish_tx(tx: #tx_name<'_>) -> (::std::vec::Vec<etchdb::Op>, #overlay_name) {
                let mut ops = ::std::vec::Vec::new();
                #(#finish_lets)*
                (ops, #overlay_name {
                    #(#finish_overlay_fields,)*
                })
            }

            fn apply_overlay(&mut self, overlay: #overlay_name) {
                #(#apply_stmts)*
            }
        }
    })
}

fn map_type_tokens(f: &EtchField) -> proc_macro2::TokenStream {
    let k = &f.key_ty;
    let v = &f.value_ty;
    match f.map_kind {
        MapKind::BTreeMap => quote! { std::collections::BTreeMap<#k, #v> },
        MapKind::HashMap => quote! { std::collections::HashMap<#k, #v> },
    }
}
