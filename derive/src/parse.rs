//! Shared parsing of `#[etch(collection = N, version = V)]` fields and the
//! `where`-clause construction used by both derive macros.

use quote::quote;
use syn::spanned::Spanned;
use syn::{DeriveInput, Fields, PathSegment};

/// Parsed info about one `#[etch(collection = N, version = V)]` field.
pub(crate) struct EtchField {
    pub(crate) ident: syn::Ident,
    pub(crate) collection_id: u8,
    /// Current schema version for this collection's value type.
    /// Defaults to 1. Version 0 is reserved for legacy unversioned (pre-0.4.0)
    /// postcard values.
    pub(crate) schema_version: u16,
    pub(crate) map_kind: MapKind,
    pub(crate) key_ty: syn::Type,
    pub(crate) value_ty: syn::Type,
}

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum MapKind {
    BTreeMap,
    HashMap,
}

pub(crate) fn parse_etch_fields(input: &DeriveInput) -> syn::Result<Vec<EtchField>> {
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

        let ident = field
            .ident
            .clone()
            .ok_or_else(|| syn::Error::new(field.span(), "etch derives require named fields"))?;
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

/// Build a `where` clause carrying the bounds implied by the `#[etch]`
/// fields, so a *generic* `#[etch]` struct compiles. For a non-generic struct
/// the concrete field types satisfy these automatically, and emitting them
/// would be a stable-Rust trivial-bounds error — so we return the input's
/// original clause unchanged in that case.
///
/// When `self_default` is set (the `Replayable` impl, whose `from_snapshot`
/// calls `Self::default()`), a `Self: Default` bound is added.
pub(crate) fn build_where(
    input: &DeriveInput,
    fields: &[EtchField],
    self_default: bool,
) -> proc_macro2::TokenStream {
    let has_type_params = input.generics.type_params().next().is_some();
    if !has_type_params {
        return match &input.generics.where_clause {
            Some(w) => quote!(#w),
            None => quote!(),
        };
    }

    // Only bound field types that actually mention a generic parameter.
    // A concrete bound like `where String: EtchKey` is a stable-Rust
    // trivial-bounds error even inside a generic item, so it must be skipped.
    let params: Vec<String> = input
        .generics
        .type_params()
        .map(|t| t.ident.to_string())
        .collect();

    let mut preds: Vec<proc_macro2::TokenStream> = Vec::new();
    if let Some(w) = &input.generics.where_clause {
        for p in &w.predicates {
            preds.push(quote!(#p));
        }
    }
    for f in fields {
        let k = &f.key_ty;
        let v = &f.value_ty;
        if ty_mentions_param(k, &params) {
            preds.push(quote!(#k: ::etchdb::EtchKey));
        }
        if ty_mentions_param(v, &params) {
            preds.push(quote!(#v: ::serde::Serialize + ::serde::de::DeserializeOwned));
        }
    }
    if self_default {
        preds.push(quote!(Self: ::std::default::Default));
    }
    if preds.is_empty() {
        return quote!();
    }
    quote!(where #(#preds),*)
}

/// Whether a type's token stream references any of the given generic
/// parameter names (matched on identifier boundaries).
fn ty_mentions_param(ty: &syn::Type, params: &[String]) -> bool {
    if params.is_empty() {
        return false;
    }
    let s = quote!(#ty).to_string();
    let idents: Vec<&str> = s
        .split(|c: char| !c.is_alphanumeric() && c != '_')
        .filter(|w| !w.is_empty())
        .collect();
    params.iter().any(|p| idents.iter().any(|w| w == p))
}

pub(crate) fn map_type_tokens(f: &EtchField) -> proc_macro2::TokenStream {
    let k = &f.key_ty;
    let v = &f.value_ty;
    match f.map_kind {
        MapKind::BTreeMap => quote! { ::std::collections::BTreeMap<#k, #v> },
        MapKind::HashMap => quote! { ::std::collections::HashMap<#k, #v> },
    }
}
