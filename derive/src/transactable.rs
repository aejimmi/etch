//! Code generation for `#[derive(Transactable)]`.

use quote::{format_ident, quote};
use syn::DeriveInput;

use crate::parse::{MapKind, build_where, map_type_tokens, parse_etch_fields};

pub(crate) fn derive_transactable_inner(
    input: &DeriveInput,
) -> syn::Result<proc_macro2::TokenStream> {
    let fields = parse_etch_fields(input)?;
    let name = &input.ident;
    let vis = &input.vis;
    let tx_name = format_ident!("{}Tx", name);
    let overlay_name = format_ident!("{}Overlay", name);
    let (impl_generics, ty_generics, _) = input.generics.split_for_impl();
    let where_clause = build_where(input, &fields, false);
    // The Tx struct carries the input's generics plus a fresh `'a`.
    let mut tx_generics = input.generics.clone();
    tx_generics.params.insert(0, syn::parse_quote!('a));
    let (_, tx_ty_generics, _) = tx_generics.split_for_impl();

    // Overlay struct fields.
    let overlay_fields: Vec<_> = fields
        .iter()
        .map(|f| {
            let ident = &f.ident;
            let k = &f.key_ty;
            let v = &f.value_ty;
            quote! { pub #ident: ::etchdb::Overlay<#k, #v> }
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
            quote! { pub #ident: ::etchdb::Collection<'a, #k, #v, #m> }
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
                #ident: ::etchdb::Collection::new(&self.#ident, #id, #version)
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
                MapKind::BTreeMap => quote! { ::etchdb::apply_overlay_btree },
                MapKind::HashMap => quote! { ::etchdb::apply_overlay_hash },
            };
            quote! { #merge_fn(&mut self.#ident, overlay.#ident); }
        })
        .collect();

    Ok(quote! {
        #vis struct #overlay_name #ty_generics #where_clause {
            #(#overlay_fields,)*
        }

        #vis struct #tx_name #tx_ty_generics #where_clause {
            #(#tx_fields,)*
        }

        impl #impl_generics ::etchdb::Transactable for #name #ty_generics #where_clause {
            type Tx<'a> = #tx_name #tx_ty_generics;
            type Overlay = #overlay_name #ty_generics;

            fn begin_tx(&self) -> Self::Tx<'_> {
                #tx_name {
                    #(#begin_fields,)*
                }
            }

            fn finish_tx(tx: Self::Tx<'_>) -> (::std::vec::Vec<::etchdb::Op>, Self::Overlay) {
                let mut ops = ::std::vec::Vec::new();
                #(#finish_lets)*
                (ops, #overlay_name {
                    #(#finish_overlay_fields,)*
                })
            }

            fn apply_overlay(&mut self, overlay: Self::Overlay) {
                #(#apply_stmts)*
            }
        }
    })
}
