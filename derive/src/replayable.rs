//! Code generation for `#[derive(Replayable)]`.

use quote::quote;
use syn::DeriveInput;

use crate::fingerprint::schema_fingerprint;
use crate::parse::{MapKind, build_where, parse_etch_fields};

pub(crate) fn derive_replayable_inner(
    input: &DeriveInput,
) -> syn::Result<proc_macro2::TokenStream> {
    let fields = parse_etch_fields(input)?;
    let name = &input.ident;
    let (impl_generics, ty_generics, _) = input.generics.split_for_impl();
    // Augmented where clause carries the implied bounds so a generic
    // `#[etch]` struct compiles (from_snapshot needs `Self: Default`).
    let where_clause = build_where(input, &fields, true);

    // Schema fingerprint const for the generated `schema_fingerprint()`.
    let schema_fingerprint_value: u64 = schema_fingerprint(&fields);

    // Simple dispatch arms for `apply_with_format` (no migration, no report).
    //
    // A per-op decode/key error is propagated with `?` rather than swallowed:
    // this path has no `ReplayContext` to record into, so surfacing the error
    // (instead of the old `eprintln!` + continue) is the correct
    // "don't silently drop" behavior. The migration-aware `apply_with_ctx`
    // path below is what production replay uses, and it records recoverable
    // skips into the report instead of erroring.
    let simple_arms: Vec<_> = fields
        .iter()
        .map(|f| {
            let id = f.collection_id;
            let version = f.schema_version;
            let field = &f.ident;
            let key_ty = &f.key_ty;
            let apply_fn = match f.map_kind {
                MapKind::BTreeMap => quote! { ::etchdb::wal::apply_op_versioned_with },
                MapKind::HashMap => quote! { ::etchdb::wal::apply_op_versioned_hash_with },
            };
            quote! {
                #id => {
                    #apply_fn(
                        &mut self.#field,
                        op,
                        format,
                        #version,
                        |bytes| <#key_ty as ::etchdb::EtchKey>::from_bytes(bytes),
                    )?;
                }
            }
        })
        .collect();

    // Context-aware dispatch arms (migration + quarantine + report).
    //
    // The apply helper records recoverable per-op outcomes (applied,
    // value-decode quarantine, key-decode skip) into `ctx.report` and returns
    // `Ok(())`; only a genuine entry-level error propagates via `?`.
    let ctx_arms: Vec<_> = fields
        .iter()
        .map(|f| {
            let id = f.collection_id;
            let version = f.schema_version;
            let field = &f.ident;
            let key_ty = &f.key_ty;
            let apply_fn = match f.map_kind {
                MapKind::BTreeMap => quote! { ::etchdb::wal::apply_op_versioned_with_ctx },
                MapKind::HashMap => quote! { ::etchdb::wal::apply_op_versioned_hash_with_ctx },
            };
            quote! {
                #id => {
                    #apply_fn(
                        &mut self.#field,
                        op,
                        #id,
                        #version,
                        ctx,
                        |bytes| <#key_ty as ::etchdb::EtchKey>::from_bytes(bytes),
                    )?;
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
                        let value_bytes = ::etchdb::wal::encode_msgpack_value(v)?;
                        entries.push(::etchdb::wal::SnapshotEntry {
                            key: ::etchdb::EtchKey::to_bytes(k),
                            version: #version,
                            value: value_bytes,
                        });
                    }
                    collections.push(::etchdb::wal::CollectionSection {
                        collection_id: #id,
                        current_version: #version,
                        entries,
                    });
                }
            }
        })
        .collect();

    // from_snapshot: per-collection dispatch using load_snapshot_entry.
    // A value-decode failure is quarantined + reported inside
    // `load_snapshot_entry`; a residual key-decode failure is recorded here.
    let from_snapshot_arms: Vec<_> = fields
        .iter()
        .map(|f| {
            let id = f.collection_id;
            let version = f.schema_version;
            let field = &f.ident;
            let key_ty = &f.key_ty;
            quote! {
                #id => {
                    for entry in &section.entries {
                        if let Some(v) = ::etchdb::wal::load_snapshot_entry(
                            entry,
                            #id,
                            #version,
                            ctx,
                        ) {
                            match <#key_ty as ::etchdb::EtchKey>::from_bytes(&entry.key) {
                                Ok(k) => {
                                    state.#field.insert(k, v);
                                }
                                Err(_) => {
                                    ctx.record_key_decode();
                                }
                            }
                        }
                    }
                }
            }
        })
        .collect();

    Ok(quote! {
        impl #impl_generics ::etchdb::Replayable for #name #ty_generics #where_clause {
            fn apply_with_format(
                &mut self,
                ops: &[::etchdb::Op],
                format: ::etchdb::ReplayFormat,
            ) -> ::etchdb::Result<()> {
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
                ops: &[::etchdb::Op],
                ctx: &mut ::etchdb::ReplayContext<'_>,
            ) -> ::etchdb::Result<()> {
                for op in ops {
                    match op.collection() {
                        #(#ctx_arms)*
                        other => {
                            // Op for a collection id this binary doesn't
                            // declare — record it as lost rather than dropping
                            // it without a trace.
                            ctx.record_unknown_collection(other);
                        }
                    }
                }
                Ok(())
            }

            fn schema_fingerprint() -> u64 {
                #schema_fingerprint_value
            }

            fn to_snapshot(&self) -> ::etchdb::Result<::etchdb::wal::SnapshotPayload> {
                let mut collections = ::std::vec::Vec::new();
                #(#to_snapshot_sections)*
                Ok(::etchdb::wal::SnapshotPayload {
                    schema_fingerprint: #schema_fingerprint_value,
                    collections,
                })
            }

            fn from_snapshot(
                payload: ::etchdb::wal::SnapshotPayload,
                ctx: &mut ::etchdb::ReplayContext<'_>,
            ) -> ::etchdb::Result<Self>
            where
                Self: Sized,
            {
                let mut state = <Self as ::std::default::Default>::default();
                for section in &payload.collections {
                    match section.collection_id {
                        #(#from_snapshot_arms)*
                        other => {
                            // Unknown collection id in snapshot — removed or
                            // renamed in this binary. Record every orphaned
                            // entry as lost instead of dropping without a trace.
                            for _ in &section.entries {
                                ctx.record_unknown_collection(other);
                            }
                        }
                    }
                }
                Ok(state)
            }
        }
    })
}
