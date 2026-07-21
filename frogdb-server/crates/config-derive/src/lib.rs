//! Proc macros for the FrogDB CONFIG GET/SET parameter registry.
//!
//! Provides two complementary derives:
//!
//! - `#[derive(ConfigParams)]` — per-**field** coverage. Turns a serde
//!   config-section struct into the single source of truth for its CONFIG
//!   parameters. Every field MUST carry either a `#[param(...)]` attribute (the
//!   field is exposed as a CONFIG parameter) or `#[param(skip)]` (the field is
//!   internal and has no CONFIG parameter). A field lacking both is a compile
//!   error — a new serde field can no longer silently miss the registry.
//! - `#[derive(ConfigSections)]` — per-**struct** coverage. Applied to the root
//!   `Config`, it makes an unclassified *section* (a new field on `Config`) a
//!   compile error. See that derive's docs below.
//!
//! # Grammar
//!
//! ```ignore
//! #[derive(ConfigParams)]
//! #[params(section = "memory")]          // TOML section name (required)
//! #[serde(deny_unknown_fields, rename_all = "kebab-case")]
//! pub struct MemoryConfig {
//!     #[param(mutable)]                  // CONFIG GET/SET, runtime-mutable
//!     pub maxmemory: u64,
//!
//!     #[param]                           // CONFIG GET only (immutable)
//!     pub some_startup_field: u64,
//!
//!     #[param(mutable, name = "loglevel")] // CONFIG name diverges from field
//!     pub level: String,
//!
//!     #[param(mutable, noop)]            // Redis-compat no-op (section/field cleared)
//!     pub legacy_knob: u64,
//!
//!     #[param(skip)]                     // internal field, no CONFIG parameter
//!     pub doctor_big_key_threshold: u64,
//! }
//! ```
//!
//! The derive emits `impl <Struct> { pub const PARAMS: &'static
//! [crate::params::ConfigParamInfo] = &[ ... ]; }`. The registry in
//! `frogdb-config`'s `params.rs` concatenates these per-struct tables (plus a
//! small hand-written list of "virtual" params that have no serde backing).
//!
//! # Emitted-row rules
//!
//! For a `#[param(...)]` field the emitted `ConfigParamInfo` row is:
//! - `name`: the `name = "..."` override if present, else the field's serde name
//!   (kebab-case of the ident, honoring `#[serde(rename)]`).
//! - `section`: `Some(<struct section>)`, or `None` when `noop` is set.
//! - `field`: `Some(<serde field name>)`, or `None` when `noop` is set.
//! - `mutable`: `true` iff the `mutable` flag is present.
//! - `noop`: `true` iff the `noop` flag is present.

use proc_macro::TokenStream;
use quote::quote;
use syn::{Attribute, Data, DeriveInput, Fields, LitStr, Token, parse_macro_input};

/// Derive the CONFIG parameter table for a config-section struct.
#[proc_macro_derive(ConfigParams, attributes(params, param))]
pub fn derive_config_params(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand(&input) {
        Ok(ts) => ts.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Derive per-**struct** coverage for the root `Config`.
///
/// This is the per-struct analogue of the per-field guarantee `ConfigParams`
/// provides. `ConfigParams` makes an unclassified *field* a compile error;
/// `ConfigSections` makes an unclassified *section* (a new field on the root
/// `Config`) a compile error, so a new config section can no longer be added
/// without deciding whether it contributes CONFIG parameters.
///
/// # Grammar
///
/// ```ignore
/// #[derive(ConfigSections)]
/// pub struct Config {
///     #[section(skip)]                 // not a CONFIG-param section (e.g. an
///     pub config_source_path: ...,     // internal / programmatic field)
///
///     #[section]                       // a section whose type has `PARAMS`
///     pub server: ServerConfig,        // (i.e. it carries #[derive(ConfigParams)])
///     // ...
/// }
/// ```
///
/// Every field MUST carry either `#[section]` or `#[section(skip)]`. A field
/// lacking both is a compile error — this is the whole point of the derive.
///
/// # What it emits
///
/// An `impl Config { pub const SECTION_PARAMS: &'static [&'static
/// [ConfigParamInfo]]; }` referencing each `#[section]` field's
/// `<FieldType>::PARAMS`. That reference does double duty:
///
/// 1. **Compile-time completeness link.** A `#[section]` field whose type has no
///    `PARAMS` const (i.e. is not a `#[derive(ConfigParams)]` section) fails to
///    compile with "no associated item named `PARAMS`". Classifying a field as a
///    section therefore forces it through the per-field coverage guarantee.
/// 2. **Test hook.** `frogdb_config`'s `params.rs` asserts that the hand-spliced,
///    order-preserving `config_param_registry()` assembly covers *exactly* this
///    derived section set (`tests::test_registry_covers_derived_sections`), so a
///    new section that is classified but never wired into the assembly is a red
///    test rather than a silent hole.
///
/// The derive deliberately does **not** generate the registry assembly itself:
/// the row order is historical and load-bearing (pinned by a golden snapshot),
/// so assembly stays hand-written while this derive enforces coverage.
#[proc_macro_derive(ConfigSections, attributes(section))]
pub fn derive_config_sections(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand_sections(&input) {
        Ok(ts) => ts.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn expand(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let struct_ident = &input.ident;

    // --- Struct-level `#[params(section = "...")]` (required) ---
    let section = parse_section(&input.attrs)?.ok_or_else(|| {
        syn::Error::new_spanned(
            struct_ident,
            "ConfigParams requires a `#[params(section = \"...\")]` attribute naming the TOML section",
        )
    })?;

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(named) => &named.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    struct_ident,
                    "ConfigParams can only be derived for structs with named fields",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                struct_ident,
                "ConfigParams can only be derived for structs",
            ));
        }
    };

    let mut rows = Vec::new();

    for field in fields {
        let field_ident = field
            .ident
            .as_ref()
            .expect("named field always has an ident");

        let opts = parse_param_opts(field)?;
        let opts = match opts {
            // Every field MUST opt in or out. This is the invariant the derive
            // exists to enforce: a serde field with no decision is a hard error.
            None => {
                return Err(syn::Error::new_spanned(
                    field_ident,
                    format!(
                        "field `{field_ident}` is missing a `#[param(...)]` or `#[param(skip)]` \
                         attribute; every config field must declare whether it is a CONFIG parameter",
                    ),
                ));
            }
            Some(o) => o,
        };

        if opts.skip {
            continue;
        }

        // serde field name: `#[serde(rename)]` override, else kebab-case ident.
        let serde_field = serde_rename(&field.attrs).unwrap_or_else(|| kebab_case(field_ident));
        // CONFIG name: `name = "..."` override, else the serde field name.
        let param_name = opts.name.clone().unwrap_or_else(|| serde_field.clone());

        let mutable = opts.mutable;
        let noop = opts.noop;

        let (section_tokens, field_tokens) = if noop {
            // No-op params have no TOML backing (matches the registry invariant
            // that `noop => section.is_none() && field.is_none()`).
            (quote! { None }, quote! { None })
        } else {
            (quote! { Some(#section) }, quote! { Some(#serde_field) })
        };

        rows.push(quote! {
            crate::params::ConfigParamInfo {
                name: #param_name,
                section: #section_tokens,
                field: #field_tokens,
                mutable: #mutable,
                noop: #noop,
            }
        });
    }

    Ok(quote! {
        impl #struct_ident {
            /// CONFIG GET/SET parameter rows derived from this section's fields.
            ///
            /// Generated by `#[derive(ConfigParams)]`. Consumed by
            /// `frogdb_config::config_param_registry`.
            pub const PARAMS: &'static [crate::params::ConfigParamInfo] = &[
                #(#rows),*
            ];
        }
    })
}

fn expand_sections(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let struct_ident = &input.ident;

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(named) => &named.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    struct_ident,
                    "ConfigSections can only be derived for structs with named fields",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                struct_ident,
                "ConfigSections can only be derived for structs",
            ));
        }
    };

    // Types of the fields classified as sections, in declaration order. Each
    // contributes `<Type>::PARAMS` to the emitted `SECTION_PARAMS` table.
    let mut section_types = Vec::new();

    for field in fields {
        let field_ident = field
            .ident
            .as_ref()
            .expect("named field always has an ident");

        match parse_section_opts(field)? {
            // Every field MUST opt in or out. This is the invariant the derive
            // exists to enforce: a new section with no decision is a hard error.
            None => {
                return Err(syn::Error::new_spanned(
                    field_ident,
                    format!(
                        "field `{field_ident}` is missing a `#[section]` or `#[section(skip)]` \
                         attribute; every root config field must declare whether it is a CONFIG \
                         parameter section",
                    ),
                ));
            }
            // `#[section(skip)]`: not a CONFIG-param section, contributes nothing.
            Some(true) => continue,
            // `#[section]`: its type must expose `PARAMS`.
            Some(false) => section_types.push(&field.ty),
        }
    }

    Ok(quote! {
        impl #struct_ident {
            /// Per-section CONFIG parameter tables, one `PARAMS` slice per
            /// `#[section]` field of the root config.
            ///
            /// Generated by `#[derive(ConfigSections)]`. Referencing each
            /// section type's `PARAMS` here is a compile-time completeness link:
            /// a field classified `#[section]` whose type is not a
            /// `#[derive(ConfigParams)]` section fails to compile. Consumed by
            /// `frogdb_config`'s `config_param_registry` coverage test to prove
            /// the hand-spliced assembly covers exactly this derived set.
            pub const SECTION_PARAMS: &'static [&'static [crate::params::ConfigParamInfo]] = &[
                #( <#section_types>::PARAMS ),*
            ];
        }
    })
}

/// Parse the `#[section]` / `#[section(skip)]` attribute for a root config field.
///
/// Returns `None` when the field carries no `#[section]` attribute (a compile
/// error, handled by the caller), `Some(true)` for `#[section(skip)]`, and
/// `Some(false)` for a bare `#[section]` marker.
fn parse_section_opts(field: &syn::Field) -> syn::Result<Option<bool>> {
    let mut found = false;
    let mut skip = false;

    for attr in &field.attrs {
        if !attr.path().is_ident("section") {
            continue;
        }
        found = true;

        // Bare `#[section]` (a path meta) means "this is a param section".
        if matches!(attr.meta, syn::Meta::Path(_)) {
            continue;
        }

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("skip") {
                skip = true;
                Ok(())
            } else {
                Err(meta.error("unknown `section` option; expected `skip`"))
            }
        })?;
    }

    if !found {
        return Ok(None);
    }

    Ok(Some(skip))
}

/// Parse `#[params(section = "...")]` from the struct attributes.
fn parse_section(attrs: &[Attribute]) -> syn::Result<Option<String>> {
    let mut section = None;
    for attr in attrs {
        if !attr.path().is_ident("params") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("section") {
                let value = meta.value()?;
                let lit: LitStr = value.parse()?;
                section = Some(lit.value());
                Ok(())
            } else {
                Err(meta.error("unknown `params` option; expected `section = \"...\"`"))
            }
        })?;
    }
    Ok(section)
}

#[derive(Default)]
struct ParamOpts {
    skip: bool,
    mutable: bool,
    noop: bool,
    name: Option<String>,
}

/// Parse the `#[param(...)]` attribute for a field.
///
/// Returns `None` when the field has no `#[param]` attribute at all (a compile
/// error, handled by the caller), `Some(opts)` otherwise.
fn parse_param_opts(field: &syn::Field) -> syn::Result<Option<ParamOpts>> {
    let mut found = false;
    let mut opts = ParamOpts::default();

    for attr in &field.attrs {
        if !attr.path().is_ident("param") {
            continue;
        }
        found = true;

        // Bare `#[param]` (a path meta) means "registered, all defaults".
        if matches!(attr.meta, syn::Meta::Path(_)) {
            continue;
        }

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("skip") {
                opts.skip = true;
            } else if meta.path.is_ident("mutable") {
                opts.mutable = true;
            } else if meta.path.is_ident("noop") {
                opts.noop = true;
            } else if meta.path.is_ident("name") {
                let value = meta.value()?;
                let lit: LitStr = value.parse()?;
                opts.name = Some(lit.value());
            } else {
                return Err(meta.error(
                    "unknown `param` option; expected one of `skip`, `mutable`, `noop`, `name = \"...\"`",
                ));
            }
            Ok(())
        })?;
    }

    if !found {
        return Ok(None);
    }

    if opts.skip && (opts.mutable || opts.noop || opts.name.is_some()) {
        return Err(syn::Error::new_spanned(
            field,
            "`#[param(skip)]` cannot be combined with other `param` options",
        ));
    }

    Ok(Some(opts))
}

/// Extract `#[serde(rename = "...")]` from a field's attributes, if present.
///
/// Other serde options (`default = "..."`, flags, etc.) are consumed but
/// ignored; any serde form this does not understand is silently skipped (serde's
/// own derive validates it).
fn serde_rename(attrs: &[Attribute]) -> Option<String> {
    let mut rename = None;
    for attr in attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }
        // Errors are swallowed on purpose: this macro is not the authority on
        // serde attribute syntax, it only opportunistically reads `rename`.
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                let value = meta.value()?;
                let lit: LitStr = value.parse()?;
                rename = Some(lit.value());
            } else if meta.input.peek(Token![=]) {
                // `key = <expr>` — consume the value so parsing can continue.
                let value = meta.value()?;
                let _: syn::Expr = value.parse()?;
            }
            // Bare flags (`default`, `flatten`, ...) have nothing to consume.
            Ok(())
        });
    }
    rename
}

/// Convert a snake_case Rust identifier to serde's kebab-case form.
///
/// Config-section structs use `#[serde(rename_all = "kebab-case")]` on
/// snake_case fields, so this simple substitution reproduces the serde field
/// name (explicit `#[serde(rename)]` overrides take precedence upstream).
fn kebab_case(ident: &syn::Ident) -> String {
    ident.to_string().replace('_', "-")
}
