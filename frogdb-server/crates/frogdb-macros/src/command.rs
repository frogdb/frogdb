//! Command derive macro implementation.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{Attribute, DeriveInput, LitStr, parse_macro_input};

/// Parse the #[command(...)] attribute.
struct CommandAttr {
    name: String,
    arity: AritySpec,
    flags: Vec<String>,
    strategy: Option<String>,
}

/// Parsed arity specification.
enum AritySpec {
    Fixed(usize),
    AtLeast(usize),
    Range { min: usize, max: usize },
}

/// Parse the #[keys(...)] attribute.
#[derive(Default)]
enum KeysSpec {
    #[default]
    First,
    All,
    None,
    Range(String),
    Step(usize),
    Custom,
}

pub fn derive_command_impl(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = &input.ident;

    // Parse attributes
    let command_attr = match parse_command_attr(&input.attrs) {
        Ok(attr) => attr,
        Err(e) => return e.to_compile_error().into(),
    };

    let keys_spec = match parse_keys_attr(&input.attrs) {
        Ok(spec) => spec,
        Err(e) => return e.to_compile_error().into(),
    };

    // Generate implementations
    let name_impl = generate_name_impl(&command_attr.name);
    let arity_impl = generate_arity_impl(&command_attr.arity);
    let flags_impl = generate_flags_impl(&command_attr.flags);
    let keys_impl = generate_keys_impl(&keys_spec);
    let strategy_impl = generate_strategy_impl(&command_attr.strategy);

    let expanded = quote! {
        impl ::frogdb_core::Command for #struct_name {
            fn name(&self) -> &'static str {
                #name_impl
            }

            fn arity(&self) -> ::frogdb_core::Arity {
                #arity_impl
            }

            fn flags(&self) -> ::frogdb_core::CommandFlags {
                #flags_impl
            }

            #strategy_impl

            fn execute(
                &self,
                ctx: &mut ::frogdb_core::CommandContext,
                args: &[::bytes::Bytes],
            ) -> ::std::result::Result<::frogdb_protocol::Response, ::frogdb_core::CommandError> {
                self.execute_impl(ctx, args)
            }

            fn keys<'a>(&self, args: &'a [::bytes::Bytes]) -> ::std::vec::Vec<&'a [u8]> {
                #keys_impl
            }
        }
    };

    expanded.into()
}

fn parse_command_attr(attrs: &[Attribute]) -> syn::Result<CommandAttr> {
    let mut name = None;
    let mut arity = None;
    let mut flags = Vec::new();
    let mut strategy = None;

    for attr in attrs {
        if !attr.path().is_ident("command") {
            continue;
        }

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("name") {
                let value: LitStr = meta.value()?.parse()?;
                name = Some(value.value());
            } else if meta.path.is_ident("arity") {
                // Parse arity - can be a number or "min.." or "min..max"
                let value: LitStr = meta.value()?.parse()?;
                arity = Some(parse_arity_spec(&value.value())?);
            } else if meta.path.is_ident("flags") {
                let value: LitStr = meta.value()?.parse()?;
                flags = value.value().split_whitespace().map(String::from).collect();
            } else if meta.path.is_ident("strategy") {
                let value: LitStr = meta.value()?.parse()?;
                strategy = Some(value.value());
            }
            Ok(())
        })?;
    }

    Ok(CommandAttr {
        name: name.ok_or_else(|| {
            syn::Error::new_spanned(&attrs[0], "missing `name` in #[command(...)]")
        })?,
        arity: arity.ok_or_else(|| {
            syn::Error::new_spanned(&attrs[0], "missing `arity` in #[command(...)]")
        })?,
        flags,
        strategy,
    })
}

fn parse_arity_spec(s: &str) -> syn::Result<AritySpec> {
    let s = s.trim();

    // Check for range patterns
    if s.contains("..") {
        if s.ends_with("..") {
            // "N.." means at least N
            let min: usize = s.trim_end_matches("..").parse().map_err(|_| {
                syn::Error::new(proc_macro2::Span::call_site(), "invalid arity number")
            })?;
            return Ok(AritySpec::AtLeast(min));
        } else if s.contains("..=") {
            // "min..=max" inclusive range
            let parts: Vec<&str> = s.split("..=").collect();
            if parts.len() == 2 {
                let min: usize = parts[0].parse().map_err(|_| {
                    syn::Error::new(proc_macro2::Span::call_site(), "invalid arity min")
                })?;
                let max: usize = parts[1].parse().map_err(|_| {
                    syn::Error::new(proc_macro2::Span::call_site(), "invalid arity max")
                })?;
                return Ok(AritySpec::Range { min, max });
            }
        } else {
            // "min..max" exclusive range (treat as inclusive for simplicity)
            let parts: Vec<&str> = s.split("..").collect();
            if parts.len() == 2 {
                let min: usize = parts[0].parse().map_err(|_| {
                    syn::Error::new(proc_macro2::Span::call_site(), "invalid arity min")
                })?;
                let max: usize = parts[1].parse().map_err(|_| {
                    syn::Error::new(proc_macro2::Span::call_site(), "invalid arity max")
                })?;
                return Ok(AritySpec::Range {
                    min,
                    max: max.saturating_sub(1),
                });
            }
        }
    }

    // Simple number = fixed arity
    let n: usize = s
        .parse()
        .map_err(|_| syn::Error::new(proc_macro2::Span::call_site(), "invalid arity number"))?;
    Ok(AritySpec::Fixed(n))
}

fn parse_keys_attr(attrs: &[Attribute]) -> syn::Result<KeysSpec> {
    for attr in attrs {
        if !attr.path().is_ident("keys") {
            continue;
        }

        // Parse the keys attribute value
        let content = attr.meta.require_list()?;
        let tokens = content.tokens.to_string();
        let tokens = tokens.trim();

        if tokens == "first" {
            return Ok(KeysSpec::First);
        } else if tokens == "all" {
            return Ok(KeysSpec::All);
        } else if tokens == "none" {
            return Ok(KeysSpec::None);
        } else if tokens == "custom" {
            return Ok(KeysSpec::Custom);
        } else if tokens.starts_with("range") {
            // Parse #[keys(range = "1..")]
            if let Some(eq_pos) = tokens.find('=') {
                let range_str = tokens[eq_pos + 1..].trim().trim_matches('"');
                return Ok(KeysSpec::Range(range_str.to_string()));
            }
        } else if tokens.starts_with("step") {
            // Parse #[keys(step = 2)]
            if let Some(eq_pos) = tokens.find('=') {
                let step_str = tokens[eq_pos + 1..].trim();
                let step: usize = step_str
                    .parse()
                    .map_err(|_| syn::Error::new_spanned(attr, "invalid step value"))?;
                return Ok(KeysSpec::Step(step));
            }
        }

        return Err(syn::Error::new_spanned(
            attr,
            "invalid #[keys(...)] attribute. Expected: first, all, none, custom, range = \"...\", or step = N",
        ));
    }

    // Default to first key
    Ok(KeysSpec::First)
}

fn generate_name_impl(name: &str) -> TokenStream2 {
    quote! { #name }
}

fn generate_arity_impl(arity: &AritySpec) -> TokenStream2 {
    match arity {
        AritySpec::Fixed(n) => quote! { ::frogdb_core::Arity::Fixed(#n) },
        AritySpec::AtLeast(n) => quote! { ::frogdb_core::Arity::AtLeast(#n) },
        AritySpec::Range { min, max } => quote! {
            ::frogdb_core::Arity::Range { min: #min, max: #max }
        },
    }
}

fn generate_flags_impl(flags: &[String]) -> TokenStream2 {
    if flags.is_empty() {
        return quote! { ::frogdb_core::CommandFlags::empty() };
    }

    let flag_tokens: Vec<TokenStream2> = flags
        .iter()
        .map(|f| match f.to_lowercase().as_str() {
            "write" => quote! { ::frogdb_core::CommandFlags::WRITE },
            "readonly" => quote! { ::frogdb_core::CommandFlags::READONLY },
            "fast" => quote! { ::frogdb_core::CommandFlags::FAST },
            "blocking" => quote! { ::frogdb_core::CommandFlags::BLOCKING },
            "multi_key" | "multikey" => quote! { ::frogdb_core::CommandFlags::MULTI_KEY },
            "pubsub" => quote! { ::frogdb_core::CommandFlags::PUBSUB },
            "script" => quote! { ::frogdb_core::CommandFlags::SCRIPT },
            "noscript" => quote! { ::frogdb_core::CommandFlags::NOSCRIPT },
            "loading" => quote! { ::frogdb_core::CommandFlags::LOADING },
            "stale" => quote! { ::frogdb_core::CommandFlags::STALE },
            "skip_slowlog" => quote! { ::frogdb_core::CommandFlags::SKIP_SLOWLOG },
            "random" => quote! { ::frogdb_core::CommandFlags::RANDOM },
            "admin" => quote! { ::frogdb_core::CommandFlags::ADMIN },
            "nondeterministic" => quote! { ::frogdb_core::CommandFlags::NONDETERMINISTIC },
            "no_propagate" => quote! { ::frogdb_core::CommandFlags::NO_PROPAGATE },
            _ => quote! { ::frogdb_core::CommandFlags::empty() },
        })
        .collect();

    quote! {
        #(#flag_tokens)|*
    }
}

fn generate_keys_impl(spec: &KeysSpec) -> TokenStream2 {
    match spec {
        KeysSpec::First => quote! {
            if args.is_empty() {
                ::std::vec::Vec::new()
            } else {
                ::std::vec![&args[0][..]]
            }
        },
        KeysSpec::All => quote! {
            args.iter().map(|a| &a[..]).collect()
        },
        KeysSpec::None => quote! {
            ::std::vec::Vec::new()
        },
        KeysSpec::Range(range_str) => {
            // Parse range string like "1.." or "1..3"
            if range_str.ends_with("..") {
                let start: usize = range_str.trim_end_matches("..").parse().unwrap_or(0);
                quote! {
                    args.get(#start..).map(|slice| {
                        slice.iter().map(|a| &a[..]).collect()
                    }).unwrap_or_default()
                }
            } else if range_str.contains("..") {
                let parts: Vec<&str> = range_str.split("..").collect();
                let start: usize = parts.first().and_then(|s| s.parse().ok()).unwrap_or(0);
                let end: usize = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
                quote! {
                    args.get(#start..#end).map(|slice| {
                        slice.iter().map(|a| &a[..]).collect()
                    }).unwrap_or_default()
                }
            } else {
                // Single index
                let idx: usize = range_str.parse().unwrap_or(0);
                quote! {
                    if args.len() > #idx {
                        ::std::vec![&args[#idx][..]]
                    } else {
                        ::std::vec::Vec::new()
                    }
                }
            }
        }
        KeysSpec::Step(step) => {
            // Every N-th argument starting at 0 (for MSET pattern: key1 val1 key2 val2)
            quote! {
                args.iter()
                    .step_by(#step)
                    .map(|a| &a[..])
                    .collect()
            }
        }
        KeysSpec::Custom => quote! {
            self.keys_impl(args)
        },
    }
}

fn generate_strategy_impl(strategy: &Option<String>) -> TokenStream2 {
    match strategy.as_deref() {
        None => quote! {},
        Some("standard") => quote! {},
        Some("blocking") => quote! {
            fn execution_strategy(&self) -> ::frogdb_core::ExecutionStrategy {
                ::frogdb_core::ExecutionStrategy::Blocking { default_timeout: None }
            }
        },
        Some("scatter_gather") | Some("scatter-gather") => quote! {
            fn execution_strategy(&self) -> ::frogdb_core::ExecutionStrategy {
                ::frogdb_core::ExecutionStrategy::ScatterGather {
                    merge: ::frogdb_core::MergeStrategy::Custom
                }
            }
        },
        Some("async_external") | Some("async-external") => quote! {
            fn execution_strategy(&self) -> ::frogdb_core::ExecutionStrategy {
                ::frogdb_core::ExecutionStrategy::AsyncExternal
            }
        },
        Some("raft_consensus") | Some("raft-consensus") => quote! {
            fn execution_strategy(&self) -> ::frogdb_core::ExecutionStrategy {
                ::frogdb_core::ExecutionStrategy::RaftConsensus
            }
        },
        Some(other) => {
            let msg = format!("unknown strategy: {}", other);
            quote! {
                compile_error!(#msg)
            }
        }
    }
}
