//! Proc macros for FrogDB typed metrics system.
//!
//! Provides two main features:
//! 1. `MetricLabel` derive macro for enum label types
//! 2. `define_metrics!` macro for declaring typed metrics

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    DeriveInput, Expr, Ident, LitStr, Token, Type, braced, bracketed, parse_macro_input,
    punctuated::Punctuated,
};

/// Derive macro for metric label enums.
///
/// Generates `as_str()` method and `MetricLabel` trait implementation.
///
/// # Example
/// ```ignore
/// #[derive(MetricLabel)]
/// pub enum ErrorType {
///     #[label = "timeout"]
///     Timeout,
///     #[label = "oom"]
///     OutOfMemory,
/// }
/// ```
#[proc_macro_derive(MetricLabel, attributes(label))]
pub fn derive_metric_label(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let name = &input.ident;

    let variants = match &input.data {
        syn::Data::Enum(data) => &data.variants,
        _ => {
            return syn::Error::new_spanned(input, "MetricLabel can only be derived for enums")
                .to_compile_error()
                .into();
        }
    };

    let mut match_arms = Vec::new();
    let mut all_values = Vec::new();

    for variant in variants {
        let variant_name = &variant.ident;

        // Find the #[label = "..."] attribute
        let label_value = variant
            .attrs
            .iter()
            .find_map(|attr| {
                if attr.path().is_ident("label") {
                    attr.parse_args::<LitStr>().ok()
                } else {
                    None
                }
            })
            .map(|lit| lit.value())
            .unwrap_or_else(|| variant_name.to_string().to_lowercase());

        match_arms.push(quote! {
            #name::#variant_name => #label_value
        });

        all_values.push(quote! { #name::#variant_name });
    }

    let expanded = quote! {
        impl #name {
            /// Get the string representation for this label value.
            #[inline]
            pub const fn as_str(&self) -> &'static str {
                match self {
                    #(#match_arms),*
                }
            }

            /// Get all possible values of this label.
            pub const fn all_values() -> &'static [#name] {
                &[#(#all_values),*]
            }
        }

        impl std::fmt::Display for #name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.as_str())
            }
        }
    };

    TokenStream::from(expanded)
}

/// Input for a single metric definition in define_metrics!
struct MetricDef {
    doc: Option<String>,
    metric_type: Ident,
    struct_name: Ident,
    metric_name: LitStr,
    labels: Vec<LabelDef>,
}

struct LabelDef {
    name: Ident,
    ty: Type,
}

/// Parses the define_metrics! macro input.
struct MetricsInput {
    metrics: Vec<MetricDef>,
}

impl syn::parse::Parse for MetricsInput {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut metrics = Vec::new();

        while !input.is_empty() {
            // Parse doc comments using Attribute::parse_outer
            let attrs = syn::Attribute::parse_outer(input)?;
            let mut doc = None;
            for attr in &attrs {
                if attr.path().is_ident("doc")
                    && let syn::Meta::NameValue(nv) = &attr.meta
                    && let Expr::Lit(syn::ExprLit {
                        lit: syn::Lit::Str(s),
                        ..
                    }) = &nv.value
                {
                    doc = Some(s.value().trim().to_string());
                }
            }

            // Parse: metric_type StructName("metric_name") { ... }
            let metric_type: Ident = input.parse()?;
            let struct_name: Ident = input.parse()?;

            let content;
            syn::parenthesized!(content in input);
            let metric_name: LitStr = content.parse()?;

            let body;
            braced!(body in input);

            // Parse labels if present
            let mut labels = Vec::new();
            if body.peek(Ident) {
                let key: Ident = body.parse()?;
                if key == "labels" {
                    let _colon: Token![:] = body.parse()?;
                    let label_content;
                    bracketed!(label_content in body);

                    let label_list: Punctuated<_, Token![,]> = label_content.parse_terminated(
                        |input| {
                            let name: Ident = input.parse()?;
                            let _colon: Token![:] = input.parse()?;
                            let ty: Type = input.parse()?;
                            Ok(LabelDef { name, ty })
                        },
                        Token![,],
                    )?;

                    labels = label_list.into_iter().collect();
                }

                // Consume trailing comma if present
                let _ = body.parse::<Option<Token![,]>>();
            }

            metrics.push(MetricDef {
                doc,
                metric_type,
                struct_name,
                metric_name,
                labels,
            });
        }

        Ok(MetricsInput { metrics })
    }
}

/// Define typed metrics with compile-time safety.
///
/// # Example
/// ```ignore
/// define_metrics! {
///     /// Total commands executed
///     counter CommandsTotal("frogdb_commands_total") {
///         labels: [command: &str],
///     }
///
///     /// Server uptime in seconds
///     gauge UptimeSeconds("frogdb_uptime_seconds") {}
///
///     /// Command execution duration
///     histogram CommandsDuration("frogdb_commands_duration_seconds") {
///         labels: [command: &str],
///     }
/// }
/// ```
#[proc_macro]
pub fn define_metrics(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as MetricsInput);

    let mut structs = Vec::new();
    let mut registry_entries = Vec::new();

    for metric in &input.metrics {
        let MetricDef {
            doc,
            metric_type,
            struct_name,
            metric_name,
            labels,
        } = metric;

        let metric_name_str = metric_name.value();
        let help_str = doc.clone().unwrap_or_default();

        let metric_type_variant = match metric_type.to_string().as_str() {
            "counter" => quote! { crate::typed::MetricType::Counter },
            "gauge" => quote! { crate::typed::MetricType::Gauge },
            "histogram" => quote! { crate::typed::MetricType::Histogram },
            _ => {
                return syn::Error::new_spanned(
                    metric_type,
                    "Unknown metric type. Use: counter, gauge, histogram",
                )
                .to_compile_error()
                .into();
            }
        };

        let label_names: Vec<_> = labels.iter().map(|l| l.name.to_string()).collect();
        let label_names_tokens = if label_names.is_empty() {
            quote! { &[] }
        } else {
            quote! { &[#(#label_names),*] }
        };

        // Generate method parameters for labels
        let label_params: Vec<_> = labels
            .iter()
            .map(|l| {
                let name = &l.name;
                let ty = &l.ty;
                quote! { #name: #ty }
            })
            .collect();

        // Generate label array construction
        let label_array_items: Vec<_> = labels.iter().map(|l| {
            let name = &l.name;
            let name_str = name.to_string();
            // Check if type is an enum (implements as_str) or &str
            let ty = &l.ty;
            let is_str_ref = matches!(ty, Type::Reference(r) if matches!(&*r.elem, Type::Path(p) if p.path.is_ident("str")));
            if is_str_ref {
                quote! { (#name_str, #name) }
            } else {
                quote! { (#name_str, #name.as_str()) }
            }
        }).collect();

        let labels_slice = if labels.is_empty() {
            quote! { &[] }
        } else {
            quote! { &[#(#label_array_items),*] }
        };

        // Generate methods based on metric type
        let methods = match metric_type.to_string().as_str() {
            "counter" => {
                if labels.is_empty() {
                    quote! {
                        /// Increment this counter by 1.
                        #[inline]
                        pub fn inc(recorder: &dyn frogdb_core::MetricsRecorder) {
                            recorder.increment_counter(Self::NAME, 1, &[]);
                        }

                        /// Increment this counter by the given value.
                        #[inline]
                        pub fn inc_by(recorder: &dyn frogdb_core::MetricsRecorder, value: u64) {
                            recorder.increment_counter(Self::NAME, value, &[]);
                        }
                    }
                } else {
                    quote! {
                        /// Increment this counter by 1.
                        #[inline]
                        pub fn inc(recorder: &dyn frogdb_core::MetricsRecorder, #(#label_params),*) {
                            recorder.increment_counter(Self::NAME, 1, #labels_slice);
                        }

                        /// Increment this counter by the given value.
                        #[inline]
                        pub fn inc_by(recorder: &dyn frogdb_core::MetricsRecorder, value: u64, #(#label_params),*) {
                            recorder.increment_counter(Self::NAME, value, #labels_slice);
                        }
                    }
                }
            }
            "gauge" => {
                if labels.is_empty() {
                    quote! {
                        /// Set this gauge to the given value.
                        #[inline]
                        pub fn set(recorder: &dyn frogdb_core::MetricsRecorder, value: f64) {
                            recorder.record_gauge(Self::NAME, value, &[]);
                        }

                        /// Increment this gauge by the given value.
                        #[inline]
                        pub fn inc(recorder: &dyn frogdb_core::MetricsRecorder, value: f64) {
                            recorder.record_gauge(Self::NAME, value, &[]);
                        }
                    }
                } else {
                    quote! {
                        /// Set this gauge to the given value.
                        #[inline]
                        pub fn set(recorder: &dyn frogdb_core::MetricsRecorder, value: f64, #(#label_params),*) {
                            recorder.record_gauge(Self::NAME, value, #labels_slice);
                        }

                        /// Increment this gauge by the given value.
                        #[inline]
                        pub fn inc(recorder: &dyn frogdb_core::MetricsRecorder, value: f64, #(#label_params),*) {
                            recorder.record_gauge(Self::NAME, value, #labels_slice);
                        }
                    }
                }
            }
            "histogram" => {
                if labels.is_empty() {
                    quote! {
                        /// Record an observation in this histogram.
                        #[inline]
                        pub fn observe(recorder: &dyn frogdb_core::MetricsRecorder, value: f64) {
                            recorder.record_histogram(Self::NAME, value, &[]);
                        }
                    }
                } else {
                    quote! {
                        /// Record an observation in this histogram.
                        #[inline]
                        pub fn observe(recorder: &dyn frogdb_core::MetricsRecorder, value: f64, #(#label_params),*) {
                            recorder.record_histogram(Self::NAME, value, #labels_slice);
                        }
                    }
                }
            }
            _ => quote! {},
        };

        let doc_attr = if let Some(d) = doc {
            quote! { #[doc = #d] }
        } else {
            quote! {}
        };

        structs.push(quote! {
            #doc_attr
            pub struct #struct_name;

            impl #struct_name {
                /// The Prometheus metric name.
                pub const NAME: &'static str = #metric_name_str;
                /// Help text describing this metric.
                pub const HELP: &'static str = #help_str;
                /// The type of this metric.
                pub const METRIC_TYPE: crate::typed::MetricType = #metric_type_variant;
                /// Label names for this metric.
                pub const LABELS: &'static [&'static str] = #label_names_tokens;

                #methods
            }
        });

        registry_entries.push(quote! {
            crate::typed::MetricDefinition {
                name: #metric_name_str,
                help: #help_str,
                metric_type: #metric_type_variant,
                labels: #label_names_tokens,
            }
        });
    }

    let num_metrics = registry_entries.len();

    let expanded = quote! {
        #(#structs)*

        /// Registry of all defined metrics for introspection.
        pub static ALL_METRICS: &[crate::typed::MetricDefinition] = &[
            #(#registry_entries),*
        ];

        /// Number of defined metrics.
        pub const METRICS_COUNT: usize = #num_metrics;
    };

    TokenStream::from(expanded)
}
