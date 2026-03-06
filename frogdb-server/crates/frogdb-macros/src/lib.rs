//! FrogDB procedural macros.
//!
//! This crate provides derive macros to reduce boilerplate in command implementations.
//!
//! # Command Derive Macro
//!
//! The `#[derive(Command)]` macro eliminates repetitive `name()`, `arity()`, `flags()`,
//! and `keys()` implementations across 246+ commands.
//!
//! ## Usage
//!
//! ```rust,ignore
//! use frogdb_macros::Command;
//!
//! #[derive(Command)]
//! #[command(name = "GET", arity = 1, flags = "readonly fast")]
//! #[keys(first)]  // First argument is the key
//! pub struct GetCommand;
//!
//! impl GetCommand {
//!     // Only implement the execute method - no boilerplate needed
//!     fn execute_impl(
//!         &self,
//!         ctx: &mut CommandContext,
//!         args: &[Bytes],
//!     ) -> Result<Response, CommandError> {
//!         // Your command logic here
//!     }
//! }
//! ```
//!
//! ## Key Extraction Patterns
//!
//! - `#[keys(first)]` - First argument is the key (most common)
//! - `#[keys(all)]` - All arguments are keys (DEL, EXISTS)
//! - `#[keys(none)]` - No keys (admin commands)
//! - `#[keys(range = "1..")]` - Range of arguments
//! - `#[keys(step = 2)]` - Every other argument starting at 0 (MSET pattern)
//! - `#[keys(custom)]` - Use hand-written `keys()` method
//!
//! ## Flags
//!
//! Flags are specified as space-separated strings:
//! - `write` - Command modifies data
//! - `readonly` - Command only reads data
//! - `fast` - O(1) operation
//! - `blocking` - May block the connection
//! - `multi_key` - Operates on multiple keys
//! - `pubsub` - Pub/sub command
//! - `script` - Script execution
//! - `noscript` - Cannot be called from scripts
//! - `admin` - Modifies server state
//!
//! ## Execution Strategy
//!
//! Default is `Standard`. Override with:
//! - `#[command(strategy = "blocking")]` - Blocking command
//! - `#[command(strategy = "scatter_gather")]` - Scatter-gather command
//! - `#[command(strategy = "connection_level")]` - Connection-level command

mod command;

use proc_macro::TokenStream;

/// Derive macro for implementing the Command trait.
///
/// This macro generates implementations for:
/// - `name()` - Returns the command name
/// - `arity()` - Returns the argument count specification
/// - `flags()` - Returns command flags
/// - `keys()` - Extracts keys from arguments (based on #[keys(...)] attribute)
///
/// The `execute()` method must be implemented manually (as `execute_impl`).
#[proc_macro_derive(Command, attributes(command, keys))]
pub fn derive_command(input: TokenStream) -> TokenStream {
    command::derive_command_impl(input)
}
